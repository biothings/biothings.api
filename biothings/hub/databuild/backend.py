'''
Backend for storing merged genedoc after building.
Support MongoDB, ES, CouchDB
'''
import os
from functools import partial
from biothings.utils.common import get_timestamp, get_random_string
from biothings.utils.backend import DocBackendBase, DocMongoBackend, DocESBackend
from biothings.utils.es import ESIndexer
import biothings.utils.mongo as mongo
from biothings.utils.hub_db import get_source_fullname

# Source specific backend (deals with build config, master docs, etc...)
class SourceDocBackendBase(DocBackendBase):

    def __init__(self, build_config, build, master, dump, sources):
        if type(build_config) == partial:
            self._build_config_provider = build_config
            self._build_config = None
        else:
            self._build_config_provider = None
            self._build_config = build_config
        if type(build) == partial:
            self._build_provider = build
            self._build = None
        else:
            self._build_provider = None
            self._build = build
        if type(master) == partial:
            self._master_provider = master
            self._master = None
        else:
            self._master_provider = None
            self._master = master
        if type(dump) == partial:
            self._dump_provider = dump
            self._dump = None
        else:
            self._dump_provider = None
            self._dump = dump
        if type(sources) == partial:
            self._sources_provider = sources
            self._sources = None
        else:
            self._sources_provider = None
            self._sources = sources
        self._build_config = None
        self.src_masterdocs = None
        # keep track of sources which were accessed
        self.sources_accessed = {}

    def __getattr__(self,attr):
        if attr not in ["build_config","build","dump","master","sources"]:
            return AttributeError(attr)
        privattr = "_" + attr
        if getattr(self,privattr) is None:
            if getattr(self,privattr + "_provider"):
                res = getattr(self,privattr + "_provider")()
            else:
                res = getattr(self,privattr)
            setattr(self,privattr,res)
        return getattr(self,privattr)

    def get_build_configuration(self, build_name):
        raise NotImplementedError("sub-class and implement me")

    def get_src_master_docs(self):
        raise NotImplementedError("sub-class and implement me")

    def validate_sources(self,sources=None):
        raise NotImplementedError("sub-class and implement me")

    def get_src_metadata(self):
        raise NotImplementedError("sub-class and implement me")

    def __getitem__(self,src_name):
        self.sources_accessed[src_name] = 1
        return self.sources[src_name]


# Target specific backend
class TargetDocBackend(DocBackendBase):

    def __init__(self,*args,**kwargs):
        super(TargetDocBackend,self).__init__(*args,**kwargs)
        self._target_name = None

    def set_target_name(self,target_name, build_name=None):
        """
        Create/prepare a target backend, either strictly named "target_name"
        or named derived from "build_name" (for temporary backends)
        """
        self._target_name = target_name or self.generate_target_name(build_name)

    def generate_target_name(self,build_config_name):
        assert not build_config_name is None
        return '{}_{}_{}'.format(build_config_name,
                                         get_timestamp(), get_random_string()).lower()

    def post_merge(self):
        pass

class SourceDocMongoBackend(SourceDocBackendBase):

    def get_build_configuration(self, build_name):
        self._config = self.build_config.find_one({'_id' : build_name})
        return self._config

    def validate_sources(self,sources=None):
        assert self._build_config, "'self._build_config' cannot be empty."

    def get_src_master_docs(self):
        if self.src_masterdocs is None:
            self.src_masterdocs = dict([(src['_id'], src) for src in list(self.master.find())])
        return self.src_masterdocs

    def get_src_metadata(self,src_filter=[]):
        """
        Return source versions which have been previously accessed wit this backend object
        or all source versions if none were accessed. Accessing means going through __getitem__
        (the usual way) and allows to auto-keep track of sources of interest, thus returning
        versions only for those.
        src_filter can be passed (list of source _id) to add a filter step.
        """
        src_version = {}
        # what's registered in each uploader, from src_master.
        # also includes versions and "src_version" key as a temp duplicate
        src_meta = {}
        srcs = []
        if self.sources_accessed:
            for src in self.sources_accessed:
                fullname = get_source_fullname(src)
                main_name = fullname.split(".")[0]
                doc = self.dump.find_one({"_id":main_name})
                srcs.append(doc["_id"])
            srcs = list(set(srcs))
        else:
            srcs = [d["_id"] for d in self.dump.find()]
        # we need to return main_source named, but if accessed, it's been through sub-source names
        # query is different in that case
        if src_filter:
            srcs = list(set(srcs).intersection(set(src_filter)))
        for src in self.dump.find({"_id":{"$in":srcs}}):
            version = src.get('release', src.get('timestamp', None))
            if version:
                src_version[src['_id']] = version
                src_meta.setdefault(src["_id"],{}).setdefault("version",version)
            # now merge other extra information from src_master (src_meta key). src_master _id
            # are sub-source names, not main source so we have to deal with src_dump as well
            # in order to resolve/map main/sub source name
            if src and src.get("upload"):
                meta = []
                for job_name in src["upload"].get("jobs",[]):
                    job = src["upload"]["jobs"][job_name]
                    # "step" is the actual sub-source name
                    docm  = self.master.find_one({"_id":job.get("step")})
                    if docm and docm.get("src_meta"):
                        meta.append(docm["src_meta"])
                # we'll make sure to have the same src_meta at main source level,
                # whatever we have at sub-source level. In other words, if a main source
                # has multiple sub-sources, there should be only src metadata anyway
                if len(meta) > 1:
                    first = meta[0]
                    assert set([e == first for e in meta[1:]]) == {True}, \
                        "Source '%s' has different metadata declared in its sub-sources" % src["_id"]
                # now we can safely merge src_meta
                if meta:
                    first = meta[0]
                    for k,v in first.items():
                        src_meta.setdefault(src["_id"],{}).setdefault(k,v)
        # backward compatibility
        src_meta["src_version"] = src_version
        return src_meta


class TargetDocMongoBackend(TargetDocBackend,DocMongoBackend):

    def set_target_name(self,target_name=None, build_name=None):
        super(TargetDocMongoBackend,self).set_target_name(target_name,build_name)
        self.target_collection = self.target_db[self._target_name]


def create_backend(db_col_names,name_only=False,**kwargs):
    """
    Guess what's inside 'db_col_names' and return the corresponding backend.
    - It could be a string (by default, will lookup a mongo collection in target database)
    - or a tuple("target|src","col_name")
    - or a ("mongodb://user:pass@host","db","col_name") URI.
    - or a ("es_host:port","index_name","doc_type")
    If name_only is true, just return the name uniquely identifying the collection or index
    URI connection.
    """
    col = None
    db = None
    is_mongo = True
    if type(db_col_names) == str:
        db = mongo.get_target_db()
        col = db[db_col_names]
        # normalize params
        db_col_names = ["%s:%s" % (db.client.HOST,db.client.PORT),db.name,col.name]
    elif db_col_names[0].startswith("mongodb://"):
        assert len(db_col_names) == 3, "Missing connection information for %s" % repr(db_col_names)
        conn = mongo.MongoClient(db_col_names[0])
        db = conn[db_col_names[1]]
        col = db[db_col_names[2]]
        # normalize params
        db_col_names = ["%s:%s" % (db.client.HOST,db.client.PORT),db.name,col.name]
    elif len(db_col_names) == 3 and ":" in db_col_names[0]:
        is_mongo = False
        idxr = ESIndexer(index=db_col_names[1],doc_type=db_col_names[2],es_host=db_col_names[0],**kwargs)
        db = idxr
        col = db_col_names[1]
    else:
        assert len(db_col_names) == 2, "Missing connection information for %s" % repr(db_col_names)
        db = db_col_names[0] == "target" and mongo.get_target_db() or mongo.get_src_db()
        col = db[db_col_names[1]]
        # normalize params (0:host, 1:port)
        db_col_names = ["%s:%s" % (db.client.address[0],db.client.address[1]),db.name,col.name]
    assert not col is None, "Could not create collection object from %s" % repr(db_col_names)
    if name_only:
        if is_mongo:
            return "mongo_%s_%s_%s" % (db_col_names[0].replace(":","_"),
                                      db_col_names[1],db_col_names[2])
        else:
            return "es_%s_%s_%s" % (db_col_names[0].replace(":","_"),
                                    db_col_names[1],db_col_names[2])
    else:
        if is_mongo:
            return DocMongoBackend(db,col)
        else:
            return DocESBackend(db)

def generate_folder(root_folder, old_db_col_names, new_db_col_names):
    new = create_backend(new_db_col_names,name_only=True)
    old = create_backend(old_db_col_names,name_only=True)
    diff_folder = os.path.join(root_folder, "%s-%s" % (old, new))
    return diff_folder

