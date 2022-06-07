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
from biothings.utils.hub_db import get_source_fullname, get_src_build


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

    def __getattr__(self, attr):
        if attr not in ["build_config", "build", "dump", "master", "sources"]:
            return AttributeError(attr)
        privattr = "_" + attr
        if getattr(self, privattr) is None:
            if getattr(self, privattr + "_provider"):
                res = getattr(self, privattr + "_provider")()
            else:
                res = getattr(self, privattr)
            setattr(self, privattr, res)
        return getattr(self, privattr)

    def get_build_configuration(self, build_name):
        raise NotImplementedError("sub-class and implement me")

    def get_src_master_docs(self):
        raise NotImplementedError("sub-class and implement me")

    def validate_sources(self, sources=None):
        raise NotImplementedError("sub-class and implement me")

    def get_src_metadata(self):
        raise NotImplementedError("sub-class and implement me")

    def __getitem__(self, src_name):
        self.sources_accessed[src_name] = 1
        return self.sources[src_name]


# Target specific backend
class TargetDocBackend(DocBackendBase):
    def __init__(self, *args, **kwargs):
        super(TargetDocBackend, self).__init__(*args, **kwargs)
        self._target_name = None

    def set_target_name(self, target_name, build_name=None):
        """
        Create/prepare a target backend, either strictly named "target_name"
        or named derived from "build_name" (for temporary backends)
        """
        self._target_name = target_name or self.generate_target_name(
            build_name)

    @property
    def target_name(self):
        return self._target_name

    def generate_target_name(self, build_config_name):
        assert build_config_name is not None
        return '{}_{}_{}'.format(build_config_name, get_timestamp(),
                                 get_random_string()).lower()

    def get_backend_url(self):
        """
        Return backend URL (see create_backend() for formats)
        """
        # default is a collection in target database
        return self._target_name

    def post_merge(self):
        pass


class SourceDocMongoBackend(SourceDocBackendBase):
    def get_build_configuration(self, build_name):
        self._config = self.build_config.find_one({'_id': build_name})
        return self._config

    def validate_sources(self, sources=None):
        assert self._build_config, "'self._build_config' cannot be empty."

    def get_src_master_docs(self):
        if self.src_masterdocs is None:
            self.src_masterdocs = dict([(src['_id'], src)
                                        for src in list(self.master.find())])
        return self.src_masterdocs

    def get_src_metadata(self):
        """
        Return source versions which have been previously accessed wit this backend object
        or all source versions if none were accessed. Accessing means going through __getitem__
        (the usual way) and allows to auto-keep track of sources of interest, thus returning
        versions only for those.
        """
        src_version = {}
        # what's registered in each uploader, from src_master.
        # also includes versions
        src_meta = {}
        srcs = []
        if self.sources_accessed:
            for src in self.sources_accessed:
                fullname = get_source_fullname(src)
                main_name = fullname.split(".")[0]
                doc = self.dump.find_one({"_id": main_name})
                srcs.append(doc["_id"])
            srcs = list(set(srcs))
        else:
            srcs = [d["_id"] for d in self.dump.find()]
        # we need to return main_source named, but if accessed, it's been through sub-source names
        # query is different in that case
        for src in self.dump.find({"_id": {"$in": srcs}}):
            # now merge other extra information from src_master (src_meta key). src_master _id
            # are sub-source names, not main source so we have to deal with src_dump as well
            # in order to resolve/map main/sub source name
            subsrc_versions = []

            if src and src.get("download"):
                # Store the latest dump time
                src_meta.setdefault(src["_id"], {}).setdefault("dump_date", src["download"]["started_at"])

            if src and src.get("upload"):
                latest_upload_date = None
                meta = {}
                for job_name in src["upload"].get("jobs", {}):
                    job = src["upload"]["jobs"][job_name]
                    # "step" is the actual sub-source name
                    docm = self.master.find_one({"_id": job.get("step")})
                    if docm and docm.get("src_meta"):
                        meta[job.get("step")] = docm["src_meta"]
                    # Store the latest upload time
                    if not latest_upload_date or latest_upload_date < job["started_at"]:
                        latest_upload_date = job["started_at"]
                        meta[job.get("step")]["upload_date"] = latest_upload_date
                # when more than 1 sub-sources, we can have different version in sub-sources
                # (not normal) if one sub-source uploaded, then dumper downloaded a new version,
                # then the other sub-source uploaded that version. This should never happen, just make sure
                subsrc_versions = [{"sub-source": job.get("step"), "version": job.get("release")}
                                   for job in src["upload"].get("jobs", {}).values()]
                assert len(set([s["version"] for s in subsrc_versions])) == 1, "Expecting one version " + \
                    "in upload sub-sources for main source '%s' but got: %s" % (src["_id"], subsrc_versions)
                # usually, url & license are the same wathever the sub-sources are. They are
                # share common metadata, and we don't want them to be repeated for each sub-sources.
                # but, code key is always different for instance and must specific for each sub-sources
                # here we make sure to factor common keys, while the specific ones at sub-level
                if len(meta) > 1:
                    common = {}
                    any = list(meta)[0]
                    topop = []  # common keys
                    for anyk in meta[any]:
                        if len({
                                meta[s].get(anyk) == meta[any][anyk]
                                for s in meta
                        }) == 1:
                            topop.append(anyk)
                    for k in topop:
                        common[k] = meta[any][k]
                        [meta[subname].pop(k, None) for subname in meta]

                    for k, v in common.items():
                        src_meta.setdefault(src["_id"], {}).setdefault(k, v)
                    for subname in meta:
                        for k, v in meta[subname].items():
                            src_meta.setdefault(src["_id"], {}).setdefault(
                                k, {}).setdefault(subname, v)
                # we have metadata, but just one (ie. no sub-source), don't display it
                elif meta:
                    assert len(meta) == 1
                    subname, metad = meta.popitem()
                    for k, v in metad.items():
                        src_meta.setdefault(src["_id"], {}).setdefault(k, v)
            if subsrc_versions:
                version = subsrc_versions[0]["version"]
                src_version[src['_id']] = version
                src_meta.setdefault(src["_id"],
                                    {}).setdefault("version", version)
        return src_meta


class TargetDocMongoBackend(TargetDocBackend, DocMongoBackend):
    def set_target_name(self, target_name=None, build_name=None):
        super(TargetDocMongoBackend,
              self).set_target_name(target_name, build_name)
        self.target_collection = self.target_db[self._target_name]


class ShardedTargetDocMongoBackend(TargetDocMongoBackend):
    def prepare(self):
        assert self.target_name, "target_name not set"
        assert self.target_db, "target_db not set"
        dba = self.target_db.client.admin
        dba.command("shardCollection",
                    "%s.%s" % (self.target_db.name, self.target_name),
                    key={"_id": "hashed"})


class LinkTargetDocMongoBackend(TargetDocBackend):
    """
    This backend type act as a dummy target backend, the data
    is actually stored in source database. It means only one
    datasource can be linked to that target backend, as a consequence,
    when this backend is used in a merge, there's no actual data
    merge. This is useful when "merging/indexing" only one datasource,
    where the merge step is just a duplication of datasource data.
    """
    name = 'link'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # will be set later by LinkDataBuilder, should be the name
        # of the datasource in src database
        self.datasource_name = None
        self.source_db = None

    @property
    def target_collection(self):
        assert self.source_db
        assert self.datasource_name
        return self.source_db[self.datasource_name]

    def get_backend_url(self):
        assert self.datasource_name
        return ("src", self.datasource_name)

    def drop(self):
        # nothing to drop we don't store data
        # but needs to be implemented
        pass


def create_backend(db_col_names, name_only=False, follow_ref=False, **kwargs):
    """
    Guess what's inside 'db_col_names' and return the corresponding backend.
    - It could be a string (will first check for an src_build doc to check
      a backend_url field, if nothing there, will lookup a mongo collection
      in target database)
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
        # first check build doc, if there's backend_url key, we'll use it instead of
        # direclty using db_col_names as target collection (see LinkDataBuilder)
        bdoc = get_src_build().find_one({"_id": db_col_names})
        if follow_ref and bdoc and bdoc.get(
                "backend_url") and bdoc["backend_url"] != db_col_names:
            return create_backend(bdoc["backend_url"],
                                  name_only=name_only,
                                  follow_ref=follow_ref,
                                  **kwargs)
        else:
            db = mongo.get_target_db()
            col = db[db_col_names]
            # normalize params
            db_col_names = [
                "%s:%s" % (db.client.HOST, db.client.PORT), db.name, col.name
            ]
    elif db_col_names[0].startswith("mongodb://"):
        assert len(
            db_col_names
        ) == 3, "Missing connection information for %s" % repr(db_col_names)
        conn = mongo.MongoClient(db_col_names[0])
        db = conn[db_col_names[1]]
        col = db[db_col_names[2]]
        # normalize params
        db_col_names = [
            "%s:%s" % (db.client.HOST, db.client.PORT), db.name, col.name
        ]
    elif len(db_col_names) == 3 and ":" in db_col_names[0]:
        is_mongo = False
        idxr = ESIndexer(index=db_col_names[1],
                         doc_type=db_col_names[2],
                         es_host=db_col_names[0],
                         **kwargs)
        db = idxr
        col = db_col_names[1]
    else:
        assert len(
            db_col_names
        ) == 2, "Missing connection information for %s" % repr(db_col_names)
        db = db_col_names[0] == "target" and mongo.get_target_db(
        ) or mongo.get_src_db()
        col = db[db_col_names[1]]
        # normalize params (0:host, 1:port)
        db_col_names = [
            "%s:%s" % (db.client.HOST, db.client.PORT), db.name,
            col.name
        ]
    assert col is not None, "Could not create collection object from %s" % repr(
        db_col_names)
    if name_only:
        if is_mongo:
            return "mongo_%s_%s_%s" % (db_col_names[0].replace(
                ":", "_"), db_col_names[1], db_col_names[2])
        else:
            return "es_%s_%s_%s" % (db_col_names[0].replace(
                ":", "_"), db_col_names[1], db_col_names[2])
    else:
        if is_mongo:
            return DocMongoBackend(db, col)
        else:
            return DocESBackend(db)


def generate_folder(root_folder, old_db_col_names, new_db_col_names):
    new = create_backend(new_db_col_names, name_only=True)
    old = create_backend(old_db_col_names, name_only=True)
    diff_folder = os.path.join(root_folder, "%s-%s" % (old, new))
    return diff_folder


def merge_src_build_metadata(build_docs):
    """
    Merge metadata from src_build documents. A list of docs
    should be passed, the order is important: the 1st element
    has the less precedence, the last the most. It means that,
    when needed, some values from documents on the "left" of the
    list may be overridden by one on the right.
    Ex: build_version field
    Ideally, build docs shouldn't have any sources in common to
    prevent any unexpected conflicts...
    """
    assert type(build_docs) == list and len(build_docs) >= 2, \
        "More than one build document must be passed in order metadata"
    first = build_docs[0]
    others = build_docs[1:]
    meta = first.setdefault("_meta", {})
    for new_doc in others:
        new_meta = new_doc.get("_meta", {})
        for k, v in new_meta.items():
            # src_version, src, stats: merge
            if type(v) == dict:
                meta.setdefault(k, {})
                meta[k].update(v)
            # build_version, build_date: override
            else:
                meta[k] = v
    # TODO: some fields in stats don't make when merged: total, observed/vcf in mv
    return meta
