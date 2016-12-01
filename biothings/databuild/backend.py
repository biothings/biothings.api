'''
Backend for storing merged genedoc after building.
Support MongoDB, ES, CouchDB
'''
from biothings.utils.common import get_timestamp, get_random_string
from biothings.utils.backend import DocBackendBase, DocMongoBackend, DocESBackend

# Source specific backend (deals with build config, master docs, etc...)
class SourceDocBackendBase(DocBackendBase):

    def __init__(self, build, master, dump, sources):
        if callable(build):
            self._build_provider = build
            self._build = None
        else:
            self._build = build
        if callable(master):
            self._master_provider = master
            self._master = None
        else:
            self._master = master
        if callable(dump):
            self._dump_provider = dump
            self._dump = None
        else:
            self._dump = dump
        if callable(sources):
            self._sources_provider = sources
            self._sources = None
        else:
            self._sources = sources
        self._build_config = None
        self.src_masterdocs = None

    def __getattr__(self,attr):
        if attr not in ["build","dump","master","sources"]:
            return AttributeError(attr)
        privattr = "_" + attr
        if getattr(self,privattr) is None:
            res= getattr(self,privattr + "_provider")()
            setattr(self,privattr,res)
        return getattr(self,privattr)

    def get_build_configuration(self, build):
        raise NotImplementedError("sub-class and implement me")

    def get_src_master_docs(self):
        raise NotImplementedError("sub-class and implement me")

    def validate_sources(self,sources=None):
        raise NotImplementedError("sub-class and implement me")

    def get_src_versions(self):
        raise NotImplementedError("sub-class and implement me")

    def __getitem__(self,src_name):
        return self.sources[src_name]


# Target specific backend
class TargetDocBackend(DocBackendBase):

    def __init__(self,*args,**kwargs):
        super(TargetDocBackend,self).__init__(*args,**kwargs)
        self.target_name = None

    def set_target_name(self,target_name, build_name=None):
        """
        Create/prepare a target backend, either strictly named "target_name"
        or named derived from "build_name" (for temporary backends)
        """
        self.target_name = target_name or self.generate_target_name(build_name)

    def generate_target_name(self,build_config_name):
        assert not build_config_name is None
        return '{}_{}_{}'.format(build_config_name,
                                         get_timestamp(), get_random_string()).lower()

    def post_merge(self):
        pass

class SourceDocMongoBackend(SourceDocBackendBase):

    def get_build_configuration(self, build):
        self._build_config = self.build.find_one({'_id' : build})
        return self._build_config

    def validate_sources(self,sources=None):
        assert self._build_config, "'self._build_config' cannot be empty."

    def get_src_master_docs(self):
        if self.src_masterdocs is None:
            self.src_masterdocs = dict([(src['_id'], src) for src in list(self.master.find())])
        return self.src_masterdocs

    def get_src_versions(self):
        src_version = {}
        for src in self.dump.find():
            version = src.get('release', src.get('timestamp', None))
            if version:
                src_version[src['_id']] = version
        return src_version


class TargetDocMongoBackend(TargetDocBackend,DocMongoBackend):

    def set_target_name(self,target_name=None, build_name=None):
        super(TargetDocMongoBackend,self).set_target_name(target_name,build_name)
        self.target_collection = self.target_db[self.target_name]

