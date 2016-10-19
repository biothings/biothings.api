'''
Backend for storing merged genedoc after building.
Support MongoDB, ES, CouchDB
'''
from biothings.utils.common import get_timestamp, get_random_string
from biothings.utils.backend import DocBackendBase, DocMongoBackend, DocESBackend

# Source specific backend (deals with build config, master docs, etc...)
class SourceDocBackendBase(DocBackendBase):

    def __init__(self, build, master, dump, sources):
        self.build = build
        self.master = master
        self.dump = dump
        self.sources = sources
        self._build_config = None
        self.src_masterdocs = None

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


