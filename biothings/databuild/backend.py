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

class SourceDocMongoBackend(SourceDocBackendBase):

    def get_build_configuration(self, build):
        self._build_config = self.build.find_one({'_id' : build})
        return self._build_config

    def validate_sources(self,sources=None):
        assert self._build_config, "'self._build_config' cannot be empty."
        if self.src_masterdocs is None:
            self.src_masterdocs = self.get_src_master_docs()
        if not sources:
            sources = set(self.sources.collection_names())
            build_conf_src = self._build_config['sources']
        else:
            build_conf_src = sources
        # check interseciton between what's needed and what's existing
        for src in build_conf_src:
            assert src in self.src_masterdocs, '"%s" not found in "src_master"' % src
            assert src in sources, '"%s" not an existing collection in "%s"' % (src, self.sources.name)

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


class TargetDocESBackend(TargetDocBackend, DocESBackend):

    def __init__(self,*args,**kwargs):
        raise NotImplementedError("ES backend for building/merging isn't implemented")

    def set_target_name(self,name):
        raise NotImplementedError("Unsupported")
        self.target_esidxer.ES_INDEX_NAME = name
        self.target_esidxer._mapping = self.get_mapping()

    def post_merge(self):
        self.update_mapping_meta()

    def update_mapping_meta(self):
        '''updating _meta field of ES mapping data, including index stats, versions.
           This is for DocESBackend only.
        '''
        _meta = {}
        src_version = self.get_src_version()
        if src_version:
            _meta['src_version'] = src_version
        if getattr(self, '_stats', None):
            _meta['stats'] = self._stats

        if _meta:
            self.target.target_esidxer.update_mapping_meta({'_meta': _meta})


