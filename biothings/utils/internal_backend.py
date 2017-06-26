from biothings import config


get_config_conn = None
get_src_dump = None
get_src_master = None
get_src_build = None
get_src_build_config = None
get_source_fullname = None


def setup():
    global get_config_conn
    global get_src_dump
    global get_src_master
    global get_src_build
    global get_src_build_config
    global get_source_fullname
    get_config_conn = config.internal_backend.get_config_conn
    get_src_dump = config.internal_backend.get_src_dump
    get_src_master = config.internal_backend.get_src_master
    get_src_build = config.internal_backend.get_src_build
    get_src_build_config = config.internal_backend.get_src_build_config
    get_source_fullname = config.internal_backend.get_source_fullname


class IConnection(object):
    """
    This class declares an interface and partially implements some of it, 
    mimicking mongokit.Connection class. It's used to keep used document model.
    Any internal backend should implement (derives) this interface
    """
    def __init__(self, *args, **kwargs):
        super(IConnection,self).__init__(*args,**kwargs)
        self._registered_documents = {}
    def register(self, obj):
        self._registered_documents[obj.__name__] = obj
    def __getattr__(self,key):
        if key in self._registered_documents:
            document = self._registered_documents[key]
            return document
        else:
            try:
                return self[key]
            except Exception:
                raise AttributeError(key)

