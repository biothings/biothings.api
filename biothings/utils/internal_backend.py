from biothings import config


get_src_conn = None
get_src_dump = None
get_src_master = None
get_src_build = None
get_src_build_config = None


def setup():
    global get_src_conn
    global get_src_dump
    global get_src_master
    global get_src_build
    global get_src_build_config
    get_src_conn = config.internal_backend.get_src_conn
    get_src_dump = config.internal_backend.get_src_dump
    get_src_master = config.internal_backend.get_src_master
    get_src_build = config.internal_backend.get_src_build
    get_src_build_config = config.internal_backend.get_src_build_config


class Connection(object):
    """
    This class mimicks / is a mock for mongokit.Connection class,
    used to keep used interface (registering document model for instance)
    """
    def __init__(self, *args, **kwargs):
        super(Connection,self).__init__(*args,**kwargs)
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


def get_cache_filename(col_name):
    cache_folder = getattr(config,"CACHE_FOLDER",None)
    if not cache_folder:
        return # we don't even use cache, forget it
    cache_format = getattr(config,"CACHE_FORMAT",None)
    cache_file = os.path.join(config.CACHE_FOLDER,col_name)
    cache_file = cache_format and (cache_file + ".%s" % cache_format) or cache_file
    return cache_file


def invalidate_cache(col_name,col_type="src"):
    if col_type == "src":
        src_dump = get_src_dump()
        if not "." in col_name:
            fullname = get_source_fullname(col_name)
        assert fullname, "Can't resolve source '%s' (does it exist ?)" % col_name

        main,sub = fullname.split(".")
        doc = src_dump.find_one({"_id":main})
        assert doc, "No such source '%s'" % main
        assert doc.get("upload",{}).get("jobs",{}).get(sub), "No such sub-source '%s'" % sub
        # this will make the cache too old
        doc["upload"]["jobs"][sub]["started_at"] = datetime.datetime.now()
        src_dump.update_one({"_id":main},{"$set" : {"upload.jobs.%s.started_at" % sub:datetime.datetime.now()}})
    elif col_type == "target":
        # just delete the cache file
        cache_file = get_cache_filename(col_name)
        if cache_file:
            try:
                os.remove(cache_file)
            except FileNotFoundError:
                pass

