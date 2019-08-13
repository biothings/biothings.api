"""
hub_db module is a place-holder for internal hub database
functions. Hub DB contains informations about sources, 
configurations variables, etc... It's for internal usage.
When biothings.config_for_app() is called, this module will be
"filled" with the actual implementations from the specified backend
(speficied in config.py, or defaulting to MongoDB).

Hub DB can be implemented over different backend, it's orginally
been done using MongoDB, so the dialect is very inspired by pymongo.
Any hub db backend implementation must implement the functions and
classes below. See biothings.utils.mongo and biothings.utils.sqlit3 for
some examples.
"""

import os, asyncio, logging
from functools import wraps, partial

from biothings.utils.common import dump as dumpobj, loadobj, \
                        get_random_string, get_timestamp

def get_hub_db_conn():
    """Return a Database instance (connection to hub db)"""
    raise NotImplementedError()

def get_src_dump():
    """Return a Collection instance for src_dump collection/table"""
    raise NotImplementedError()

def get_src_master():
    """Return a Collection instance for src_master collection/table"""
    raise NotImplementedError()


def get_src_build():
    """Return a Collection instance for src_build collection/table"""
    raise NotImplementedError()


def get_src_build_config():
    """Return a Collection instance for src_build_hnonfig collection/table"""
    raise NotImplementedError()


def get_data_plugin():
    """Return a Collection instance for data_plugin collection/table"""
    raise NotImplementedError()

def get_api():
    """Return a Collection instance for api collection/table"""
    raise NotImplementedError()

def get_cmd():
    """Return a Collection instance for commands collection/table"""
    raise NotImplementedError()

def get_event():
    """Return a Collection instance for events collection/table"""
    raise NotImplementedError()

def get_hub_config():
    """Return a Collection instance storing configuration values"""
    raise NotImplementedError()

def get_last_command():
    """Return the latest cmd document (according to _id)"""
    raise NotImplementedError()



def get_source_fullname(col_name):
    """
    Assuming col_name is a collection created from an upload process,
    find the main source & sub_source associated.
    """
    raise NotImplementedError()

class IDatabase(object):
    """
    This class declares an interface and partially implements some of it, 
    mimicking mongokit.Connection class. It's used to keep used document model.
    Any internal backend should implement (derives) this interface
    """

    def __init__(self):
        super(IDatabase,self).__init__()
        self.name = None # should be set from config module
        # any other initialization can be done here, 
        # depending on the backend specifics

    @property
    def address(self):
        """Returns sufficient information so a connection to a database
        can be created. Information can be a dictionary, object, etc...
        and depends on the actual backend"""
        raise NotImplementedError()

    #TODO: really needed ? on is it for src_db only ?
    def collection_names(self):
        """Return a list of all collections (or tables) found in this database"""
        raise NotImplementedError()

    def create_collection(self,colname):
        """Create a table/colleciton named colname. If backend is using a schema-based
        database (ie. SQL), backend should enforce the schema with at least field "_id"
        as the primary key (as a string).""" 
        raise NotImplementedError()

    def __getitem__(self, colname):
        """Return a Collection object for colname
        (and create it if it doesn't exist)"""
        raise NotImplementedError()

    def __repr__(self):
        return "<%s at %s, %s>" % (self.__class__.__name__,hex(id(self)),self.address)


class Collection(object):
    """
    Defines a minimal subset of MongoDB collection behavior.
    Note: Collection instances must be pickleable (if not, __getstate__
    can be implemented to deal with those attributes for instance)
    """

    def __init__(self, colname, db):
        """Init args can differ depending on the backend requirements.
           colname is the only one required."""
        self.colname = colname
        raise NotImplementedError()

    @property
    def name(self):
        """Return the collection/table name"""
        return self.colname

    def database(self):
        """Return the database name"""
        raise NotImplementedError()

    def find_one(self,*args,**kwargs):
        """Return one document from the collection. *args will contain
        a dict with the query parameters. See also find()"""
        raise NotImplementedError()

    def find(self,*args,**kwargs):
        """Return an iterable of documents matching criterias defined in 
        *args[0] (which will be a dict). Query dialect is a minimal one, inspired
        by MongoDB. Dict can contain the name of a key, and the value being searched for.
        Ex: {"field1":"value1"} will return all documents where field1 == "value1".
        Nested key (field1.subfield1) aren't supported (no need to implement).
        Exact matches only are required.

        If no query is passed, or if query is an empty dict, return all documents.
        """
        raise NotImplementedError()

    def insert_one(self,doc):
        """Insert a document in the collection. Raise an error if already inserted"""
        raise NotImplementedError()

    def update_one(self,query,what):
        """Update one document (or the first matching query). See find() for query parameter.
        "what" tells how to update the document. $set/$unset/$push operators must be implemented
        (refer to MongoDB documentation for more). Nested keys operation aren't necesary.
        """
        raise NotImplementedError()

    def update(self,query,what):
        """Same as update_one() but operate on all documents matching 'query'"""
        raise NotImplementedError()

    def save(self,doc):
        """Shortcut to update_one() or insert_one(). Save the document, by
        either inserting if it doesn't exist, or update existing one"""
        raise NotImplementedError()

    def replace_one(self,query,doc):
        """Replace a document matching 'query' (or the first found one) with passed doc"""
        raise NotImplementedError()

    def remove(self,query):
        """Delete all documents matching 'query'"""
        raise NotImplementedError()

    def count(self):
        """Return the number of documents in the collection"""
        raise NotImplementedError()

    def __getitem__(self, _id):
        """Shortcut to find_one({"_id":_id})"""
        raise NotImplementedError()

def backup(folder=".",archive=None):
    """
    Dump the whole hub_db database in given folder. "archive" can be pass
    to specify the target filename, otherwise, it's randomly generated
    Note: this doesn't backup source/merge data, just the internal data
          used by the hub
    """
    # get database name (ie. hub_db internal database)
    db_name = get_src_dump().database.name
    dump = {}
    for getter in [get_src_dump,get_src_master,get_src_build,
            get_src_build_config,get_data_plugin,get_api]:
        col = getter()
        dump[col.name] = []
        for doc in col.find():
            dump[col.name].append(doc)
    if not archive:
        archive = "%s_dump_%s_%s.pyobj" % (db_name,get_timestamp(),get_random_string())
    path = os.path.join(folder,archive)
    dumpobj(dump,path)
    return path

def restore(archive,drop=False):
    """Restore database from given archive. If drop is True, then delete existing collections"""
    data = loadobj(archive)
    db = get_src_dump().database
    for colname in data:
        docs = data[colname]
        col = db[colname]
        if drop:
            # we don't have a drop command but we can remove all docs
            col.remove({})
        for doc in docs:
            col.save(doc)

#########################################################################
# small pubsub framework to track changes in hub db internal collection #
#########################################################################


class ChangeListener(object):

    def read(self):
        raise NotImplementedError("Implement me")


class ChangeWatcher(object):

    listeners = set()
    event_queue = asyncio.Queue()
    do_publish = False

    col_entity = {
            "src_dump" : "source",
            "src_build" : "build",
            "src_build_config" : "build_config",
            "src_master" : "master",
            "cmd" : "command",
            }

    @classmethod
    def publish(klass):
        klass.do_publish = True
        @asyncio.coroutine
        def do():
            while klass.do_publish:
                evt = yield from klass.event_queue.get()
                logging.debug("Publishing event %s" % evt)
                for listener in klass.listeners:
                    try:
                        listener.read(evt)
                    except Exception as e:
                        pass
                        #logging.error("Can't publish %s to %s: %s" % (evt,listener,e))
        return asyncio.ensure_future(do())

    @classmethod
    def add(klass,listener):
        assert hasattr(listener,"read"), "Listener '%s' has no read() method" % listener
        klass.listeners.add(listener)
        klass.publish()

    @classmethod
    def monitor(klass,func,entity,op):
        @wraps(func)
        def func_wrapper(*args,**kwargs):
            # don't speak alone in the immensity of the void
            if klass.listeners:
                # try to narrow down the event to a doc
                # analyse the query/filter (1st elem in args), it tells how many docs are
                # impacted, thus telling us wether to send a detailed or general event
                if args and type(args[0]) == dict and "_id" in args[0]:
                    # single event associated to one ID, we send an "detailed" event
                    event = {"_id" : args[0]["_id"], "obj" : entity, "op" : op}
                    if entity == "event":
                        # sends everything
                        event["data"] = args[0]
                    klass.event_queue.put_nowait(event)
                else:
                    # can't find ID, we send a general event (not specific to one doc)
                    event = {"obj" : entity, "op" : op}
                    klass.event_queue.put_nowait(event)

            return func(*args,**kwargs)
        return func_wrapper

    @classmethod
    def wrap(klass,getfunc):
        def decorate():
            col = getfunc()
            for method in ["insert_one","update_one","update",
                    "save","replace_one","remove"]:
                colmethod = getattr(col,method)
                colname = getfunc.__name__.replace("get_","")
                colmethod = klass.monitor(colmethod,
                    entity=klass.col_entity.get(colname,colname),
                    op=method)
                setattr(col,method,colmethod)
            return col
        return partial(decorate)


def setup(config):
    global get_hub_db_conn
    global get_src_dump
    global get_src_master
    global get_src_build
    global get_src_build_config
    global get_data_plugin
    global get_api
    global get_cmd
    global get_event
    global get_hub_config
    global get_source_fullname
    global get_last_command
    get_hub_db_conn = config.hub_db.get_hub_db_conn
    # use ChangeWatcher on internal collections so we can publish changes in real-time
    get_src_dump = ChangeWatcher.wrap(config.hub_db.get_src_dump)
    get_src_master = ChangeWatcher.wrap(config.hub_db.get_src_master)
    get_src_build = ChangeWatcher.wrap(config.hub_db.get_src_build)
    get_src_build_config = ChangeWatcher.wrap(config.hub_db.get_src_build_config)
    get_data_plugin = ChangeWatcher.wrap(config.hub_db.get_data_plugin)
    get_api = ChangeWatcher.wrap(config.hub_db.get_api)
    get_cmd = ChangeWatcher.wrap(config.hub_db.get_cmd)
    get_event = ChangeWatcher.wrap(config.hub_db.get_event)
    get_hub_config = ChangeWatcher.wrap(config.hub_db.get_hub_config)
    get_source_fullname = config.hub_db.get_source_fullname
    get_last_command = config.hub_db.get_last_command
    # propagate config module to classes
    config.hub_db.Database.CONFIG = config


