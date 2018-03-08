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
    """Return a Collection instance for src_build_config collection/table"""
    raise NotImplementedError()


def get_data_plugin():
    """Return a Collection instance for data_plugin collection/table"""
    raise NotImplementedError()

def get_api():
    """Return a Collection instance for api collection/table"""
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
        self.dbname = None # should be set from config module
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


def setup(config):
    global get_hub_db_conn
    global get_src_dump
    global get_src_master
    global get_src_build
    global get_src_build_config
    global get_data_plugin
    global get_api
    global get_source_fullname
    get_hub_db_conn = config.hub_db.get_hub_db_conn
    get_src_dump = config.hub_db.get_src_dump
    get_src_master = config.hub_db.get_src_master
    get_src_build = config.hub_db.get_src_build
    get_src_build_config = config.hub_db.get_src_build_config
    get_data_plugin = config.hub_db.get_data_plugin
    get_api = config.hub_db.get_api
    get_source_fullname = config.hub_db.get_source_fullname
    # propagate config module to classes
    config.hub_db.Database.CONFIG = config


