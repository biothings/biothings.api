raise Exception("Don't use me, no concurrency supported...")

import os
import shelve

from biothings import config
from biothings.utils.hub_db import IConnection
from biothings.utils.dotfield import parse_dot_fields
from biothings.utils.dataload import update_dict_recur


def get_src_conn():
    return Database(config.DATA_SRC_DATABASE)

def get_src_dump():
    db = Database(config.DATA_SRC_DATABASE)
    return db[config.DATA_SRC_DUMP_COLLECTION]

def get_src_build():
    db = Database(config.DATA_SRC_DATABASE)
    return db[config.DATA_SRC_BUILD_COLLECTION]

def get_src_build_config():
    db = Database(config.DATA_SRC_DATABASE)
    return db[config.DATA_SRC_BUILD_CONFIG_COLLECTION]

# shelve won't allow opening db file in 2 different threads
# so we want to make sure to use singleton pattern here
_db_register = {}
class Database(Connection):
    def __new__(cls, dbname):
        global _db_register
        if not dbname in _db_register:
            obj = Connection.__new__(cls)
            obj.dbname = dbname
            _db_register[dbname] = obj
        return _db_register[dbname]

    def __init__(self,dbname):
        super(Database,self).__init__()
        self.dbname = dbname
        self.cols = {}
        self.dbfolder = os.path.join(config.SHELVE_DB_FOLDER,dbname)

    def collection_names(self):
        # for shelve, it's just the db file in db folder
        return os.listdir(self.dbfolder)

    def create_collection(self,colname):
        return self[colname]

    def __getitem__(self, colname):
        if not colname in self.cols:
            self.cols[colname] = Collection(self.dbname,colname,self.dbfolder)
        return self.cols[colname]

    def __repr__(self):
        return "<%s at %s, %s>" % (self.__class__.__name__,hex(id(self)),self.dbname)


class Collection(object):

    def __init__(self, dbname, colname, dbfolder):
        if not os.path.exists(dbfolder):
            os.makedirs(dbfolder)
        self.colname = colname
        self.dbname = dbname
        self.shelf = shelve.open(os.path.join(dbfolder,self.colname))

    @property
    def name(self):
        return self.colname

    @property
    def database(self):
        return Database(self.dbname)

    def find_one(self,*args,**kwargs):
        if args and len(args) == 1 and type(args[0]) == dict:
            if len(args[0]) == 1 and "_id" in args[0]:
                return self.shelf.get(str(args[0]["_id"])) # keys in shelve are str only...
            else:
                return self.find(*args,find_one=True)
        elif args or kwargs:
            raise NotImplementedError("find(): %s %s" % (repr(args),repr(kwargs)))

    def find(self,*args,**kwargs):
        results = []
        if args and len(args) == 1 and type(args[0]) == dict:
            # it's key/value search, let's iterate
            for doc in self.shelf.values():
                found = False
                for k,v in args[0].items():
                    if k in doc:
                        if doc[k] == v:
                            found = True
                    else:
                        found = False
                        break
                if found:
                    if "find_one" in kwargs:
                        return doc
                    else:
                        results.append(doc)
            return results
        elif args:
            raise NotImplementedError("find(): %s %s" % (repr(args),repr(kwargs)))
        else:
            return (v for v in self.shelf.values())

    def insert_one(self,doc):
        assert "_id" in doc
        sid = str(doc["_id"])
        if sid in self.shelf:
            raise Exception("Duplicated key error '%s'" % sid)
        self.shelf[sid] = doc

    def save(self,doc):
        sid = str(doc["_id"])
        self.shelf[sid] = doc

    def update_one(self,query,what):
        assert len(what) == 1 and "$set" in what
        doc = self.find_one(query)
        if not doc:
            raise Exception("No result for query %s" % repr(query))
        what = parse_dot_fields(what)
        doc = update_dict_recur(doc,what)
        self.save(doc)

    def count(self):
        return len(self.shelf)

    def __getitem__(self, _id):
        return self.cols[colname]
