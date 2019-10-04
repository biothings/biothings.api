import os
import sqlite3
import json

from biothings import config
from biothings.utils.hub_db import IDatabase
from biothings.utils.dotfield import parse_dot_fields
from biothings.utils.dataload import update_dict_recur
from biothings.utils.common import json_serial

def get_hub_db_conn():
    return Database()

def get_src_dump():
    db = Database()
    return db[db.CONFIG.DATA_SRC_DUMP_COLLECTION]

def get_src_master():
    db = Database()
    return db[db.CONFIG.DATA_SRC_MASTER_COLLECTION]

def get_src_build():
    db = Database()
    return db[db.CONFIG.DATA_SRC_BUILD_COLLECTION]

def get_src_build_config():
    db = Database()
    return db[db.CONFIG.DATA_SRC_BUILD_CONFIG_COLLECTION]

def get_data_plugin():
    db = Database()
    return db[db.CONFIG.DATA_PLUGIN_COLLECTION]

def get_api():
    db = Database()
    return db[db.CONFIG.API_COLLECTION]

def get_cmd():
    db = Database()
    return db[db.CONFIG.CMD_COLLECTION]

def get_event():
    db = Database()
    return db[db.CONFIG.EVENT_COLLECTION]

def get_hub_config():
    db = Database()
    return db[getattr(db.CONFIG,"HUB_CONFIG_COLLECTION","hub_config")]

def get_last_command():
    try:
        db = Database()
        res = db.get_conn().execute("SELECT MAX(_id) FROM cmd").fetchall()
        return {"_id":res[0][0]}
    except Exception as e:
        return {"_id":1}

def get_source_fullname(col_name):
    """
    Assuming col_name is a collection created from an upload process,
    find the main source & sub_source associated.
    """
    src_dump = get_src_dump()
    info = None
    for doc in src_dump.find():
        if col_name in doc.get("upload",{}).get("jobs",{}).keys():
            info = doc
    if info:
        name = info["_id"]
        if name != col_name:
            # col_name was a sub-source name
            return "%s.%s" % (name,col_name)
        else:
            return name

class Database(IDatabase):

    def __init__(self):
        super(Database,self).__init__()
        self.name = self.CONFIG.DATA_HUB_DB_DATABASE
        if not os.path.exists(self.CONFIG.HUB_DB_BACKEND["sqlite_db_folder"]):
            os.makedirs(self.CONFIG.HUB_DB_BACKEND["sqlite_db_folder"])
        self.dbfile = os.path.join(self.CONFIG.HUB_DB_BACKEND["sqlite_db_folder"],self.name)
        self.cols = {}

    @property
    def address(self):
        return self.dbfile

    def get_conn(self):
        return sqlite3.connect(self.dbfile)

    def collection_names(self):
        tables = self.get_conn().execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return [name[0] for name in tables]

    def create_collection(self,colname):
        return self[colname]

    def create_if_needed(self,table):
        existings = [tname[0] for tname in self.get_conn().execute("SELECT name FROM sqlite_master WHERE type='table' and " + \
                "name = ?",(table,)).fetchall()]
        if not table in existings:
            # TODO: injection...
            self.get_conn().execute("CREATE TABLE %s (_id TEXT PRIMARY KEY, document TEXT)" % table).fetchone()

    def __getitem__(self, colname):
        if not colname in self.cols:
            self.create_if_needed(colname)
            self.cols[colname] = Collection(colname,self)
        return self.cols[colname]


class Collection(object):

    def __init__(self, colname, db):
        self.colname = colname
        self.db = db

    def get_conn(self):
        return sqlite3.connect(self.db.dbfile)

    @property
    def name(self):
        return self.colname

    @property
    def database(self):
        return self.db

    def find_one(self,*args,**kwargs):
        if args and len(args) == 1 and type(args[0]) == dict:
            if len(args[0]) == 1 and "_id" in args[0]:
                strdoc = self.get_conn().execute("SELECT document FROM %s WHERE _id = ?" % self.colname,(args[0]["_id"],)).fetchone()
                if strdoc:
                    return json.loads(strdoc[0])
                else:
                    return None
            else:
                return self.find(*args,find_one=True)
        elif args or kwargs:
            raise NotImplementedError("find(): %s %s" % (repr(args),repr(kwargs)))
        else:
            return self.find(find_one=True)

    def find(self,*args,**kwargs):
        results = []
        if args and len(args) == 1 and type(args[0]) == dict and len(args[0]) > 0:
            # it's key/value search, let's iterate
            for doc in self.get_conn().execute("SELECT document FROM %s" % self.colname).fetchall():
                found = False
                doc = json.loads(doc[0])
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
        elif not args or len(args) == 1 and len(args[0]) == 0:
            # nothing or empty dict
            return [json.loads(doc[0]) for doc in \
                    self.get_conn().execute("SELECT document FROM %s" % self.colname).fetchall()]
        else:
            raise NotImplementedError("find: args=%s kwargs=%s" % (repr(args),repr(kwargs)))

    def insert_one(self,doc):
        assert "_id" in doc
        with self.get_conn() as conn:
            conn.execute("INSERT INTO %s (_id,document) VALUES (?,?)" % self.colname, \
                    (doc["_id"],json.dumps(doc,default=json_serial))).fetchone()
            conn.commit()

    def update_one(self,query,what):
        assert len(what) == 1 and ("$set" in what or \
                "$unset" in what or "$push" in what), "$set/$unset/$push operators not found"
        doc = self.find_one(query)
        if doc:
            if "$set" in what:
                # parse_dot_fields uses json.dumps internally, we can to make
                # sure everything is serializable first
                what = json.loads(json.dumps(what,default=json_serial))
                what = parse_dot_fields(what["$set"])
                doc = update_dict_recur(doc,what)
            elif "$unset" in what:
                for keytounset in what["$unset"].keys():
                    doc.pop(keytounset,None)
            elif "$push" in what:
                for listkey,elem in what["$push"].items():
                    assert not "." in listkey, "$push not supported for nested keys: %s" % listkey
                    doc.setdefault(listkey,[]).append(elem)

            self.save(doc)

    def update(self,query,what):
        docs = self.find(query)
        for doc in docs:
            self.update_one({"_id":doc["_id"]},what)

    def save(self,doc):
        if self.find_one({"_id":doc["_id"]}):
            with self.get_conn() as conn:
                conn.execute("UPDATE %s SET document = ? WHERE _id = ?" % self.colname,
                        (json.dumps(doc,default=json_serial),doc["_id"]))
                conn.commit()
        else:
            self.insert_one(doc)

    def replace_one(self,query,doc,upsert=False):
        assert "_id" in query
        orig = self.find_one(query)
        orig["_id"] = query["_id"]
        if orig:
            with self.get_conn() as conn:
                conn.execute("UPDATE %s SET document = ? WHERE _id = ?" % self.colname,
                        (json.dumps(doc,default=json_serial),orig["_id"]))
                conn.commit()
        elif upsert:
            doc["_id"] = query["_id"]
            self.save(doc)
            

    def remove(self,query):
        docs = self.find(query)
        with self.get_conn() as conn:
            for doc in docs:
                conn.execute("DELETE FROM %s WHERE _id = ?" % self.colname,(doc["_id"],)).fetchone()
            conn.commit()

    def count(self):
        return self.get_conn().execute("SELECT count(_id) FROM %s" % self.colname).fetchone()[0]

    def __getitem__(self, _id):
        return self.find_one({"_id":_id})

    def __getstate__(self):
        self.__dict__.pop("db",None)
        return self.__dict__
