import random
import logging

try:
    import redis
except ImportError:
    logging.error('"redis" module is required to access Redis server.')


class RedisClientError(Exception): pass

class RedisClient(object):

    client = None

    @classmethod
    def get_client(klass,params):
        if klass.client is None:
            klass.client = klass(params)
        return klass.client

    def __init__(self,connection_params):
        self._mapdb = None
        self.connection_params = connection_params

    @property
    def mapdb(self):
        if self._mapdb is None:
            self._mapdb = redis.StrictRedis(db=0,**self.connection_params)
        return self._mapdb

    def get_db(self,db_name=None):
        """
        Return a redict client instance from a database name or
        database number (if db_name is an integer)
        """
        self.check()
        try:
            db_num = int(db_name)
        except ValueError:
            db_num = self.mapdb.get(db_name)
            if not db_num:
                db_num = self.pick_db()
        self.mapdb.set(db_name,int(db_num))

        return redis.StrictRedis(db=int(db_num),**self.connection_params)

    def pick_db(self):
        """
        Return a database number, preferably not used (db doesn't exist).
        If no database available (all are used), will be one and flush it...
        """
        db_max_num = int(self.mapdb.config_get("databases")["databases"] or 16)
        # -1: we always keep db=0 (meta db)
        avail = dict(zip(range(1,db_max_num),[True]*(db_max_num-1)))
        for info in self.mapdb.info("keyspace"):
            if not info.startswith("db"):
                continue
            num = int(info.replace("db",""))
            if num == 0:
                continue
            avail.pop(num)
        if not avail:
            db_num = random.randint(1,db_max_num-1)
        else:
            db_num = random.choice(list(avail.keys()))

        return db_num

    def check(self):
        if not self.mapdb.get("__META__") == b'0':
            raise RedisClientError("Can't find database metadata, you may want to use initialize()")

    def initialize(self,deep=False):
        """
        Careful: this may delete data.
        Prepare Redis instance to work with biothings hub:
        - database 0: this db is used to store a mapping between
          database index and database name (so a database can be accessed
          by name). This method will flush this db and prepare it.
        - any other databases will be flushed if deep is True, making the redis
          server fully dedicated to
        """
        if deep:
            self.mapdb.flushall()
        self.mapdb.flushdb()
        self.mapdb.set("__META__",0)
        self.check()
