import biothings.utils.redis as redis
from biothings.utils.mongo import doc_feeder
import pymongo


class BasePreCompiledDataProvider(object):
    def __init__(self, name):
        """
        'name' is a way to identify this provider
        (usually linked to a database name behind the scene)
        """
        self.name = name

    def register(self, _id, col_name):
        """
        Tell provider that _id can be found in collection named 'col_name'
        """
        raise NotImplementedError("implement in sub-class")

    def get_all(self):
        """
        Iterate over all register _ids, return a list of collection names
        where they can be found
        """
        raise NotImplementedError("implement in sub-class")


class RedisPreCompiledDataProvider(BasePreCompiledDataProvider):
    def __init__(self, name, connection_params):
        super(RedisPreCompiledDataProvider, self).__init__(name)
        self.connection_params = connection_params
        self.client = redis.RedisClient(connection_params)
        try:
            self.client.check()
        except AssertionError:
            self.client.initialize()
        self.db = self.client.get_db(self.name)

    def register(self, _id, col_name):
        self.db.hset(_id, col_name, 1)

    def get_all(self):
        for _id in self.db.scan_iter():
            #cols = list(self.db.hgetall(_id).keys())
            cols = []
            yield (_id, cols)


class MongoDBPreCompiledDataProvider(BasePreCompiledDataProvider):
    def __init__(self, db_name, name, connection_params):
        self.db_name = db_name
        self.col_name = name
        self.connection_params = connection_params
        self.client = pymongo.MongoClient(connection_params)
        self.col = self.client[self.db_name][self.col_name]

    def register(self, _id, col_name):
        updt = {"$set": {"srcs.%s" % col_name: 1}}
        if type(_id) == list:
            bulk = []
            for oneid in _id:
                bulk.append(pymongo.UpdateOne(filter={"_id": oneid}, update=updt, upsert=True))
            if bulk:
                self.col.bulk_write(bulk, ordered=False)
        else:
            self.col.update_one({"_id": _id}, updt, upsert=True)

    def get_all(self, batch_size=100000):
        for doc_ids in doc_feeder(self.col, step=batch_size, inbatch=True):
            for d in doc_ids:
                yield d
