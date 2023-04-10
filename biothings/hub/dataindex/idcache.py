import biothings.utils.redis as redis


class IDCache(object):
    def mark_done(self, _ids):
        raise NotImplementedError()

    def load(self, name, id_provider, flush=True):
        """
        name is the cache name
        id_provider returns batch of ids, ie. list(_ids)
        flush to delete existing cache
        """
        raise NotImplementedError()


class RedisIDCache(IDCache):
    def __init__(self, name, connection_params):
        self.name = name
        self.redis_client = redis.RedisClient(connection_params)
        try:
            self.redis_client.check()
        except redis.RedisClientError:
            self.redis_client.initialize()
            self.redis_client.check()

    def load(self, id_provider, flush=True):
        db = self.redis_client.get_db(self.name)
        if flush:
            db.flushdb()
        for _ids in id_provider:
            dids = dict([(_id, 0) for _id in _ids])
            db.mset(dids)

    def mark_done(self, _ids):
        db = self.redis_client.get_db(self.name)
        db.delete(*_ids)
