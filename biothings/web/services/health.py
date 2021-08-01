from elasticsearch import Elasticsearch, AsyncElasticsearch
from collections import UserDict

class HCResult(UserDict):
    pass  # Health Check Result

class DBHealth():

    def __init__(self, client):
        self.client = client

    def check(self, **kwargs):
        # raise Exception in case of an error
        # return db server status details.
        raise NotImplementedError()

class ESHealth(DBHealth):

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html
    # GET http://localhost:9200/_cluster/health
    # {
    #     "cluster_name": "docker-cluster",
    #     "status": "yellow",
    #     "timed_out": false,
    #     "number_of_nodes": 1,
    #     "number_of_data_nodes": 1,
    #     "active_primary_shards": 5,
    #     "active_shards": 5,
    #     "relocating_shards": 0,
    #     "initializing_shards": 0,
    #     "unassigned_shards": 1,
    #     "delayed_unassigned_shards": 0,
    #     "number_of_pending_tasks": 0,
    #     "number_of_in_flight_fetch": 0,
    #     "task_max_waiting_in_queue_millis": 0,
    #     "active_shards_percent_as_number": 83.33333333333334
    # }

    # TODO
    # Add transport level cluster information,
    # like connected nodes, etc

    # TODO
    # It is useful to provide two endpoints,
    # one indicating the service status, factoring in more
    # measurement dimensions like cluster health and data integrity,
    # primarily used for internal error reporting and operations,
    # and another one indicating just the web application health,
    # indicating if we need to apply resource provision level
    # failure resolution strategy like instance auto re-launch.

    def __init__(self, client, payload=None):
        self.client = client
        self.payload = payload
        # {
        #   "index": "genedoc_current",
        #   "id": "1017"
        # }

    async def async_check(self, **kwargs):
        assert isinstance(self.client, AsyncElasticsearch)
        response = HCResult()
        response.update(await self.client.cluster.health())

        if kwargs.get('info'):
            response.update(await self.client.info())

        if self.payload:
            document = await self.client.get(**self.payload)
            response['payload'] = self.payload
            response['document'] = document

        return response

    def check(self):
        assert isinstance(self.client, Elasticsearch)
        return HCResult(self.client.cluster.health())

class MongoHealth(DBHealth):

    def check(self, **kwargs):
        # typical response: {'ok': 1.0}
        return HCResult(self.client.command('ping'))

class SQLHealth(DBHealth):

    def check(self, **kwargs):
        # https://docs.sqlalchemy.org/en/13/core/connections.html
        # #sqlalchemy.engine.Connection.closed
        return HCResult(closed=self.client.closed)
