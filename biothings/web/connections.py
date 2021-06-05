"""
Improve logging output for Elasticsearch Connections
"""

import hashlib
import logging
import pickle
# from elasticsearch_async.connection import AIOHttpConnection
from collections import UserString, namedtuple
from functools import partial
from typing import NamedTuple

import elasticsearch
import elasticsearch_dsl

logger = logging.getLogger(__name__)

# logger = logging.getLogger("elasticsearch")
# tracer = logging.getLogger("elasticsearch.trace")

from tornado.ioloop import IOLoop


class ESClusterInfo(NamedTuple):
    name: str
    version: str

async def get_cluster_info_async(client):
    es_version = 'unknown'
    es_cluster = 'unknown'
    try:
        info = await client.info(request_timeout=3)
        version = info['version']['number']
        cluster = info['cluster_name']
        health = await client.cluster.health(request_timeout=3)
        status = health['status']
    except elasticsearch.TransportError as exc:
        logger = logging.getLogger(__name__)
        logger.error('Error reading elasticsearch status.')
        logger.debug(exc)
    else:
        es_version = version
        es_cluster = f"{cluster} ({status})"

    return ESClusterInfo(es_cluster, es_version)

def get_cluster_info(client):
    es_version = 'unknown'
    es_cluster = 'unknown'
    try:
        info = client.info(request_timeout=3)
        version = info['version']['number']
        cluster = info['cluster_name']
        health = client.cluster.health(request_timeout=3)
        status = health['status']
    except elasticsearch.TransportError as exc:
        logger = logging.getLogger(__name__)
        logger.error('Error reading elasticsearch status.')
        logger.debug(exc)
    else:
        es_version = version
        es_cluster = f"{cluster} ({status})"

    return ESClusterInfo(es_cluster, es_version)


class ESPackageInfo():  # TODO combine this with get_cluster_info_async

    def __init__(self):
        self.es_ver = elasticsearch.__version__
        self.es_dsl_ver = elasticsearch_dsl.__version__
        logger.info("Python Elasticsearch Version: %s",
                    '.'.join(map(str, self.es_dsl_ver)))
        logger.info("Python Elasticsearch DSL Version: %s",
                    '.'.join(map(str, self.es_dsl_ver)))

        # TODO: log not shown

        if self.es_ver[0] != self.es_dsl_ver[0]:
            logger.error("ES Pacakge Version Mismatch with ES-DSL.")

    def is_compatible(self, version):
        assert isinstance(version, str)
        major_version = version.split('.')[0]
        assert major_version.isdigit()
        return int(major_version) == self.es_ver[0]


espi = ESPackageInfo()

def _log_db(client, uri):
    logger.info(client)

def _log_es(client, hosts):
    _log_db(client, hosts)

    # only perform health check with the async client
    # so that it doesn't slow down program start time
    if isinstance(client, elasticsearch.AsyncElasticsearch):
        async def log_cluster(async_client):
            logger = logging.getLogger(__name__ + '.healthcheck')
            cluster = await get_cluster_info_async(async_client)
            if espi.is_compatible(cluster.version):
                level = logging.INFO
                suffix = "✓" # TODO
            else:
                level = logging.WARNING
                suffix = "Incompatible"
            logger.log(level, 'ES [%s] %s: %s [%s]', hosts, cluster.name, cluster.version, suffix)  # TOO JUST ADD A X
        IOLoop.current().add_callback(log_cluster, client)


# ------------------------
#   Low Level Functions
# ------------------------


def get_es_client(hosts, async_=False, **settings):

    if settings.pop('sniff', None):
        settings.update({
            "sniff_on_start": True,
            "sniff_on_connection_fail": True,
            "sniffer_timeout": 60
        })
    if async_:
        from elasticsearch import AsyncElasticsearch
        client = AsyncElasticsearch
    else:
        from elasticsearch import Elasticsearch
        client = Elasticsearch

    return client(hosts, **settings)

def get_sql_client(uri, **settings):
    from sqlalchemy import create_engine
    return create_engine(uri, **settings).connect()

def get_mongo_client(uri, **settings):
    from pymongo import MongoClient
    return MongoClient(uri, **settings).get_default_database()

def _not_implemented_client():
    raise NotImplementedError()

# ------------------------
#   High Level Utilities
# ------------------------

class _Connections:

    def __init__(self, client_factory, async_factory, callback=None):

        self._client_factory = client_factory
        self._clients = {}

        self._async_client_factory = async_factory
        self._async_clients = {}

        self.callback = callback or _log_db

    @staticmethod
    def hash(config):
        _config = pickle.dumps(config)
        _hash = hashlib.md5(_config)
        return _hash.hexdigest()

    def _get_client(self, repo, factory, uri, settings):
        hash = self.hash((uri, settings))
        if hash in repo:
            return repo[hash]
        repo[hash] = factory(uri, **settings)
        self.callback(repo[hash], uri)
        return repo[hash]

    def get_client(self, uri, **settings):
        return self._get_client(
            self._clients,
            self._client_factory,
            uri, settings
        )

    def get_async_client(self, uri, **settings):
        return self._get_client(
            self._async_clients,
            self._async_client_factory,
            uri, settings
        )


es = _Connections(get_es_client, partial(get_es_client, async_=True), _log_es)
sql = _Connections(get_sql_client, _not_implemented_client)
mongo = _Connections(get_mongo_client, _not_implemented_client)
