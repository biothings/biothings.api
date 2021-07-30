"""
Improve logging output for Elasticsearch Connections
"""

import hashlib
import logging
import os
import pickle
from functools import partial

import boto3
import elasticsearch
import elasticsearch_dsl
import requests
from elasticsearch import AIOHttpConnection
from elasticsearch import RequestsHttpConnection as _Conn
from requests_aws4auth import AWS4Auth
from tornado.ioloop import IOLoop

logger = logging.getLogger(__name__)


class ESPackageInfo():

    def __init__(self):
        self.es_ver = elasticsearch.__version__
        self.es_dsl_ver = elasticsearch_dsl.__version__
        logger.info("Python Elasticsearch Version: %s",
                    '.'.join(map(str, self.es_dsl_ver)))
        logger.info("Python Elasticsearch DSL Version: %s",
                    '.'.join(map(str, self.es_dsl_ver)))

        if self.es_ver[0] != self.es_dsl_ver[0]:
            logger.error("ES Pacakge Version Mismatch with ES-DSL.")

    def is_compatible(self, version):
        assert isinstance(version, str)
        major_version = version.split('.')[0]
        assert major_version.isdigit()
        return int(major_version) == self.es_ver[0]


es_local = None


def _log_db(client, uri):
    logger.info(client)


def _log_es(client, hosts):
    _log_db(client, hosts)

    # only perform health check with the async client
    # so that it doesn't slow down program start time
    if isinstance(client, elasticsearch.AsyncElasticsearch):
        async def log_cluster(async_client):
            cluster = await async_client.info()
            # not specifying timeout in the function above because
            # there could be a number of es tasks scheduled before
            # this call and would take the cluster a while to respond

            cluster_name = cluster['cluster_name']
            version = cluster['version']['number']

            global es_local
            if not es_local:
                es_local = ESPackageInfo()

            if es_local.is_compatible(version):
                level = logging.INFO
                suffix = ""
            else:  # package version mismatch
                level = logging.WARNING
                suffix = "(Incompatible)"

            logger.log(
                level, 'Elasticsearch [%s] %s: %s %s',
                hosts, cluster_name, version, suffix
            )
        IOLoop.current().add_callback(log_cluster, client)


# ------------------------
#   Low Level Functions
# ------------------------


class _AsyncConn(AIOHttpConnection):
    def __init__(self, *args, **kwargs):
        self.aws_auth = None
        if isinstance(kwargs.get('http_auth'), AWS4Auth):
            self.aws_auth = kwargs['http_auth']
            kwargs['http_auth'] = None
        super().__init__(*args, **kwargs)

    async def perform_request(
        self, method, url, params=None, body=None,
        timeout=None, ignore=(), headers=None
    ):
        req = requests.PreparedRequest()
        req.prepare(method, self.host + url, headers, None, body, params)
        self.aws_auth(req)  # sign the request
        headers.update(req.headers)
        return await super().perform_request(
            method, url, params, body,
            timeout, ignore, headers
        )


# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
AWS_META_URL = "http://169.254.169.254/latest/dynamic/instance-identity/document"


def get_es_client(hosts=None, async_=False, **settings):

    if settings.pop('aws', False):
        # find region
        session = boto3.Session()
        region = session.region_name

        if not region:  # not in ~/.aws/config
            region = os.environ.get("AWS_REGION")

        if not region:  # not in environment variable
            try:
                res = requests.get(AWS_META_URL)
                region = res.json()["region"]
            except:
                pass

        if not region:  # not running in VPC (classic EC2)
            region = "us-west-2"  # default

        # find credentials
        credentials = session.get_credentials()
        awsauth = AWS4Auth(
            refreshable_credentials=credentials,
            region=region, service='es'
        )

        settings.update(
            http_auth=awsauth, use_ssl=True, verify_certs=True,
            connection_class=_AsyncConn if async_ else _Conn
        )
    elif settings.pop('sniff', None):
        settings.update(
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=60
        )
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


class _ClientPool:

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


es = _ClientPool(get_es_client, partial(get_es_client, async_=True), _log_es)
sql = _ClientPool(get_sql_client, _not_implemented_client)
mongo = _ClientPool(get_mongo_client, _not_implemented_client)
