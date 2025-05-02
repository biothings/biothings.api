import hashlib
import logging
import os
import pickle
from functools import partial

import elasticsearch
import elasticsearch_dsl
import requests
from tornado.ioloop import IOLoop

from biothings.utils.common import run_once

try:
    import boto3
    from requests_aws4auth import AWS4Auth

    aws_avail = True
except ImportError:
    # only needed for connecting to AWS OpenSearch
    aws_avail = False

logger = logging.getLogger(__name__)

_should_log = run_once()


def _log_pkg():
    es_ver = elasticsearch.__version__
    es_dsl_ver = elasticsearch_dsl.__versionstr__

    logger.info("Elasticsearch Package Version: %s", ".".join(map(str, es_ver)))
    logger.info("Elasticsearch DSL Package Version: %s", ".".join(map(str, es_dsl_ver)))


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

            if _should_log():
                _log_pkg()

            cluster_name = cluster["cluster_name"]
            version = cluster["version"]["number"]

            logger.info("%s: %s %s", hosts, cluster_name, version)

        IOLoop.current().add_callback(log_cluster, client)


# ------------------------
#   Low Level Functions
# ------------------------

# TODO https://elastic-transport-python.readthedocs.io/en/latest/transport.html
# Convert to use the new async transport class, AIOHttpConnection was removed in 8.x
# class _AsyncConn(AIOHttpConnection):
#     def __init__(self, *args, **kwargs):
#         self.aws_auth = None
#         _auth = kwargs.get("http_auth")
#         if _auth and hasattr(_auth, "region") and isinstance(_auth, AWS4Auth):
#             self.aws_auth = _auth
#             kwargs["http_auth"] = None
#         super().__init__(*args, **kwargs)

#     async def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=(), headers=None):
#         req = requests.PreparedRequest()
#         req.prepare(method, self.host + url, headers, None, body, params)
#         self.aws_auth(req)  # sign the request
#         headers.update(req.headers)
#         return await super().perform_request(method, url, params, body, timeout, ignore, headers)


# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
AWS_META_URL = "http://169.254.169.254/latest/dynamic/instance-identity/document"


def get_es_client(hosts=None, async_=False, **settings):
    """Enhanced ES client initialization.

    Additionally support these parameters:
        async_: use AsyncElasticserach instead of Elasticsearch.
        aws: setup request signing and provide reasonable ES settings
            to access AWS OpenSearch, by default assuming it is on HTTPS.
        sniff: provide resonable default settings to enable client-side
            LB to an ES cluster. this param itself is not an ES param.
    """

    if settings.pop("aws", False):
        if not aws_avail:
            raise ImportError('"boto3" and "requests_aws4auth" are required for AWS OpenSearch')
        # find region
        session = boto3.Session()
        region = session.region_name

        if not region:  # not in ~/.aws/config
            region = os.environ.get("AWS_REGION")
        if not region:  # not in environment variable
            try:  # assume same-region service access
                res = requests.get(AWS_META_URL)
                region = res.json()["region"]
            except Exception:  # not running in VPC
                region = "us-west-2"  # default

        # find credentials
        credentials = session.get_credentials()
        awsauth = AWS4Auth(refreshable_credentials=credentials, region=region, service="es")

        # No longer needed in 8.x, AIOHttpConnection was removed
        # _cc = _AsyncConn if async_ else _Conn
        # settings.update(http_auth=awsauth, connection_class=_cc)
        settings.update(http_auth=awsauth)
        settings.setdefault("use_ssl", True)
        settings.setdefault("verify_certs", True)

    # not evaluated when 'aws' flag is set because
    # AWS OpenSearch is internally load-balanced
    # and does not support client-side sniffing.
    elif settings.pop("sniff", False):
        settings.setdefault("sniff_on_start", True)
        settings.setdefault("sniff_on_connection_fail", True)
        settings.setdefault("sniffer_timeout", 60)

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
        return self._get_client(self._clients, self._client_factory, uri, settings)

    def get_async_client(self, uri, **settings):
        return self._get_client(self._async_clients, self._async_client_factory, uri, settings)


es = _ClientPool(get_es_client, partial(get_es_client, async_=True), _log_es)
sql = _ClientPool(get_sql_client, _not_implemented_client)
mongo = _ClientPool(get_mongo_client, _not_implemented_client)
