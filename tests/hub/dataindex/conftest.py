"""
Fixtures for testing the indexer_task functionality
"""

from typing import Callable, Tuple


import elasticsearch
import mongomock
import pytest
import pytest_asyncio

ELASTICSEARCH_HOST = "http://localhost:9200"


@pytest.fixture(scope="module")
def task_clients() -> Tuple[Callable, Callable]:
    mockmongo_client = mongomock.MongoClient()
    database = mockmongo_client["biothings_build"]

    def mongodb_callback():
        return database["mynews_202012280220_vsdevjdk"]

    from biothings.hub.dataindex.indexer_task import ESIndex

    index_name = "indexer-test"
    elasticsearch_client = elasticsearch.Elasticsearch(hosts=ELASTICSEARCH_HOST)

    def elasticsearch_callback():
        return ESIndex(elasticsearch_client, index_name)

    yield elasticsearch_callback, mongodb_callback


@pytest_asyncio.fixture
async def es_client():
    client = elasticsearch.AsyncElasticsearch(hosts=ELASTICSEARCH_HOST)
    try:
        yield client
    finally:
        await client.close()


@pytest_asyncio.fixture
async def index_name(es_client):
    """An empty index, removed again once the test is done."""
    name = "biothings-exists-alias-test"
    await es_client.options(ignore_status=404).indices.delete(index=name)
    await es_client.indices.create(index=name)
    try:
        yield name
    finally:
        await es_client.options(ignore_status=404).indices.delete(index=name)
