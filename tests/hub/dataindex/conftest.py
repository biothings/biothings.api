"""
Fixtures for testing the indexer_task functionality
"""

from typing import Callable, Tuple


import elasticsearch
import mongomock
import pytest


@pytest.fixture(scope="module")
def task_clients() -> Tuple[Callable, Callable]:
    mockmongo_client = mongomock.MongoClient()
    database = mockmongo_client["biothings_build"]

    def mongodb_callback():
        return database["mynews_202012280220_vsdevjdk"]

    from biothings.hub.dataindex.indexer_task import ESIndex

    elasticsearch_host = "http://localhost:9200"
    index_name = "indexer-test"
    elasticsearch_client = elasticsearch.Elasticsearch(hosts=elasticsearch_host)

    def elasticsearch_callback():
        return ESIndex(elasticsearch_client, index_name)

    yield elasticsearch_callback, mongodb_callback
