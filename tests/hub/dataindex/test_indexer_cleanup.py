import elasticsearch
import mongomock
import pytest

from biothings.hub.dataindex.indexer_cleanup import Cleaner

ES_HOST = "http://localhost:9200"


@pytest.mark.asyncio
async def test_clean_deletes_indices():
    index_name = "biothings-test-index-cleanup"
    client = elasticsearch.Elasticsearch(ES_HOST)
    client.indices.delete(index=index_name, ignore_unavailable=True)  # left by a previous run
    client.indices.create(index=index_name)
    src_build = mongomock.MongoClient()["hubdb"]["src_build"]
    src_build.insert_one({"_id": "mygene_1", "index": {index_name: {"environment": "local"}}})
    cleaner = Cleaner(src_build, {"local": {"args": {"hosts": ES_HOST}}})
    await cleaner.clean([[{"_id": index_name, "environment": "local"}]])
    assert not client.indices.exists(index=index_name)
    assert src_build.find_one({"_id": "mygene_1"})["index"] == {}
