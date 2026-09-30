import datetime

import mongomock
import pytest

from biothings.hub.dataindex import mongo_build_cleanup
from biothings.hub.dataindex.mongo_build_cleanup import MongoBuildCleaner


class AsyncCollection:
    """pymongo's async API (the part used by MongoBuildCleaner) over a mongomock collection"""

    def __init__(self, collection):
        self.collection = collection

    def find(self, *args, **kwargs):
        async def cursor():
            for doc in self.collection.find(*args, **kwargs):
                yield doc

        return cursor()

    async def delete_many(self, *args, **kwargs):
        return self.collection.delete_many(*args, **kwargs)


class AsyncDatabase:
    def __init__(self, database):
        self.database = database

    def __getitem__(self, name):
        return AsyncCollection(self.database[name])

    async def list_collection_names(self):
        return self.database.list_collection_names()


class AsyncClient:
    def __init__(self, client):
        self.client = client

    def __getitem__(self, name):
        return AsyncDatabase(self.client[name])

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_validate_builds_keeps_archived_builds(monkeypatch):
    client = mongomock.MongoClient()
    src_build = client["hubdb"]["src_build"]
    src_build.insert_many(
        [
            {"_id": "mygene_1"},
            {"_id": "mygene_2"},  # its collection was deleted: an orphaned record
            {"_id": "mygene_3", "archived": datetime.datetime(2024, 1, 1)},  # no collection anymore, on purpose
            {"_id": "mygene_4", "target_name": "mygene_4_target"},
        ]
    )
    for name in ("mygene_1", "mygene_4_target"):
        client["target"][name].insert_one({"_id": "doc"})
    monkeypatch.setattr(mongo_build_cleanup.mongo, "get_hub_db_async_conn", lambda: AsyncClient(client))
    monkeypatch.setattr(mongo_build_cleanup.mongo, "get_src_build_async", lambda conn: conn["hubdb"]["src_build"])
    monkeypatch.setattr(mongo_build_cleanup.btconfig, "DATA_TARGET_DATABASE", "target", raising=False)

    result = await MongoBuildCleaner(job_manager=None).validate_builds()
    assert result == {"builds_removed": 1, "builds_removed_names": ["mygene_2"]}
    assert sorted(doc["_id"] for doc in src_build.find()) == ["mygene_1", "mygene_3", "mygene_4"]
