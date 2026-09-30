import asyncio

import pytest

from biothings.utils import mongo
from biothings.utils.hub_db import ChangeWatcher
from biothings.utils.sqlite3 import Database


class FakeMongoCollection:
    """biothings.utils.mongo.Collection's methods, over fake pymongo ones"""

    update = mongo.Collection.update
    remove = mongo.Collection.remove
    save = mongo.Collection.save

    def __init__(self):
        self.docs = {}

    def insert_one(self, doc, *args, **kwargs):
        self.docs[doc["_id"]] = dict(doc)

    def replace_one(self, query, doc, *args, **kwargs):
        self.docs[query["_id"]] = dict(doc)

    def update_one(self, query, what, *args, **kwargs):
        self.docs[query["_id"]].update(what["$set"])

    def delete_one(self, query, **kwargs):
        self.docs.pop(query["_id"], None)

    def find_one(self, query):
        return self.docs.get(query["_id"])


@pytest.fixture
def events(monkeypatch):
    # with a listener, changes are queued as events (but not published, the queue is only read here)
    monkeypatch.setattr(ChangeWatcher, "listeners", {object()})
    queue = asyncio.Queue()
    monkeypatch.setattr(ChangeWatcher, "event_queue", queue)
    return lambda: [queue.get_nowait() for _ in range(queue.qsize())]


@pytest.mark.parametrize("backend", ["mongo", "sqlite3"])
def test_one_event_per_change(backend, events, tmp_path):
    collection = FakeMongoCollection() if backend == "mongo" else Database(str(tmp_path), "hubdb")["event"]

    def get_event():
        return collection

    col = ChangeWatcher.wrap(get_event)()
    # save() calls replace_one() or insert_one(), update() calls update_one()
    col.save({"_id": "1", "msg": "dump started"})
    col.save({"_id": "1", "msg": "dump done"})
    col.update({"_id": "1"}, {"$set": {"level": "INFO"}})
    # inner calls still run, only their events are skipped
    assert col.find_one({"_id": "1"}) == {"_id": "1", "msg": "dump done", "level": "INFO"}
    col.remove({"_id": "1"})
    assert events() == [
        {"_id": "1", "obj": "event", "op": "save", "data": {"_id": "1", "msg": "dump started"}},
        {"_id": "1", "obj": "event", "op": "save", "data": {"_id": "1", "msg": "dump done"}},
        {"_id": "1", "obj": "event", "op": "update", "data": {"_id": "1"}},
        {"_id": "1", "obj": "event", "op": "remove", "data": {"_id": "1"}},
    ]
