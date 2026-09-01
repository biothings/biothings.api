import copy
import importlib
from functools import partial
from types import SimpleNamespace

import pytest


@pytest.fixture
def indexer_task_module(root_configuration, tmp_path):
    root_configuration.override(
        {
            "__file__": str(tmp_path / "test_config.py"),
            "HUB_DB_BACKEND": {
                "module": "biothings.utils.sqlite3",
                "sqlite_db_folder": str(tmp_path),
            },
            "DATA_HUB_DB_DATABASE": "indexer_task_hubdb",
        }
    )

    importlib.import_module("biothings.hub")._config_for_app(root_configuration)
    yield importlib.import_module("biothings.hub.dataindex.indexer_task")

    root_configuration.reset()


class FakeMongoCollection:
    def __init__(self, database, name, documents):
        self.database = database
        self.name = name
        self.documents = documents


class FakeMongoDatabase:
    def __init__(self, client, documents):
        self.client = client
        self.documents = documents
        self.collections = {}

    def __getitem__(self, name):
        self.client.ensure_open()
        return self.collections.setdefault(name, FakeMongoCollection(self, name, self.documents))


class FakeMongoClient:
    def __init__(self, documents):
        self.documents = {doc["_id"]: copy.deepcopy(doc) for doc in documents}
        self.databases = {}
        self.closed = False
        self.close_calls = 0

    def __getitem__(self, name):
        self.ensure_open()
        return self.databases.setdefault(name, FakeMongoDatabase(self, self.documents))

    def ensure_open(self):
        if self.closed:
            raise RuntimeError("Cannot use fake MongoClient after close")

    def close(self):
        self.close_calls += 1
        self.closed = True


class FakeElasticsearchClient:
    def __init__(self, documents):
        self.documents = {doc["_id"]: copy.deepcopy(doc) for doc in documents}
        self.closed = False
        self.close_calls = 0

    def ensure_open(self):
        if self.closed:
            raise RuntimeError("Cannot use fake Elasticsearch client after close")

    def close(self):
        self.close_calls += 1
        self.closed = True


class FakeESIndex:
    def __init__(self, client, index_name, **bulk_index_args):
        self.client = client
        self.index_name = index_name

    def mget(self, ids):
        self.client.ensure_open()
        for _id in ids:
            if _id in self.client.documents:
                yield copy.deepcopy(self.client.documents[_id])

    def mexists(self, ids):
        self.client.ensure_open()
        for _id in ids:
            yield SimpleNamespace(id=_id, exists=_id in self.client.documents)

    def mindex(self, docs):
        self.client.ensure_open()
        docs = list(docs)
        for doc in docs:
            self.client.documents[doc["_id"]] = copy.deepcopy(doc)
        return len(docs)


def make_cached_backends(monkeypatch, indexer_task_module, source_documents, indexed_documents=()):
    from biothings.utils import mongo as mongo_utils

    mongo_clients = []
    es_clients = []

    def make_mongo_client(**kwargs):
        client = FakeMongoClient(source_documents)
        mongo_clients.append(client)
        return client

    def make_es_client(**kwargs):
        client = FakeElasticsearchClient(indexed_documents)
        es_clients.append(client)
        return client

    def fake_doc_feeder(collection, *, query, **kwargs):
        collection.database.client.ensure_open()
        for _id in query["_id"]["$in"]:
            if _id in collection.documents:
                yield copy.deepcopy(collection.documents[_id])

    monkeypatch.setattr(mongo_utils, "_client_cache", {})
    monkeypatch.setattr(indexer_task_module, "MongoClient", make_mongo_client)
    monkeypatch.setattr(indexer_task_module, "Elasticsearch", make_es_client)
    monkeypatch.setattr(indexer_task_module, "ESIndex", FakeESIndex)
    monkeypatch.setattr(indexer_task_module, "doc_feeder", fake_doc_feeder)

    return SimpleNamespace(
        module=indexer_task_module,
        es=partial(
            indexer_task_module._get_es_client,
            {"hosts": ["http://fake-elasticsearch:9200"]},
            {},
            "test-index",
        ),
        mongo=partial(
            indexer_task_module._get_mg_client,
            {"host": "fake-mongodb", "port": 27017},
            "test-database",
            "test-collection",
        ),
        es_clients=es_clients,
        mongo_clients=mongo_clients,
    )


def run_sequential_tasks(backends, mode, batches):
    counts = []
    for ids in batches:
        task = backends.module.IndexingTask(backends.es, backends.mongo, ids, mode=mode)
        counts.append(task.dispatch())

        assert len(backends.es_clients) == 1
        assert len(backends.mongo_clients) == 1
        assert backends.es_clients[0].closed is False
        assert backends.mongo_clients[0].closed is False
        assert backends.es_clients[0].close_calls == 0
        assert backends.mongo_clients[0].close_calls == 0

    assert backends.es().client is backends.es_clients[0]
    assert backends.mongo().database.client is backends.mongo_clients[0]
    return counts


def test_index_tasks_reuse_cached_clients_without_closing_them(monkeypatch, indexer_task_module):
    too_long_id = "x" * 513
    source_documents = [
        {"_id": "index-1", "value": 1},
        {"_id": "index-2", "value": 2},
    ]
    backends = make_cached_backends(monkeypatch, indexer_task_module, source_documents)

    counts = run_sequential_tasks(backends, "index", [("index-1",), ("index-2", too_long_id)])

    assert counts == [1, 2]
    assert backends.es_clients[0].documents == {
        "index-1": {"_id": "index-1", "value": 1},
        "index-2": {"_id": "index-2", "value": 2},
    }


def test_merge_tasks_reuse_cached_clients_without_closing_them(monkeypatch, indexer_task_module):
    source_documents = [
        {"_id": "existing-1", "value": "new-1", "_timestamp": "ignored"},
        {"_id": "new-1", "value": "new-1", "_timestamp": "ignored"},
        {"_id": "existing-2", "value": "new-2", "_timestamp": "ignored"},
        {"_id": "new-2", "value": "new-2", "_timestamp": "ignored"},
    ]
    indexed_documents = [
        {"_id": "existing-1", "value": "old", "preserved": 1},
        {"_id": "existing-2", "value": "old", "preserved": 2},
    ]
    backends = make_cached_backends(monkeypatch, indexer_task_module, source_documents, indexed_documents)

    counts = run_sequential_tasks(
        backends,
        "merge",
        [("existing-1", "new-1"), ("existing-2", "new-2")],
    )

    assert counts == [2, 2]
    assert backends.es_clients[0].documents == {
        "existing-1": {"_id": "existing-1", "value": "new-1", "preserved": 1},
        "new-1": {"_id": "new-1", "value": "new-1"},
        "existing-2": {"_id": "existing-2", "value": "new-2", "preserved": 2},
        "new-2": {"_id": "new-2", "value": "new-2"},
    }


def test_resume_tasks_reuse_cached_clients_without_closing_them(monkeypatch, indexer_task_module):
    source_documents = [
        {"_id": "missing-1", "value": 1},
        {"_id": "missing-2", "value": 2},
    ]
    indexed_documents = [
        {"_id": "existing-1", "value": "preserved-1"},
        {"_id": "existing-2", "value": "preserved-2"},
    ]
    backends = make_cached_backends(monkeypatch, indexer_task_module, source_documents, indexed_documents)

    counts = run_sequential_tasks(
        backends,
        "resume",
        [("existing-1", "missing-1"), ("existing-2", "missing-2")],
    )

    assert counts == [2, 2]
    assert backends.es_clients[0].documents == {
        "existing-1": {"_id": "existing-1", "value": "preserved-1"},
        "missing-1": {"_id": "missing-1", "value": 1},
        "existing-2": {"_id": "existing-2", "value": "preserved-2"},
        "missing-2": {"_id": "missing-2", "value": 2},
    }
