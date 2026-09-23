import asyncio
import copy
import importlib

import pytest


class FakeSrcBuildCollection:
    def __init__(self, docs):
        self.docs = {doc["_id"]: copy.deepcopy(doc) for doc in docs}

    def find(self, query=None):
        return [copy.deepcopy(doc) for doc in self.docs.values() if self._matches(doc, query or {})]

    def find_one(self, query):
        for doc in self.docs.values():
            if self._matches(doc, query):
                return copy.deepcopy(doc)
        return None

    def replace_one(self, query, doc):
        existing = self.find_one(query)
        assert existing, f"document not found: {query}"
        self.docs[existing["_id"]] = copy.deepcopy(doc)

    def update(self, query, update):
        doc = self._find_stored(query)
        assert doc, f"document not found: {query}"
        for op, changes in update.items():
            if op == "$push":
                for path, value in changes.items():
                    parent, key = self._ensure_parent(doc, path)
                    parent.setdefault(key, []).append(copy.deepcopy(value))
            elif op == "$addToSet":
                for path, value in changes.items():
                    parent, key = self._ensure_parent(doc, path)
                    values = parent.setdefault(key, [])
                    if value not in values:
                        values.append(copy.deepcopy(value))
            elif op == "$pull":
                for path, value in changes.items():
                    parent, key = self._ensure_parent(doc, path)
                    parent[key] = [item for item in parent.get(key, []) if item != value]
            elif op == "$unset":
                for path in changes:
                    parent, key = self._ensure_parent(doc, path)
                    parent.pop(key, None)
            else:
                raise NotImplementedError(op)

    def _find_stored(self, query):
        for doc in self.docs.values():
            if self._matches(doc, query):
                return doc
        return None

    def _matches(self, doc, query):
        return all(self._get(doc, path) == expected for path, expected in query.items())

    def _get(self, doc, path):
        value = doc
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
        return value

    def _ensure_parent(self, doc, path):
        value = doc
        parts = path.split(".")
        for part in parts[:-1]:
            value = value.setdefault(part, {})
        return value, parts[-1]


def make_build_doc(index=None):
    doc = {
        "_id": "test_build",
        "target_name": "test_build",
        "build_config": {
            "name": "test",
            "doc_type": "doc",
        },
        "mapping": {},
        "_meta": {},
        "jobs": [],
    }
    if index is not None:
        doc["index"] = index
    return doc


@pytest.fixture
def dataindex_modules(root_configuration, tmp_path):
    root_configuration.override(
        {
            "__file__": str(tmp_path / "test_config.py"),
            "HUB_DB_BACKEND": {
                "module": "biothings.utils.sqlite3",
                "sqlite_db_folder": str(tmp_path),
            },
            "DATA_HUB_DB_DATABASE": "indexer_lifecycle_hubdb",
        }
    )

    importlib.import_module("biothings.hub")._config_for_app(root_configuration)
    indexer_module = importlib.import_module("biothings.hub.dataindex.indexer")
    snapshooter_module = importlib.import_module("biothings.hub.dataindex.snapshooter")

    yield indexer_module, snapshooter_module

    root_configuration.reset()


def make_lifecycle_indexer_class(indexer_module):
    class LifecycleIndexer(indexer_module.Indexer):
        async def pre_index(self, *args, mode, **kwargs):
            return {
                "__REPLACE__": True,
                "host": self.es_client_args.get("hosts"),
                "environment": self.env_name,
            }

        async def do_index(self, job_manager, batch_size, ids, mode, **kwargs):
            return {
                "count": 2,
                "created_at": "index-step",
            }

        async def post_index(self, *args, **kwargs):
            return {
                "post": {
                    "embeddings": "done",
                    "force_merge": "done",
                }
            }

    return LifecycleIndexer


def make_indexer(collection, indexer_class):
    build_doc = collection.find_one({"_id": "test_build"})
    return indexer_class(
        build_doc,
        {
            "name": "local",
            "args": {
                "hosts": "http://localhost:9200",
            },
        },
        "test_index",
    )


def test_full_index_flow_registers_ready_index_only_after_post(monkeypatch, dataindex_modules):
    indexer_module, _ = dataindex_modules
    lifecycle_indexer = make_lifecycle_indexer_class(indexer_module)
    collection = FakeSrcBuildCollection([make_build_doc()])
    monkeypatch.setattr(indexer_module, "get_src_build", lambda: collection)
    observations = []

    class ObservingIndexer(lifecycle_indexer):
        async def post_index(self, *args, **kwargs):
            build_doc = collection.find_one({"_id": self.build_name})
            observations.append(copy.deepcopy(build_doc.get("index", {})))
            return await super().post_index(*args, **kwargs)

    indexer = make_indexer(collection, ObservingIndexer)

    asyncio.run(indexer.index(object(), steps=("pre", "index", "post")))

    assert "test_index" not in observations[0]
    build_doc = collection.find_one({"_id": "test_build"})
    assert build_doc["index"]["test_index"] == {
        "host": "http://localhost:9200",
        "environment": "local",
        "count": 2,
        "created_at": "index-step",
        "post": {
            "embeddings": "done",
            "force_merge": "done",
        },
    }


def test_index_without_post_registers_ready_index_after_index(monkeypatch, dataindex_modules):
    indexer_module, _ = dataindex_modules
    lifecycle_indexer = make_lifecycle_indexer_class(indexer_module)
    collection = FakeSrcBuildCollection([make_build_doc()])
    monkeypatch.setattr(indexer_module, "get_src_build", lambda: collection)

    indexer = make_indexer(collection, lifecycle_indexer)

    asyncio.run(indexer.index(object(), steps=("pre", "index")))

    build_doc = collection.find_one({"_id": "test_build"})
    assert build_doc["index"]["test_index"] == {
        "host": "http://localhost:9200",
        "environment": "local",
        "count": 2,
        "created_at": "index-step",
    }


def test_post_failure_after_index_success_does_not_expose_ready_index(monkeypatch, dataindex_modules):
    indexer_module, _ = dataindex_modules
    lifecycle_indexer = make_lifecycle_indexer_class(indexer_module)
    collection = FakeSrcBuildCollection(
        [
            make_build_doc(
                index={
                    "test_index": {
                        "environment": "local",
                        "count": 99,
                    }
                }
            )
        ]
    )
    monkeypatch.setattr(indexer_module, "get_src_build", lambda: collection)

    class FailingPostIndexer(lifecycle_indexer):
        async def post_index(self, *args, **kwargs):
            raise RuntimeError("force merge failed")

    indexer = make_indexer(collection, FailingPostIndexer)

    with pytest.raises(RuntimeError, match="force merge failed"):
        asyncio.run(indexer.index(object(), steps=("pre", "index", "post")))

    build_doc = collection.find_one({"_id": "test_build"})
    assert "test_index" not in build_doc.get("index", {})
    assert build_doc["jobs"][-2]["step"] == "index"
    assert build_doc["jobs"][-2]["status"] == "success"
    assert build_doc["jobs"][-1]["step"] == "post-index"
    assert build_doc["jobs"][-1]["status"] == "failed"
    assert build_doc["jobs"][-1]["err"] == "force merge failed"


def test_snapshot_lookup_cannot_find_index_until_post_succeeds(monkeypatch, dataindex_modules):
    indexer_module, snapshooter = dataindex_modules
    lifecycle_indexer = make_lifecycle_indexer_class(indexer_module)
    collection = FakeSrcBuildCollection([make_build_doc()])
    monkeypatch.setattr(indexer_module, "get_src_build", lambda: collection)
    monkeypatch.setattr(snapshooter, "get_src_build", lambda: collection)
    snapshot_env = object.__new__(snapshooter.SnapshotEnv)
    snapshot_env.idxenv = "local"

    class SnapshotLookupIndexer(lifecycle_indexer):
        async def post_index(self, *args, **kwargs):
            with pytest.raises(ValueError, match="Not a hub-managed index"):
                snapshooter.SnapshotEnv._doc(snapshot_env, "test_index")
            return await super().post_index(*args, **kwargs)

    indexer = make_indexer(collection, SnapshotLookupIndexer)

    asyncio.run(indexer.index(object(), steps=("pre", "index", "post")))

    build_doc = snapshooter.SnapshotEnv._doc(snapshot_env, "test_index")
    assert build_doc["_id"] == "test_build"
