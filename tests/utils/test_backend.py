from biothings.utils.backend import DocBackendBase, DocESBackend, DocMemoryBackend, DocMongoBackend


class FallbackBackend(DocBackendBase):
    def __init__(self, documents):
        self.documents = documents

    def mget_from_ids(self, ids):
        requested_ids = set(ids)
        return [document for document in self.documents if document["_id"] in requested_ids]


class StubMongoCollection:
    def __init__(self, documents):
        self.documents = documents
        self.find_calls = []

    def find(self, query, projection=None):
        self.find_calls.append((query, projection))
        requested_ids = set(query["_id"]["$in"])
        return [{"_id": document["_id"]} for document in self.documents if document["_id"] in requested_ids]


class StubESIndexer:
    def __init__(self, documents):
        self.documents = documents
        self.get_docs_calls = []

    def get_docs(self, ids, **kwargs):
        self.get_docs_calls.append((list(ids), kwargs))
        return iter(self.documents)


def test_base_get_existing_ids_falls_back_to_full_documents():
    backend = FallbackBackend([{"_id": "a", "value": 1}, {"_id": "b", "value": 2}])

    assert list(backend.get_existing_ids(["b"])) == ["b"]


def test_memory_get_existing_ids_preserves_requested_order():
    backend = DocMemoryBackend()
    backend.insert([{"_id": "a"}, {"_id": "b"}])

    assert list(backend.get_existing_ids(["b", "missing", "a"])) == ["b", "a"]


def test_mongo_get_existing_ids_uses_id_only_projection():
    collection = StubMongoCollection([{"_id": "a", "value": 1}, {"_id": "b", "value": 2}])
    backend = DocMongoBackend(target_db=None, target_collection=collection)

    assert list(backend.get_existing_ids(["b"])) == ["b"]
    assert collection.find_calls == [({"_id": {"$in": ["b"]}}, {"_id": 1})]


def test_es_get_existing_ids_disables_source_loading():
    indexer = StubESIndexer([{"_id": "b", "found": True}, {"_id": "a", "found": True}])
    backend = DocESBackend(indexer)

    assert list(backend.get_existing_ids(["a", "b"], step=50)) == ["b", "a"]
    assert indexer.get_docs_calls == [(["a", "b"], {"step": 50, "only_source": False, "source": False})]
