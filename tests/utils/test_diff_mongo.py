import threading

import pytest

from biothings.utils import mongo
from biothings.utils.backend import DocMongoBackend
from biothings.utils.diff import get_backend, two_docs_iterator


class RecordingBackend:
    def __init__(self, documents):
        self.documents = documents
        self.call_threads = []
        self.cursor_threads = []

    def mget_from_ids(self, ids, asiter=False):
        self.call_threads.append(threading.get_ident())
        requested_ids = set(ids)

        def cursor():
            self.cursor_threads.append(threading.get_ident())
            yield from (document for document in self.documents if document["_id"] in requested_ids)

        documents = cursor()
        return documents if asiter else list(documents)


class SynchronizingMongoBackend(DocMongoBackend):
    def __init__(self, documents, barrier, error=None):
        self.documents = documents
        self.barrier = barrier
        self.error = error
        self.call_threads = []
        self.cursor_threads = []
        self.finished = threading.Event()

    def mget_from_ids(self, ids, asiter=False):
        self.call_threads.append(threading.get_ident())
        requested_ids = set(ids)

        def cursor():
            self.cursor_threads.append(threading.get_ident())
            try:
                self.barrier.wait(timeout=5)
                if self.error:
                    raise self.error
                yield from (document for document in self.documents if document["_id"] in requested_ids)
            finally:
                self.finished.set()

        documents = cursor()
        return documents if asiter else list(documents)


def test_two_docs_iterator_overlaps_mongo_cursor_reads():
    barrier = threading.Barrier(2)
    first = SynchronizingMongoBackend([{"_id": "a", "value": "first"}], barrier)
    second = SynchronizingMongoBackend([{"_id": "a", "value": "second"}], barrier)
    calling_thread = threading.get_ident()

    pairs = list(two_docs_iterator(first, second, ["a"]))

    assert [(doc1["_id"], doc2["_id"]) for doc1, doc2 in pairs] == [("a", "a")]
    assert first.call_threads == first.cursor_threads
    assert second.call_threads == second.cursor_threads
    assert first.call_threads[0] != calling_thread
    assert second.call_threads[0] != calling_thread
    assert first.call_threads[0] != second.call_threads[0]


def test_two_docs_iterator_propagates_mongo_read_errors_after_joining_reads():
    class BackendReadError(Exception):
        pass

    barrier = threading.Barrier(2)
    first = SynchronizingMongoBackend([{"_id": "a"}], barrier, BackendReadError("read failed"))
    second = SynchronizingMongoBackend([{"_id": "a"}], barrier)

    with pytest.raises(BackendReadError, match="read failed"):
        list(two_docs_iterator(first, second, ["a"]))

    assert first.finished.is_set()
    assert second.finished.is_set()


def test_two_docs_iterator_keeps_non_mongo_backends_sequential():
    first = RecordingBackend([{"_id": "a"}])
    second = RecordingBackend([{"_id": "a"}])
    calling_thread = threading.get_ident()

    list(two_docs_iterator(first, second, ["a"]))

    assert first.call_threads == [calling_thread]
    assert first.cursor_threads == [calling_thread]
    assert second.call_threads == [calling_thread]
    assert second.cursor_threads == [calling_thread]


@pytest.mark.parametrize("backend_type", [DocMongoBackend.name, "mongodb"])
def test_get_backend_accepts_current_and_legacy_mongo_names(monkeypatch, backend_type):
    collection = object()
    database = {"collection": collection}
    client = {"database": database}
    monkeypatch.setattr(mongo, "_cached_client", lambda uri: client)

    backend = get_backend("mongodb://example", "database", "collection", backend_type)

    assert isinstance(backend, DocMongoBackend)
    assert backend.target_db is database
    assert backend.target_collection is collection


def test_get_backend_rejects_unsupported_backend_type():
    with pytest.raises(NotImplementedError, match="Backend type 'memory' not supported"):
        get_backend("mongodb://example", "database", "collection", "memory")
