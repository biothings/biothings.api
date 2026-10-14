import pytest

from biothings.utils.diff import two_docs_iterator


class StubBackend:
    def __init__(self, documents):
        self.documents = documents
        self.requests = []

    def mget_from_ids(self, ids, asiter=False):
        self.requests.append(list(ids))
        requested_ids = set(ids)
        documents = [document for document in self.documents if document["_id"] in requested_ids]
        return iter(documents) if asiter else documents


class UnfilteredStubBackend(StubBackend):
    def mget_from_ids(self, ids, asiter=False):
        self.requests.append(list(ids))
        return iter(self.documents) if asiter else self.documents


def test_two_docs_iterator_pairs_unordered_ids_in_request_order():
    first = StubBackend(
        [
            {"_id": 1, "value": "first-1"},
            {"_id": "a", "value": "first-a"},
        ]
    )
    second = StubBackend(
        [
            {"_id": "a", "value": "second-a"},
            {"_id": 1, "value": "second-1"},
        ]
    )

    pairs = list(two_docs_iterator(first, second, ["a", 1]))

    assert [(doc1["_id"], doc2["_id"]) for doc1, doc2 in pairs] == [("a", "a"), (1, 1)]


def test_two_docs_iterator_rejects_different_backend_ids():
    first = StubBackend([{"_id": "a"}, {"_id": "b"}])
    second = StubBackend([{"_id": "a"}])

    with pytest.raises(ValueError, match=r"only_in_first=\['b'\]"):
        list(two_docs_iterator(first, second, ["a", "b"]))


@pytest.mark.parametrize("duplicate_backend", ["first", "second"])
def test_two_docs_iterator_rejects_duplicate_backend_ids(duplicate_backend):
    documents = [{"_id": "a"}, {"_id": "b"}]
    duplicates = [{"_id": "a"}, {"_id": "a"}, {"_id": "b"}]
    first = StubBackend(duplicates if duplicate_backend == "first" else documents)
    second = StubBackend(duplicates if duplicate_backend == "second" else documents)

    with pytest.raises(ValueError, match=rf"Duplicate document ID 'a' returned by the {duplicate_backend} backend"):
        list(two_docs_iterator(first, second, ["a", "b"]))


def test_two_docs_iterator_ignores_ids_missing_from_both_backends():
    first = StubBackend([{"_id": "a", "value": "first"}])
    second = StubBackend([{"_id": "a", "value": "second"}])

    pairs = list(two_docs_iterator(first, second, ["missing", "a"]))

    assert [(doc1["_id"], doc2["_id"]) for doc1, doc2 in pairs] == [("a", "a")]


def test_two_docs_iterator_rejects_unrequested_backend_ids():
    first = UnfilteredStubBackend([{"_id": "a"}, {"_id": "unexpected"}])
    second = UnfilteredStubBackend([{"_id": "a"}, {"_id": "unexpected"}])

    with pytest.raises(ValueError, match=r"unexpected document IDs: \['unexpected'\]"):
        list(two_docs_iterator(first, second, ["a"]))


def test_two_docs_iterator_deduplicates_requested_ids_within_a_batch():
    first = StubBackend([{"_id": "a"}, {"_id": "b"}])
    second = StubBackend([{"_id": "b"}, {"_id": "a"}])

    pairs = list(two_docs_iterator(first, second, ["a", "a", "b"]))

    assert [(doc1["_id"], doc2["_id"]) for doc1, doc2 in pairs] == [("a", "a"), ("b", "b")]
    assert first.requests == [["a", "b"]]
    assert second.requests == [["a", "b"]]
