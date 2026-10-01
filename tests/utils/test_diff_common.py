import pytest

from biothings.utils.diff_common import two_docs_iterator


def test_two_docs_iterator_pairs_documents_in_request_order():
    first = [{"_id": 1}, {"_id": "a"}]
    second = [{"_id": "a"}, {"_id": 1}]

    pairs = list(two_docs_iterator(first, second, ["a", "a", "missing", 1]))

    assert [(doc1["_id"], doc2["_id"]) for doc1, doc2 in pairs] == [("a", "a"), (1, 1)]


def test_two_docs_iterator_rejects_different_document_ids():
    first = [{"_id": "a"}, {"_id": "b"}]
    second = [{"_id": "a"}]

    with pytest.raises(ValueError, match=r"only_in_first=\['b'\]"):
        list(two_docs_iterator(first, second, ["a", "b"]))


@pytest.mark.parametrize("duplicate_input", ["first", "second"])
def test_two_docs_iterator_rejects_duplicate_document_ids(duplicate_input):
    documents = [{"_id": "a"}, {"_id": "b"}]
    duplicates = [{"_id": "a"}, {"_id": "a"}, {"_id": "b"}]
    first = duplicates if duplicate_input == "first" else documents
    second = duplicates if duplicate_input == "second" else documents

    with pytest.raises(ValueError, match=rf"Duplicate document ID 'a' returned by the {duplicate_input} backend"):
        list(two_docs_iterator(first, second, ["a", "b"]))
