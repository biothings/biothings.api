"""
Utils to compare two list of gene documents, no require to setup Biothings Hub.
"""

import json
import time

from .common import filter_dict, timesofar


def _index_documents_by_id(documents, backend_name):
    documents_by_id = {}
    for document in documents:
        document_id = document["_id"]
        if document_id in documents_by_id:
            raise ValueError(f"Duplicate document ID {document_id!r} returned by the {backend_name} backend")
        documents_by_id[document_id] = document
    return documents_by_id


def _validate_matching_document_ids(docs1, docs2, requested_ids):
    if docs1.keys() != docs2.keys():
        only_in_first = [_id for _id in docs1 if _id not in docs2]
        only_in_second = [_id for _id in docs2 if _id not in docs1]
        raise ValueError(
            "The backends returned different document IDs: "
            f"only_in_first={only_in_first!r}, only_in_second={only_in_second!r}"
        )
    unexpected_ids = [_id for _id in docs1 if _id not in requested_ids]
    if unexpected_ids:
        raise ValueError(f"The backends returned unexpected document IDs: {unexpected_ids!r}")


def diff_doc(doc_1, doc_2, exclude_attrs=None):
    exclude_attrs = exclude_attrs or ["_timestamp"]

    diff_d = {"update": {}, "delete": [], "add": {}}
    if exclude_attrs:
        doc_1 = filter_dict(doc_1, exclude_attrs)
        doc_2 = filter_dict(doc_2, exclude_attrs)
    for attr in set(doc_1) | set(doc_2):
        if exclude_attrs and attr in exclude_attrs:
            continue
        if attr in doc_1 and attr in doc_2:
            _v1 = doc_1[attr]
            _v2 = doc_2[attr]
            if _v1 != _v2:
                diff_d["update"][attr] = _v2
        elif attr in doc_1 and attr not in doc_2:
            diff_d["delete"].append(attr)
        else:
            diff_d["add"][attr] = doc_2[attr]
    if diff_d["update"] or diff_d["delete"] or diff_d["add"]:
        return diff_d


def full_diff_doc(doc_1, doc_2, exclude_attrs=None):
    exclude_attrs = exclude_attrs or ["_timestamp"]

    diff_d = {"update": {}, "delete": [], "add": {}}
    if exclude_attrs:
        doc_1 = filter_dict(doc_1, exclude_attrs)
        doc_2 = filter_dict(doc_2, exclude_attrs)
    for attr in set(doc_1) | set(doc_2):
        if exclude_attrs and attr in exclude_attrs:
            continue
        if attr in doc_1 and attr in doc_2:
            _v1 = doc_1[attr]
            _v2 = doc_2[attr]
            difffound = False
            if isinstance(_v1, dict) and isinstance(_v2, dict):
                if full_diff_doc(_v1, _v2, exclude_attrs):
                    difffound = True
            elif isinstance(_v1, list) and isinstance(_v2, list):
                # there can be unhashable/unordered dict in these lists
                for i in _v1:
                    if i not in _v2:
                        difffound = True
                        break
                # check the other way
                if not difffound:
                    for i in _v2:
                        if i not in _v1:
                            difffound = True
                            break
            elif _v1 != _v2:
                difffound = True

            if difffound:
                diff_d["update"][attr] = _v2

        elif attr in doc_1 and attr not in doc_2:
            diff_d["delete"].append(attr)
        else:
            diff_d["add"][attr] = doc_2[attr]
    if diff_d["update"] or diff_d["delete"] or diff_d["add"]:
        return diff_d


def two_docs_iterator(docs1, docs2, id_list, step=10000, verbose=False):
    t0 = time.time()
    n = len(id_list)
    for i in range(0, n, step):
        t1 = time.time()
        if verbose:
            print("Processing %d-%d documents..." % (i + 1, min(i + step, n)), end="")
        _ids = list(dict.fromkeys(id_list[i : i + step]))
        requested_ids = set(_ids)
        docs1_by_id = _index_documents_by_id(
            (doc for doc in docs1 if doc["_id"] in requested_ids),
            "first",
        )
        docs2_by_id = _index_documents_by_id(
            (doc for doc in docs2 if doc["_id"] in requested_ids),
            "second",
        )
        _validate_matching_document_ids(docs1_by_id, docs2_by_id, requested_ids)
        for _id in _ids:
            if _id in docs1_by_id:
                yield docs1_by_id[_id], docs2_by_id[_id]
        if verbose:
            print("Done.[%.1f%%,%s]" % (i * 100.0 / n, timesofar(t1)))
    if verbose:
        print("=" * 20)
        print("Finished.[total time: %s]" % timesofar(t0))


def diff_docs(docs1, docs2, ids, fastdiff=False, diff_func=full_diff_doc):
    """if fastdiff is True, only compare the whole doc,
    do not traverse into each attributes.
    """
    _updates = []
    for doc1, doc2 in two_docs_iterator(docs1, docs2, ids):
        assert doc1["_id"] == doc2["_id"], repr((ids, len(ids)))
        if fastdiff:
            if doc1 != doc2:
                _updates.append({"_id": doc1["_id"]})
        else:
            _diff = diff_func(doc1, doc2)
            if _diff:
                _diff["_id"] = doc1["_id"]
                _updates.append(_diff)
    return _updates


def normalize_document(docs):
    """
    If docs which is generated by MongoExport, or parsed by DataPlugin, is already in right structure.
    Only need to restructure ES Index data.

    Example output:
    [
        {
            "_id": "PA123",
            "source_name": [
                ...
            ],
        },
    ]

    """
    if not isinstance(docs, (list, tuple)):
        raise Exception("Invalid docs. Expect a list of dict")

    if len(docs) == 0:
        return []

    first_item = docs[0]
    assert "_id" in first_item, "Data struct is invalid. Missing _id field"

    result = docs
    # If data is an json exported from ES Index, then restructure it to be like datasource.
    if "_index" in first_item:
        result = [{"_id": doc["_id"], **doc["_source"]} for doc in docs]

    return result


def get_id_set(docs):
    ids = [doc["_id"] for doc in docs]
    return set(ids)


def diff_collections(docs1, docs2):
    """
    data1, data2 are one of exported data from mongo's collection, or ES index

    To export data from mongo, should use mongoexport command with jsonArray and type=json flags,
    in order to generate valid json file
    """

    _docs1 = normalize_document(docs1)
    _docs2 = normalize_document(docs2)

    id_s1 = get_id_set(_docs1)
    id_s2 = get_id_set(_docs2)
    print("Size of collection 1:\t", len(id_s1))
    print("Size of collection 2:\t", len(id_s2))

    id_in_1 = id_s1 - id_s2
    id_in_2 = id_s2 - id_s1
    id_common = id_s1 & id_s2
    print("# of docs found only in collection 1:\t", len(id_in_1))
    print("# of docs found only in collection 2:\t", len(id_in_2))
    print("# of docs found in both collections:\t", len(id_common))

    print("Comparing matching docs...")
    _updates = []
    if len(id_common) > 0:
        _updates = diff_docs(_docs1, _docs2, list(id_common))
        print("Done. [{} docs changed]".format(len(_updates)))

    _deletes = []
    if len(id_in_1) > 0:
        _deletes = sorted(id_in_1)

    _adds = []
    if len(id_in_2) > 0:
        _adds = sorted(id_in_2)

    changes = {"update": _updates, "delete": _deletes, "add": _adds}
    return changes


def diff_json_files(fpath1, fpath2):
    print("Loading data from json files")
    with open(fpath1) as f:
        docs1 = json.load(f)

    with open(fpath2) as f:
        docs2 = json.load(f)

    print("Do compare 2 docs")
    return diff_collections(docs1, docs2)
