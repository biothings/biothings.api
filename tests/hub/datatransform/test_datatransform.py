import networkx as nx
import pytest

import biothings.utils.mongo as mongo
from biothings.hub.datatransform import CIIDStruct, DataTransform, DataTransformMDB as KeyLookup


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_simple(simple_graph: nx.DiGraph):
    """
    Simple test for key lookup - artificial document.

    The network contains a cycle that is avoided by the networkx algorithm.
    :return:
    """

    @KeyLookup(simple_graph, "a", ["d", "e"])
    def load_document(doc_lst):
        yield from doc_lst

    # Initial Test Case
    doc_lst = [{"_id": "a:1234"}]
    res_lst = load_document(doc_lst)

    res = next(res_lst)
    assert res["_id"] == "d:1234"

    # Verify that the generator is out of documents
    with pytest.raises(StopIteration):
        next(res_lst)


@pytest.mark.xfail()
def test_one2many(one_to_many_graph: nx.DiGraph):
    """
    test for one to many key lookup - artificial document.
    :return:
    """
    doc_lst = [{"input_key": "a:1234"}]

    @KeyLookup(one_to_many_graph, [("aa", "input_key")], ["cc"])
    def load_document():
        yield from doc_lst

    # Initial Test Case
    res_lst = [d for d in load_document()]

    # Check for expected keys
    # There are 2 branches along the document path
    answer_lst = []
    answer_lst.append(res_lst[0]["_id"])
    answer_lst.append(res_lst[1]["_id"])
    answer_lst.append(res_lst[2]["_id"])

    assert "c:1234" in answer_lst
    assert "c:01" in answer_lst
    assert "c:02" in answer_lst


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_input_types(simple_graph: nx.DiGraph):
    """
    test for input_types - artificial documents.
    :return:
    """
    # Initial Test Case
    doc_lst = [{"a": "a:1234"}, {"b": "b:1234"}]

    @KeyLookup(simple_graph, [("a", "a"), ("b", "b")], ["d", "e"])
    def load_document(doc_lst):
        yield from doc_lst

    res_lst = load_document(doc_lst)

    for res in res_lst:
        print(res)
        # Check for expected keys
        assert res["_id"] == "d:1234"


@pytest.mark.xfail()
def test_weights(weighted_graph: nx.DiGraph):
    """
    Simple test for key lookup - artificial document.

    The network contains a shortcut path with a high weight
    that should be avoided.
    :return:
    """

    @KeyLookup(weighted_graph, "aaa", ["eee"])
    def load_document(doc_lst):
        yield from doc_lst

    # Initial Test Case
    doc_lst = [{"_id": "a:1234"}]
    res_lst = load_document(doc_lst)

    for res in res_lst:
        # Check for expected key
        assert res["_id"] == "e:1234"


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_interface(simple_graph: nx.DiGraph):
    """
    Simple test for key lookup - artificial document.

    This test is intended to test multiple douments being passed.
    :return:
    """

    @KeyLookup(simple_graph, "a", ["d", "e"])
    def load_document(data_folder):
        doc_lst = [{"_id": "a:1234"}, {"_id": "a:1234"}, {"_id": "a:1234"}]
        yield from doc_lst

    # Test a list being passed with three documents
    res_lst = load_document("data/folder/")
    res1 = next(res_lst)
    res2 = next(res_lst)
    res3 = next(res_lst)
    assert res1["_id"] == "d:1234"
    assert res2["_id"] == "d:1234"
    assert res3["_id"] == "d:1234"


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_skip_on_failure(simple_graph: nx.DiGraph):
    """
    Simple test for key lookup skip_on_failure.

    This test tests the skip_on_failure option which skips documents
    where lookup was unsuccessful.

    :return:
    """

    @KeyLookup(simple_graph, "a", ["d", "e"], skip_on_failure=True)
    def load_document(data_folder):
        doc_lst = [{"_id": "a:1234"}, {"_id": "a:invalid"}, {"_id": "a:1234"}]
        yield from doc_lst

    # Test a list being passed with 3 documents, 2 are returned, 1 is skipped
    res_lst = load_document("data/folder/")
    res1 = next(res_lst)
    res2 = next(res_lst)
    assert res1["_id"] == "d:1234"
    assert res2["_id"] == "d:1234"

    # Verify that the generator is out of documents
    with pytest.raises(StopIteration):
        next(res_lst)


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_strangecases(simple_graph: nx.DiGraph):
    """
    Test invalid input that should generate exceptions.
    :return:
    """
    # invalid input-type
    with pytest.raises(ValueError):
        @KeyLookup(simple_graph, "a-invalid", ["d", "e"], skip_on_failure=True)
        def load_document(data_folder):
            doc_lst = [{"_id": "a:1234"}, {"_id": "a:invalid"}, {"_id": "a:1234"}]
            yield from doc_lst

    # Invalid output-type
    with pytest.raises(ValueError):
        @KeyLookup(simple_graph, "a", ["d-invalid", "e"], skip_on_failure=True)
        def load_document(data_folder):  # noqa F811
            doc_lst = [{"_id": "a:1234"}, {"_id": "a:invalid"}, {"_id": "a:1234"}]
            yield from doc_lst

    # Invalid graph
    with pytest.raises(ValueError):
        @KeyLookup(graph_invalid, "a", ["d-invalid", "e"], skip_on_failure=True)
        def load_document(data_folder):  # noqa F811
            doc_lst = [{"_id": "a:1234"}, {"_id": "a:invalid"}, {"_id": "a:1234"}]
            yield from doc_lst


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_skip_w_regex(simple_graph):
    """
    Test the skip_w_regex option.
    :return:
    """
    doc_lst = [{"_id": "a:1234"}]

    @KeyLookup(simple_graph, "a", ["d"], skip_w_regex="a:")
    def load_document(data_folder):
        yield from doc_lst

    res_lst = load_document("data/folder/")
    res = next(res_lst)
    assert res["_id"] == "a:1234"


@pytest.mark.xfail()
def test_mix_mdb_api(mixed_backend_graph: nx.DiGraph):
    """
    Test with mixed lookups between MongoDB and API
    :return:
    """
    doc_lst = [{"_id": "start1"}]

    @KeyLookup(mixed_backend_graph, "mix1", ["mix3"])
    def load_document(data_folder):
        yield from doc_lst

    res_lst = load_document("data/folder/")
    res = next(res_lst)
    assert res["_id"] == "end1"


@pytest.mark.xfail()
def test_pubchem_api(pubchem_api_graph: nx.DiGraph):
    """
    Test 'inchi' to 'inchikey' conversion using mychem.info
    :return:
    """
    doc_lst = [{"_id": "InChI=1S/C8H9NO2/c1-6(10)9-7-2-4-8(11)5-3-7/h2-5,11H,1H3,(H,9,10)"}]

    @KeyLookup(pubchem_api_graph, "inchi", ["inchikey"])
    def load_document(data_folder):
        yield from doc_lst

    res_lst = load_document("data/folder/")
    res = next(res_lst)
    assert res["_id"] == "RZVAJINKPMORJF-UHFFFAOYSA-N"


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_input_source_fields(simple_graph: nx.DiGraph):
    """
    Test input source field options.  These are complicated tests with input source field
    of varying depth and complexity.  Multiple documents are converted.
    Conversion to InchiKey is performed.
    :return:
    """
    doc_lst = [
        {
            "_id": "test2_drugbank",
            "pharmgkb": {
                "xref": {
                    "drugbank_id": "a:1234",
                }
            },
        }
    ]

    @KeyLookup(simple_graph, [("a", "pharmgkb.xref.drugbank_id")], ["d", "e"])
    def load_document(data_folder):
        yield from doc_lst

    res_lst = load_document("data/folder/")
    r = next(res_lst)
    assert r["_id"] == "d:1234"


@pytest.mark.xfail(reason="Broken test - stale, perhaps a data issue")
def test_long_doc_lst(mychem_api_graph: nx.DiGraph):
    """
    Test a document list containing 12 entries.  Verify that the correct
    number of documents are returned.
    :return:
    """

    # Long document list - created manually for a unique test
    doc_lst = [
        {
            "_id": "test1",
            "chebi": "CHEBI:1391",
        },
        {
            "_id": "test2",
            "pubchem": "178014",
        },
        {
            # this test document should still be returned
            "_id": "test3",
        },
        {
            "_id": "test4",
            "drugbank": "DB11940",
        },
        {
            "_id": "test5",
            "chebi": "CHEBI:28689",
        },
        {
            "_id": "test6",
            "pubchem": "164045",
        },
        {"_id": "test7", "drugbank": "DB01076"},
        {
            "_id": "test8",
            "drugbank": "DB03510",
        },
        {
            "_id": "test9",
            "pubchem": "40467070",
        },
        {
            "_id": "test10",
            "chebi": "CHEBI:135847",
        },
        {
            "_id": "test11",
            "pubchem": "10484732",
        },
        {
            "_id": "test12",
            "pubchem": "23305354",
        },
    ]

    answers = [
        "SHXWCVYOXRDMCX-UHFFFAOYSA-N",
        "CXHDSLQCNYLQND-XQRIHRDZSA-N",
        "test3",
        "XMYKNCNAZKMVQN-NYYWCZLTSA-N",
        "FMGSKLZLMKYGDP-USOAJAOKSA-N",
        "YAFGHMIAFYQSCF-UHFFFAOYSA-N",
        "XUKUURHRXDUEBC-KAYWLYCHSA-N",
        "RXRZOKQPANIEDW-KQYNXXCUSA-N",
        "BNQDCRGUHNALGH-ZCFIWIBFSA-N",
        "CGVWPQOFHSAKRR-NDEPHWFRSA-N",
        "PCZHWPSNPWAQNF-LMOVPXPDSA-N",
        "FABUFPQFXZVHFB-CFWQTKTJSA-N",
    ]

    # Test a list being passed with 12 documents
    @KeyLookup(mychem_api_graph, [("chebi", "chebi"), ("drugbank", "drugbank"), ("pubchem", "pubchem")], ["inchikey"])
    def load_document(data_folder):
        yield from doc_lst

    res_lst = load_document("data/folder/")
    res_cnt = 0
    for res in res_lst:
        res_cnt += 1
        if not res["_id"] in answers:
            print(res)
        assert res["_id"] in answers
    assert res_cnt == 12


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_regex(regex_edge_graph: nx.DiGraph):
    """
    Test the RegExEdge in a network.
    """

    @KeyLookup(regex_edge_graph, "a", ["bregex"])
    def load_document(doc_lst):
        yield from doc_lst

    # Initial Test Case
    doc_lst = [{"_id": "a:1234"}]
    res_lst = load_document(doc_lst)

    res = next(res_lst)
    assert res["_id"] == "bregex:1234"


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_failure1(simple_graph: nx.DiGraph):
    """
    Test behavior on lookup failure
    """

    @KeyLookup(simple_graph, "a", ["e"])
    def load_document(doc_lst):
        yield from doc_lst

    # Failure Test Case
    doc_lst = [{"_id": "a:f1"}]
    res_lst = load_document(doc_lst)

    res = next(res_lst)
    assert res["_id"] == "a:f1"


@pytest.mark.xfail(reason="Implement a `CopyEdge` class")
def test_copyid(simple_graph: nx.DiGraph):
    """
    Test behavior on lookup lookup copy.
    Lookup fails, second identifier value is copied over.
    """

    @KeyLookup(simple_graph, ["a", ("b", "b_id")], ["e", "b"])
    def load_document(doc_lst):
        yield from doc_lst

    # Copy from second field
    doc_lst = [{"_id": "a:f1", "b_id": "b:f1"}]
    res_lst = load_document(doc_lst)

    res = next(res_lst)
    assert res["_id"] == "b:f1"


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_debug_mode(simple_graph: nx.DiGraph):
    """
    Test debug mode 'a' to 'e' conversion using the simple test
    :return:
    """
    # the 'debug' parameter was moved from __init__ to __call__
    keylookup = KeyLookup(simple_graph, "a", ["e"])

    def load_document(doc_lst):
        yield from doc_lst

    # Initial Test Case
    doc_lst = [{"_id": "a:1234"}, {"_id": "skip_me"}]

    # Apply the KeyLookup decorator
    res_lst = keylookup(load_document, debug=["a:1234"])(doc_lst)

    res = next(res_lst)
    assert res["_id"] == "e:1234"

    # Verify that the debug information is actually inside of the resulting document
    assert "dt_debug" in res

    # Verify that the generator is out of documents
    with pytest.raises(StopIteration):
        next(res_lst)


@pytest.mark.xfail(reason="MongoDBEdge isn't indexed causing long datatransform Error")
def test_case_insensitive(case_insensitive_graph: nx.DiGraph):
    """
    Case insensitive test for key lookup - artificial document.
    :return:
    """

    @KeyLookup(case_insensitive_graph, "a", ["b"], idstruct_class=CIIDStruct)
    def load_document(doc_lst):
        yield from doc_lst

    # Test Case - upper case A in id
    doc_lst = [{"_id": "A:1234"}]
    res_lst = load_document(doc_lst)

    res = next(res_lst)
    assert res["_id"] == "b:1234"

    # Verify that the generator is out of documents
    with pytest.raises(StopIteration):
        next(res_lst)


def test_id_priority_list():
    """
    Unit test for id_priority_list and related methods.
    """
    input_types = [("1", "doc.a"), ("5", "doc.b"), ("10", "doc.c"), ("15", "doc.d")]
    output_types = ["1", "5", "10", "15"]
    keylookup = DataTransform(input_types, output_types)

    # set th id_priority_list using the setter and verify that
    # that input_types and output_types are in the correct order.
    keylookup.id_priority_list = ["10", "1"]

    # the resulting order for both lists should be 10, 1, 5, 15
    # - 10, and 1 are brought to the beginning of the list
    # - and the order of 5 and 15 remains the same
    assert keylookup.input_types[0][0] == "10"
    assert keylookup.input_types[1][0] == "1"
    assert keylookup.input_types[2][0] == "5"
    assert keylookup.input_types[3][0] == "15"
    assert keylookup.output_types[0] == "10"
    assert keylookup.output_types[1] == "1"
    assert keylookup.output_types[2] == "5"
    assert keylookup.output_types[3] == "15"
