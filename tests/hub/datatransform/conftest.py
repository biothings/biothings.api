import networkx as nx
import pytest

@pytest.fixture(scope="module")
def graph_database_collections():
    """
    setup and teardown the expected mongodb structure for the datatransform tests
    """
    from biothings.utils import mongo
    # Collections for the first test
    db = mongo.get_src_db()
    db.create_collection("a")
    db.create_collection("b")
    db.create_collection("c")
    db.create_collection("d")
    db.create_collection("e")

    db["b"].insert({"b_id": "b:1234", "a_id": "a:1234"})
    db["c"].insert({"c_id": "c:1234", "b_id": "b:1234", "e_id": "e:1234"})
    db["d"].insert({"d_id": "d:1234", "c_id": "c:1234"})
    db["e"].insert({"e_id": "e:1234", "d_id": "d:1234"})

    # Collections for the second test (one2many)
    db.create_collection("aa")
    db.create_collection("bb")
    db.create_collection("cc")

    db["bb"].insert({"b_id": "b:1234", "a_id": "a:1234"})
    db["bb"].insert({"b_id": "b:5678", "a_id": "a:1234"})
    db["cc"].insert({"c_id": "c:1234", "b_id": "b:1234"})
    db["cc"].insert({"c_id": "c:01", "b_id": "b:5678"})
    db["cc"].insert({"c_id": "c:02", "b_id": "b:5678"})

    # Collections for the path weight test
    db.create_collection("aaa")
    db.create_collection("bbb")
    db.create_collection("ccc")
    db.create_collection("ddd")
    db.create_collection("eee")

    db["bbb"].insert({"b_id": "b:1234", "a_id": "a:1234", "e_id": "e:5678"})
    db["ccc"].insert({"c_id": "c:1234", "b_id": "b:1234"})
    db["ddd"].insert({"d_id": "d:1234", "c_id": "c:1234"})
    db["eee"].insert({"e_id": "e:1234", "d_id": "d:1234"})

    # Collections for the mix mongodb and api test
    db.create_collection("mix1")
    db.create_collection("mix3")

    db["mix1"].insert({"ensembl": "ENSG00000123374", "start_id": "start1"})
    db["mix3"].insert({"end_id": "end1", "entrez": "1017"})

    # Collections for lookup failure
    db["b"].insert({"b_id": "b:f1", "a_id": "a:f1"})
    db["c"].insert({"c_id": "c:f1", "b_id": "b:f1"})
    db["d"].insert({"d_id": "d:fail1", "c_id": "c:f1"})
    db["e"].insert({"e_id": "e:f1", "d_id": "d:f1"})
    yield db
    db.drop_collection("a")
    db.drop_collection("b")
    db.drop_collection("c")
    db.drop_collection("d")
    db.drop_collection("e")

    # Collections for the second test (one2many)
    db.drop_collection("aa")
    db.drop_collection("bb")
    db.drop_collection("cc")

    # Collections for the weighted test
    db.drop_collection("aaa")
    db.drop_collection("bbb")
    db.drop_collection("ccc")
    db.drop_collection("ddd")
    db.drop_collection("eee")

    # Collections for the mix mongodb and api test
    db.drop_collection("mix1")
    db.drop_collection("mix3")


@pytest.fixture(scope="module")
def simple_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # Simple Graph for Testing
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform_mdb import MongoDBEdge
    graph_simple = nx.DiGraph()

    graph_simple.add_node("a")
    graph_simple.add_node("b")
    graph_simple.add_node("c")
    graph_simple.add_node("d")
    graph_simple.add_node("e")

    graph_simple.add_edge("a", "b", object=MongoDBEdge("b", "a_id", "b_id", label="a_to_b"))
    graph_simple.add_edge("b", "c", object=MongoDBEdge("c", "b_id", "c_id"))
    # Test Loop
    graph_simple.add_edge("b", "a", object=MongoDBEdge("b", "b_id", "a_id"))
    graph_simple.add_edge("c", "d", object=MongoDBEdge("d", "c_id", "d_id"))
    graph_simple.add_edge("d", "e", object=MongoDBEdge("e", "d_id", "e_id"))
    graph_simple.add_edge("c", "e", object=MongoDBEdge("c", "c_id", "e_id"))
    yield graph_simple


@pytest.fixture(scope="module")
def weighted_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # Weighted graph for testing
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform_mdb import MongoDBEdge
    graph_weights = nx.DiGraph()

    graph_weights.add_node("aaa")
    graph_weights.add_node("bbb")
    graph_weights.add_node("ccc")
    graph_weights.add_node("ddd")
    graph_weights.add_node("eee")

    graph_weights.add_edge("aaa", "bbb", object=MongoDBEdge("bbb", "a_id", "b_id", weight=1))
    # Shortcut with high weight
    graph_weights.add_edge("aaa", "eee", object=MongoDBEdge("bbb", "a_id", "e_id", weight=100))
    graph_weights.add_edge("bbb", "ccc", object=MongoDBEdge("ccc", "b_id", "c_id", weight=1))
    graph_weights.add_edge("ccc", "ddd", object=MongoDBEdge("ddd", "c_id", "d_id", weight=1))
    graph_weights.add_edge("ddd", "eee", object=MongoDBEdge("eee", "d_id", "e_id", weight=1))
    yield graph_weights


@pytest.fixture(scope="module")
def one_to_many_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # Graph with One to Many relationships
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform_mdb import MongoDBEdge
    graph_one2many = nx.DiGraph()

    graph_one2many.add_node("aa")
    graph_one2many.add_node("bb")
    graph_one2many.add_node("cc")

    graph_one2many.add_edge("aa", "bb", object=MongoDBEdge("bb", "a_id", "b_id"))
    graph_one2many.add_edge("bb", "cc", object=MongoDBEdge("cc", "b_id", "c_id"))
    graph_one2many.add_edge("cc", "dd", object=MongoDBEdge("dd", "c_id", "d_id"))
    yield graph_one2many


@pytest.fixture(scope="module")
def invalid_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # Invalid-Graph
    ##########################################################################
    """
    graph_invalid = nx.DiGraph()

    graph_invalid.add_node("aa")
    graph_invalid.add_node("bb")

    graph_invalid.add_edge("aa", "bb", object="invalid-string")
    yield graph_invalid


@pytest.fixture(scope="module")
def mixed_backend_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # Mix MongoDB and API Test
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform_mdb import MongoDBEdge
    from biothings.hub.datatransform.datatransform_api import MyGeneInfoEdge
    graph_mix = nx.DiGraph()

    graph_mix.add_node("mix1")
    graph_mix.add_node("ensembl")
    graph_mix.add_node("entrez")
    graph_mix.add_node("mix3")

    graph_mix.add_edge(
        "mix1",
        "ensembl",
        object=MongoDBEdge("mix1", "start_id", "ensembl"),
    )
    graph_mix.add_edge(
        "ensembl",
        "entrez",
        object=MyGeneInfoEdge("ensembl.gene", "entrezgene"),
    )
    graph_mix.add_edge(
        "entrez",
        "mix3",
        object=MongoDBEdge("mix3", "entrez", "end_id"),
    )
    yield graph_mix


@pytest.fixture(scope="module")
def mychem_api_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # MyChem.Info API Graph for Test
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform_api import MyChemInfoEdge
    graph_mychem = nx.DiGraph()

    graph_mychem.add_node("chebi")
    graph_mychem.add_node("drugbank")
    graph_mychem.add_node("pubchem")
    graph_mychem.add_node("inchikey")

    graph_mychem.add_edge(
        "chebi",
        "inchikey",
        object=MyChemInfoEdge("chebi.id", "_id"),
    )
    graph_mychem.add_edge(
        "drugbank",
        "inchikey",
        object=MyChemInfoEdge("drugbank.drugbank_id", "_id"),
    )
    graph_mychem.add_edge(
        "pubchem",
        "inchikey",
        object=MyChemInfoEdge("pubchem.cid", "_id"),
    )
    yield graph_mychem


@pytest.fixture(scope="module")
def regex_edge_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # Regular Expression Edges in Graph
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform import RegExEdge
    from biothings.hub.datatransform.datatransform_mdb import MongoDBEdge
    graph_regex = nx.DiGraph()

    graph_regex.add_node("a")
    graph_regex.add_node("b")
    graph_regex.add_node("bregex")

    graph_regex.add_edge("a", "b", object=MongoDBEdge("b", "a_id", "b_id"))
    graph_regex.add_edge("b", "bregex", object=RegExEdge("b:", "bregex:"))
    yield graph_regex


@pytest.fixture(scope="module")
def pubchem_api_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # PubChem API Graph for Testing
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform_api import MyChemInfoEdge
    graph_pubchem = nx.DiGraph()

    graph_pubchem.add_node("inchi")
    graph_pubchem.add_node("pubchem")
    graph_pubchem.add_node("inchikey")

    inchikey_fields = [
        "pubchem.inchi_key",
        "drugbank.inchi_key",
        "chembl.inchi_key",
    ]

    graph_pubchem.add_edge(
        "inchi",
        "pubchem",
        object=MyChemInfoEdge("pubchem.inchi", "pubchem.cid"),
    )
    graph_pubchem.add_edge(
        "pubchem",
        "inchikey",
        object=MyChemInfoEdge("pubchem.cid", inchikey_fields),
    )
    yield graph_pubchem


@pytest.fixture(scope="module")
def case_insensitive_graph() -> nx.DiGraph:
    """
    ##########################################################################
    # Case Insensitive Graph for Testing
    ##########################################################################
    """
    from biothings.hub.datatransform.datatransform_mdb import CIMongoDBEdge
    graph_ci = nx.DiGraph()

    graph_ci.add_node("a")
    graph_ci.add_node("b")

    graph_ci.add_edge("a", "b", object=CIMongoDBEdge("b", "a_id", "b_id", label="a_to_b"))
    yield graph_ci
