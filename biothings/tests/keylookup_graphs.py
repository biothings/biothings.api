import biothings_client
import networkx as nx

from biothings.hub.datatransform.datatransform_api import MyChemInfoEdge, MyGeneInfoEdge
from biothings.hub.datatransform.datatransform_networkx import MongoDBEdge
from biothings.hub.datatransform.datatransform import RegExEdge

###############################################################################
# Simple Graph for Testing
###############################################################################
graph_simple = nx.DiGraph()

graph_simple.add_node('a')
graph_simple.add_node('b')
graph_simple.add_node('c')
graph_simple.add_node('d')
graph_simple.add_node('e')

graph_simple.add_edge('a', 'b',
                      object=MongoDBEdge('b', 'a_id', 'b_id'))
graph_simple.add_edge('b', 'c',
                      object=MongoDBEdge('c', 'b_id', 'c_id'))
# Test Loop
graph_simple.add_edge('b', 'a',
                      object=MongoDBEdge('b', 'b_id', 'a_id'))
graph_simple.add_edge('c', 'd',
                      object=MongoDBEdge('d', 'c_id', 'd_id'))
graph_simple.add_edge('d', 'e',
                      object=MongoDBEdge('e', 'd_id', 'e_id'))
graph_simple.add_edge('c', 'e',
                      object=MongoDBEdge('c', 'c_id', 'e_id'))

###############################################################################
# Weighted graph for testing
###############################################################################
graph_weights = nx.DiGraph()

graph_weights.add_node('aaa')
graph_weights.add_node('bbb')
graph_weights.add_node('ccc')
graph_weights.add_node('ddd')
graph_weights.add_node('eee')

graph_weights.add_edge('aaa', 'bbb',
                       object=MongoDBEdge('bbb', 'a_id', 'b_id', weight=1))
# Shortcut with high weight
graph_weights.add_edge('aaa', 'eee',
                       object=MongoDBEdge('bbb', 'a_id', 'e_id', weight=100))
graph_weights.add_edge('bbb', 'ccc',
                       object=MongoDBEdge('ccc', 'b_id', 'c_id', weight=1))
graph_weights.add_edge('ccc', 'ddd',
                       object=MongoDBEdge('ddd', 'c_id', 'd_id', weight=1))
graph_weights.add_edge('ddd', 'eee',
                       object=MongoDBEdge('eee', 'd_id', 'e_id', weight=1))


###############################################################################
# Graph with One to Many relationships
###############################################################################
graph_one2many = nx.DiGraph()

graph_one2many.add_node('aa')
graph_one2many.add_node('bb')
graph_one2many.add_node('cc')

graph_one2many.add_edge('aa', 'bb',
                        object=MongoDBEdge('bb', 'a_id', 'b_id'))
graph_one2many.add_edge('bb', 'cc',
                        object=MongoDBEdge('cc', 'b_id', 'c_id'))
graph_one2many.add_edge('cc', 'dd',
                        object=MongoDBEdge('dd', 'c_id', 'd_id'))

###############################################################################
# Invalid-Graph
###############################################################################
graph_invalid = nx.DiGraph()

graph_invalid.add_node('aa')
graph_invalid.add_node('bb')

graph_invalid.add_edge('aa', 'bb', object='invalid-string')

###############################################################################
# Mix MongoDB and API Test
###############################################################################
graph_mix = nx.DiGraph()

graph_mix.add_node('mix1')
graph_mix.add_node('ensembl')
graph_mix.add_node('entrez')
graph_mix.add_node('mix3')

graph_mix.add_edge('mix1', 'ensembl',
                   object=MongoDBEdge('mix1', 'start_id', 'ensembl'))
graph_mix.add_edge('ensembl', 'entrez',
                   object=MyGeneInfoEdge('ensembl.gene', 'entrezgene'))
graph_mix.add_edge('entrez', 'mix3',
                   object=MongoDBEdge('mix3', 'entrez', 'end_id'))

###############################################################################
# MyChem.Info API Graph for Test
###############################################################################
graph_mychem = nx.DiGraph()

graph_mychem.add_node('chebi')
graph_mychem.add_node('drugbank')
graph_mychem.add_node('pubchem')
graph_mychem.add_node('inchikey')

graph_mychem.add_edge('chebi', 'inchikey',
                      object=MyChemInfoEdge('chebi.id', '_id'))
graph_mychem.add_edge('drugbank', 'inchikey',
                      object=MyChemInfoEdge('drugbank.drugbank_id', '_id'))
graph_mychem.add_edge('pubchem', 'inchikey',
                      object=MyChemInfoEdge('pubchem.cid', '_id'))

###############################################################################
# Simple Graph for Testing
###############################################################################
graph_regex = nx.DiGraph()

graph_regex.add_node('a')
graph_regex.add_node('b')
graph_regex.add_node('bregex')

graph_regex.add_edge('a', 'b', object=MongoDBEdge('b', 'a_id', 'b_id'))
graph_regex.add_edge('b', 'bregex', object=RegExEdge('b:', 'bregex:'))
