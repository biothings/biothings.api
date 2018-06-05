import networkx as nx

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
                      object={'col': 'b',
                   'lookup': 'a_id',
                   'field': 'b_id'})
graph_simple.add_edge('b', 'c',
                      object={'col': 'c',
                   'lookup': 'b_id',
                   'field': 'c_id'})
# Test Loop
graph_simple.add_edge('b', 'a',
                      object={'col': 'b',
                   'lookup': 'b_id',
                   'field': 'a_id',
                   'comment': 'b-->a'})
graph_simple.add_edge('c', 'd',
                      object={'col': 'd',
                   'lookup': 'c_id',
                   'field': 'd_id'})
graph_simple.add_edge('d', 'e',
                      object={'col': 'e',
                   'lookup': 'd_id',
                   'field': 'e_id'})
graph_simple.add_edge('c', 'e',
                      object={'col': 'c',
                   'lookup': 'c_id',
                   'field': 'e_id'})

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
                       weight=1,
                       object={'col': 'bbb',
                   'lookup': 'a_id',
                   'field': 'b_id'})

# Shortcut with high weight
graph_weights.add_edge('aaa', 'eee',
                       weight=100,
                       object={'col': 'bbb',
                   'lookup': 'a_id',
                   'field': 'e_id'})

graph_weights.add_edge('bbb', 'ccc',
                       weight=1,
                       object={'col': 'ccc',
                   'lookup': 'b_id',
                   'field': 'c_id'})

graph_weights.add_edge('ccc', 'ddd',
                       weight=1,
                       object={'col': 'ddd',
                   'lookup': 'c_id',
                   'field': 'd_id'})

graph_weights.add_edge('ddd', 'eee',
                       weight=1,
                       object={'col': 'eee',
                   'lookup': 'd_id',
                   'field': 'e_id'})


###############################################################################
# Graph with One to Many relationships
###############################################################################
graph_one2many = nx.DiGraph()

graph_one2many.add_node('aa')
graph_one2many.add_node('bb')
graph_one2many.add_node('cc')


graph_one2many.add_edge('aa', 'bb',
                        object={'col': 'bb',
                   'lookup': 'a_id',
                   'field': 'b_id'})
graph_one2many.add_edge('bb', 'cc',
                        object={'col': 'cc',
                   'lookup': 'b_id',
                   'field': 'c_id'})
graph_one2many.add_edge('cc', 'dd',
                        object={'col': 'dd',
                   'lookup': 'c_id',
                   'field': 'd_id'})

###############################################################################
# Invalid-Graph
###############################################################################
graph_invalid = nx.DiGraph()

graph_invalid.add_node('aa')
graph_invalid.add_node('bb')

graph_invalid.add_edge('aa', 'bb',
                        object={'col': 'bb',
                   'lookup': 'a_id',
                   'field-invalid': 'b_id'})

