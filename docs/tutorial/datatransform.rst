
DataTransform Module
####################

A key problem when merging data from multiple data sources is finding a common identifier.  To ameliorate this problem,
we have written a DataTransform module to convert identifiers from one type to another.  Frequently, this conversion
process has multiple steps, where an identifier is converted to one or more intermediates before having its final value.
To describe these steps, the user defines a graph where each node represents an identifier type and each edge represents
a conversion.  The module processes documents using the network to convert their identifiers to their final form.

A graph is a mathematical model describing how different things are connected.  Using our model, our module is connecting
different identifiers together.  Each connection is an identifier conversion or lookup process.  For example, a simple graph
could describe how `pubchem` identifiers could be converted to `drugbank` identifiers using `MyChem.info`.

Graph Definition
----------------

The following graph facilitates conversion from `inchi` to `inchikey` using `pubchem` as an intermediate:

.. code-block:: python

     from biothings.hub.datatransform import MongoDBEdge
     import networkx as nx

     graph_mychem = nx.DiGraph()

     ###############################################################################
     # DataTransform Nodes and Edges
     ###############################################################################
     graph_mychem.add_node('inchi')
     graph_mychem.add_node('pubchem')
     graph_mychem.add_node('inchikey')

     graph_mychem.add_edge('inchi', 'pubchem',
                           object=MongoDBEdge('pubchem', 'pubchem.inchi', 'pubchem.cid'))

     graph_mychem.add_edge('pubchem', 'inchikey',
                           object=MongoDBEdge('pubchem', 'pubchem.cid', 'pubchem.inchi_key'))

To setup a graph, one must define nodes and edges.  There should be a node for each type of identifier and an edge
which describes how to convert from one identifier to another.  Node names can be arbitrary; the user is allowed to
chose what an identifier should be called.  Edge classes, however, must be defined precisely for conversion to be successful.

Edge Classes
------------

The following edge classes are supported by the `DataTransform` module.  One of these edge classes must be selected when
defining an edge connecting two nodes in a graph.

MongoDBEdge
~~~~~~~~~~~

.. autoclass:: biothings.hub.datatransform.MongoDBEdge

The example above uses the `MongoDBEdge` class to convert from `inchi` to `inchikey`.

MyChemInfoEdge
~~~~~~~~~~~~~~

.. autoclass:: biothings.hub.datatransform.MyChemInfoEdge

This example graph uses the `MyChemInfoEdge` class to convert from `pubchem` to `inchikey`.  The `pubchem.cid` and
`pubchem.inchi_key` fields are returned by `MyChem.info` and are listed by `/metadata/fields <http://mychem.info/v1/metadata/fields>`_.

.. code-block:: python

     from biothings.hub.datatransform import MyChemInfoEdge
     import networkx as nx

     graph_mychem = nx.DiGraph()

     ###############################################################################
     # DataTransform Nodes and Edges
     ###############################################################################
     graph_mychem.add_node('pubchem')
     graph_mychem.add_node('inchikey')

     graph_mychem.add_edge('pubchem', 'inchikey',
                           object=MyChemInfoEdge('pubchem.cid', 'pubchem.inchi_key'))

MyGeneInfoEdge
~~~~~~~~~~~~~~

.. autoclass:: biothings.hub.datatransform.MyGeneInfoEdge

RegExEdge
~~~~~~~~~

.. autoclass:: biothings.hub.datatransform.RegExEdge

This example graph uses the `RegExEdge` class to convert from `pubchem` to a shorter form.  The `CID:` prefix is removed by the regular expression substitution:

.. code-block:: python

     from biothings.hub.datatransform import RegExEdge
     import networkx as nx

     graph = nx.DiGraph()

     ###############################################################################
     # DataTransform Nodes and Edges
     ###############################################################################
     graph.add_node('pubchem')
     graph.add_node('pubchem-short')

     graph.add_edge('pubchem', 'pubchem-short',
                    object=RegExEdge('CID:', ''))

Example Usage
-------------

A complex graph developed for use with `MyChem.info <http://mychem.info/>`_ is shown
`here <https://github.com/biothings/mychem.info/blob/master/src/hub/datatransform/keylookup.py>`_.
This file includes a definition of the `MyChemKeyLookup` class which is used to call the module
on the data source.  In general, the graph and class should be supplied to the user by the
BioThings.api maintainers.

To call the `DataTransform` module on the `Biothings Uploader`, the following definition is used:

.. code-block:: python

    keylookup = MyChemKeyLookup(
            [('inchi', 'pharmgkb.inchi'),
             ('pubchem', 'pharmgkb.xrefs.pubchem.cid'),
             ('drugbank', 'pharmgkb.xrefs.drugbank'),
             ('chebi', 'pharmgkb.xrefs.chebi')])

    def load_data(self,data_folder):
        input_file = os.path.join(data_folder,"drugs.tsv")
        return self.keylookup(load_data)(input_file)

The parameters passed to `MyChemKeyLookup` are a list of input types.  The first element in an input type is
the node name that must match the graph.  The second element is the field in `dotstring` notation which should
describe where the identifier should be read from in a document.

The following report was reported when using the `DataTransform` module with PharmGKB.  Reports have a section
for document conversion and a section describing conversion along each edge.  The document section shows which
inputs were used to produce which outputs.  The edge section is useful in debugging graphs, ensuring that different
conversion edges are working properly.

.. code-block:: python

     {
          'doc_report': {
               "('inchi', 'pharmgkb.inchi')-->inchikey": 1637,
               "('pubchem', 'pharmgkb.xrefs.pubchem.cid')-->inchikey": 46
               "('drugbank', 'pharmgkb.xrefs.drugbank')-->inchikey": 41,
               "('drugbank', 'pharmgkb.xrefs.drugbank')-->drugbank": 25,
          }
          'edge_report': {
               'inchi-->chembl': 1109,
               'inchi-->drugbank': 319,
               'inchi-->pubchem': 209,
               'chembl-->inchikey': 1109,
               'drugbank-->inchikey': 360,
               'pubchem-->inchikey': 255
               'drugbank-->drugbank': 25,
          },
     }

As an example, the number identifiers converted from `inchi` to `inchikey` is 1637.  However, these conversions are done
via intermediates.  One of these intermediates is `chembl` and the number of identifiers converted from `inchi` to `chembl`
is 319.  Some identifiers are converted directly from `pubchem` and `drugbank`.  The `inchi` field is used to lookup several
intermediates (`chembl`, `drugbank`, and `pubchem`).  Eventually, most of these intermediates are converted to `inchikey`.

Advanced Usage - DataTransform MDB
----------------------------------

The `DataTransformMDB` module was written as a decorator class which is intended to be applied to the `load_data` function of
a `Biothings Uploader`.  This class can be sub-classed to simplify applification within a Biothings service.

.. autoclass:: biothings.hub.datatransform.DataTransformMDB

An example of how to apply this class is shown below:

.. code-block:: python

   keylookup = DataTransformMDB(graph, input_types, output_types,
                                skip_on_failure=False, skip_w_regex=None,
                                idstruct_class=IDStruct, copy_from_doc=False)
   def load_data(self,data_folder):
        input_file = os.path.join(data_folder,"drugs.tsv")
        return self.keylookup(load_data)(input_file)

It is possible to extend the `DataTransformEdge` type and define custom edges.  This could be useful for example
if the user wanted to define a computation that transforms one identifier to another.  For example `inchikey` may
be computed directly by performing a hash on the `inchi` identifier.

Document Maintainers
--------------------

* Greg Taylor (@gregtaylor)

* Chunlei Wu (@chunleiwu)
