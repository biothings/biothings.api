"""
DataTransform MDB module - class for performing key lookup
using conversions described in a networkx graph.
"""
# pylint: disable=E0401, E0611
import copy

from networkx import all_simple_paths, all_shortest_paths, nx
from pymongo.collation import Collation
from biothings.hub.datatransform import DataTransform
from biothings.hub.datatransform import DataTransformEdge
from biothings.hub.datatransform import IDStruct
from biothings.hub.datatransform import nested_lookup
import biothings.utils.mongo as mongo
from biothings.utils.loggers import get_logger


class MongoDBEdge(DataTransformEdge):
    """
    The MongoDBEdge uses data within a MongoDB collection to convert
    one identifier to another. The input identifier is used to search
    a collection. The output identifier values are read out of that
    collection:
    """
    def __init__(self, collection_name, lookup, field, weight=1, label=None, check_index=True):
        # pylint: disable=R0913
        """
        :param collection_name: The name of the MongoDB collection.
        :type collection_name: str
        :param lookup: The field that will match the input identifier in the collection.
        :type lookup: str
        :param field: The output identifier field that will be read out of matching documents.
        :type field: str
        :param weight: Weights are used to prefer one path over another.
                       The path with the lowest weight is preferred.
                       The default weight is 1.
        :type weight: int
        """

        super(MongoDBEdge, self).__init__(label)
        # unpickleable attributes, grouped
        self.init_state()
        self.collection_name = collection_name
        self.lookup = lookup
        self.field = field
        self.weight = weight
        if check_index:
            if self.collection_name in self.collection.database.collection_names():
                avail_idxs = {}
                for idx in self.collection.list_indexes():
                    keys = idx["key"]
                    # this could be a composite index, multiple keys being part of the index
                    # we'll consider them as individually accessible, but I'm not sure how
                    # MongoDB deals with that => TODO check
                    for k in keys:
                        avail_idxs[k] = True
                if not self.lookup in avail_idxs:
                    raise ValueError("Field '%s' isn't indexed, this would " % self.lookup + \
                            "result in very long datatransform process")
            else:
                self.logger.warning("Collection '%s' doesn't exist, can't check indices" % self.collection_name)

    def init_state(self):
        self._state = {
            "collection": None,
            "logger": None
        }

    @property
    def collection(self):
        """getting for collection member variable"""
        if not self._state["collection"]:
            try:
                self.prepare_collection()
            # pylint: disable=W0703
            except Exception:
                # if accessed but not ready, then just ignore and return invalid
                # value for a client
                return None
        return self._state["collection"]

    def prepare_collection(self):
        """
        Load the mongodb collection specified by collection_name.
        :return:
        """
        self._state["collection"] = mongo.get_src_db()[self.collection_name]
        self.logger.info("Registering collection:  {}".format(self.collection_name))

    def edge_lookup(self, keylookup_obj, id_strct, debug=False):
        """
        Follow an edge given a key.

        An edge represets a document and this method uses the data in the edge_object
        to find one key to another key using exactly one mongodb lookup.
        :param keylookup_obj:
        :param id_strct:
        :return:
        """
        if not isinstance(id_strct, IDStruct):
            raise TypeError("edge_lookup id_struct is of the wrong type")

        # Build up a new_id_strct from the results
        res_id_strct = IDStruct()

        # Keep the old debug information
        if debug:
            res_id_strct.import_debug(id_strct)

        id_lst = id_strct.id_lst
        if id_lst:
            find_lst = self.collection_find(id_lst, self.lookup, self.field)

            for doc in find_lst:
                for orig_id in id_strct.find_right(nested_lookup(doc, self.lookup)):
                    res_id_strct.add(orig_id, nested_lookup(doc, self.field))
                    if debug:
                        res_id_strct.set_debug(orig_id, self.label, nested_lookup(doc, self.field))
        return res_id_strct

    def collection_find(self, id_lst, lookup, field):
        """
        Abstract out (as one line) the call to collection.find
        """
        return self.collection.find({lookup: {"$in": id_lst}}, {lookup: 1, field: 1})


class CIMongoDBEdge(MongoDBEdge):
    """
    Case-insensitive MongoDBEdge
    """
    def __init__(self, collection_name, lookup, field, weight=1, label=None):
        # pylint: disable=R0913, W0235
        super(CIMongoDBEdge, self).__init__(collection_name, lookup, field, weight, label)

    def collection_find(self, id_lst, lookup, field):
        """
        Abstract out (as one line) the call to collection.find
        and use a case-insensitive collation
        """
        return self.collection.find({lookup: {"$in": id_lst}}, {lookup: 1, field: 1})\
            .collation(Collation(locale='en', strength=2))


class DataTransformMDB(DataTransform):
    """
    Convert document identifiers from one type to another.
    """
    # Constants
    batch_size = 1000
    default_source = '_id'

    def __init__(self, graph, *args, **kwargs):
        """
        The DataTransformNetworkX module was written as a decorator class
        which should be applied to the load_data function of a
        Biothings Uploader.  The load_data function yields documents,
        which are then post processed by call and the 'id' key
        conversion is performed.

        :param graph: nx.DiGraph (networkx 2.1) configuration graph
        :param input_types: A list of input types for the form (identifier, field) where
                            identifier matches a node and field is an optional dotstring
                            field for where the identifier should be read from
                            (the default is '_id').
        :param output_types: A priority list of identifiers to convert to. These
                             identifiers should match nodes in the graph.
        :type output_types: list(str)
        :param id_priority_list: A priority list of identifiers to to sort input
                                 and output types by.
        :type id_priority_list: list(str)
        :param skip_on_failure: If True, documents where identifier conversion fails
                                will be skipped in the final document list.
        :type skip_on_failure: bool
        :param skip_w_regex: Do not perform conversion if the identifier matches
                             the regular expression provided to this argument. By default,
                             this option is disabled.
        :type skip_w_regex: bool
        :param skip_on_success: If True, documents where identifier conversion succeeds
                                will be skipped in the final document list.
        :type skip_on_success: bool
        :param idstruct_class: Override an internal data structure used by the this
                               module (advanced usage)
        :type idstruct_class: class
        :param copy_from_doc: If true then an identifier is copied from the input
                              source document regardless as to weather it matches an
                              edge or not. (advanced usage)
        :type copy_from_doc: bool
        """
        if not isinstance(graph, nx.DiGraph):
            raise ValueError("key_lookup configuration error:  graph must be of type nx.DiGraph")
        self._validate_graph(graph)
        self.graph = graph
        self.logger, _ = get_logger('datatransform')

        super(DataTransformMDB, self).__init__(*args, **kwargs)
        self._precompute_paths()

    def _valid_input_type(self, input_type):
        return input_type.lower() in self.graph.nodes()

    def _valid_output_type(self, output_type):
        return output_type.lower() in self.graph.nodes()

    @staticmethod
    def _validate_graph(graph):
        """
        Check if the input configuration graph graph has a valid structure.
        :param graph: key_lookup configuration graph
        :return:
        """
        # all node names should be lowercase
        for node in graph.nodes():
            if node != node.lower():
                raise ValueError("node object {} is not lowercase".format(node))
        for (vert1, vert2) in graph.edges():
            if 'object' not in graph.edges[vert1, vert2].keys():
                raise ValueError("edge_object for ({}, {}) is missing".format(vert1, vert2))
            edge_object = graph.edges[vert1, vert2]['object']
            if not isinstance(edge_object, DataTransformEdge):
                raise ValueError("edge_object for ({}, {}) is of the wrong type".\
                    format(vert1, vert2))

    def _precompute_paths(self):
        """
        Precompute all paths from the given key_type to all target key types
        provided on initialization.
        :return:
        """
        self.paths = {}
        for output_type in self.output_types:
            for input_type in self.input_types:
                paths = [p for p in all_simple_paths(self.graph, input_type[0], output_type)]
                if not paths:
                    try:
                        # this will try to find self-loops. all_shortest_paths() return one element,
                        # the self-lopped node, but we need an tuple so the "*2"
                        # also make sure those self-loops actually are defined in the graph
                        try:
                            # this will raise a keyerror is edge for self-loop
                            # p-to-p isn't defined
                            # pylint: disable=W0104
                            self.graph.edges[input_type[0], output_type]
                            paths = [p*2 for p in all_shortest_paths(
                                self.graph, input_type[0], output_type)]
                        except KeyError:
                            pass
                    except nx.NetworkXNoPath:
                        pass
                # Sort by path length - try the shortest paths first
                paths = sorted(paths, key=self._compute_path_weight)
                self.paths[(input_type[0], output_type)] = paths
        # self.logger.debug("All Pre-Computed DataTransform Paths:  {}".format(self.paths))

    def key_lookup_batch(self, batchiter):
        # pylint: disable=R0912
        """
        Look up all keys for ids given in the batch iterator (1 block)
        :param batchiter:  1 lock of records to look up keys for
        :return:
        """
        doc_lst = []
        for doc in batchiter:
            # in debug mode, skip all documents not in the debug list
            if self.debug:
                # pylint: disable=C0121
                if self.debug == True or doc['_id'] in self.debug:
                    # set debug information
                    doc['dt_debug'] = {'orig_id': doc['_id']}
                    doc_lst.append(doc)
            else:
                doc_lst.append(doc)

        hit_lst = []
        miss_lst = []
        for doc in doc_lst:
            if self.skip_w_regex and self.skip_w_regex.match(doc['_id']):
                yield doc
            else:
                miss_lst.append(doc)

        # Attempt to reach each destination in order...
        for output_type in self.output_types:
            # Starting with each input_type
            for input_type in self.input_types:
                # self.logger.debug("Attempt Lookup:  from '{}' To '{}'"\
                # .format(input_type[0], output_type))
                if output_type == input_type[0]:
                    # the doc itself has the correct ID,
                    # so either there's a self-loop avail to check this ID is valid
                    if self.graph.has_edge(output_type, output_type):
                        (hit_lst, miss_lst) = self.travel(input_type, output_type, miss_lst)
                    # or if copy is allowed, we get the value from the doc
                    elif self.copy_from_doc:
                        (hit_lst, miss_lst) = self._copy(input_type, miss_lst)
                else:
                    (hit_lst, miss_lst) = self.travel(input_type, output_type, miss_lst)

                if not self.skip_on_success:
                    for doc in hit_lst:
                        yield doc

        # Keep the misses if we do not skip on failure
        if not self.skip_on_failure:
            for doc in miss_lst:
                yield doc

    def _copy(self, input_type, doc_lst):
        """Copy ids in the case where input_type == output_type"""
        hit_lst = []
        miss_lst = []
        for doc in doc_lst:
            val = nested_lookup(doc, input_type[1])
            if val:
                # ensure _id is always a str
                doc['_id'] = str(val)
                hit_lst.append(doc)
                # retain debug information if available (assumed dt_debug already in place)
                if self.debug:
                    doc['dt_debug']['copy_from'] = (input_type[1], val)
            else:
                miss_lst.append(doc)
        # Keep a record of IDs copied
        self.histogram.update_io(input_type, input_type, len(hit_lst))
        return (hit_lst, miss_lst)

    def _compute_path_weight(self, path):
        """
        Helper function to compute the weight of a path
        :param path:
        :return: computed weight
        """
        weight = 0
        for path_var in map(nx.utils.pairwise, [path]):
            for (vert1, vert2) in path_var:
                edge = self.graph.edges[vert1, vert2]['object']
                weight = weight + edge.weight
        return weight

    def travel(self, input_type, target, doc_lst):
        # pylint: disable=R0914
        """
        Traverse a graph from a start key type to a target key type using
        precomputed paths.

        :param start: key type to start from
        :param target: key type to end at
        :param key: key value of type 'start'
        :return:
        """

        def _build_path_strct(input_type, doc_lst):
            """
            Build the path structure for the travel function
            :return:
            """
            return self.idstruct_class(input_type[1], doc_lst)

        def _build_hit_miss_lsts(doc_lst, id_strct, debug):
            """
            Return a list of documents that have had their identifiers replaced
            also return a list of documents that were not changed
            :param doc_lst:
            :param id_strct:
            :return:
            """
            hit_lst = []
            miss_lst = []
            for doc in doc_lst:
                hit_flag = False
                value = nested_lookup(doc, input_type[1])
                for lookup_id in id_strct.find_left(value):
                    new_doc = copy.deepcopy(doc)
                    # ensure _id is always a str
                    new_doc['_id'] = str(lookup_id)
                    # capture debug information
                    if debug:
                        new_doc['dt_debug']['start_field'] = input_type[1]
                        new_doc['dt_debug']['debug'] = id_strct.get_debug(value)
                    hit_lst.append(new_doc)
                    hit_flag = True
                if not hit_flag:
                    miss_lst.append(doc)
            return hit_lst, miss_lst

        #self.logger.debug("Travel From '{}' To '{}'".format(input_type[0], target))

        # Keep a running list of all saved hits
        saved_hits = IDStruct()

        # Build the path structure, which will save results
        path_strct = _build_path_strct(input_type, doc_lst)

        for path in map(nx.utils.misc.pairwise, self.paths[(input_type[0], target)]):
            for (vert1, vert2) in path:
                edge = self.graph.edges[vert1, vert2]['object']
                num_input_ids = len(path_strct)
                path_strct = self._edge_lookup(edge, path_strct)
                num_output_ids = len(path_strct)
                if num_input_ids:
                    # self.logger.debug("Edge {} - {}, {} searched returned {}"\
                    #        .format(vert1, vert2, num_input_ids, num_output_ids))
                    self.histogram.update_edge(vert1, vert2, num_output_ids)

            if path_strct:
                saved_hits += path_strct

            # reset the state to lookup misses
            path_strct = self.idstruct_class()
            for doc in doc_lst:
                val = nested_lookup(doc, input_type[1])
                if val:
                    if not saved_hits.left(val):
                        path_strct.add(val, val)

        # Return a list of documents that have had their identifiers replaced
        # also return a list of documents that were not changed
        hit_lst, miss_lst = _build_hit_miss_lsts(doc_lst, saved_hits, self.debug)
        self.histogram.update_io(input_type, target, len(hit_lst))
        return hit_lst, miss_lst

    def _edge_lookup(self, edge_obj, id_strct):
        """
        Follow an edge given a key.

        This method uses the data in the edge_object
        to find one key to another key using one of
        several types of lookup functions.
        """
        return edge_obj.edge_lookup(self, id_strct, self.debug)
