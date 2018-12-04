import copy
import re

from networkx import all_simple_paths, all_shortest_paths, nx
from biothings.utils.common import iter_n
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
    def __init__(self, collection_name, lookup, field, weight=1):
        """
        :param collection_name: The name of the MongoDB collection.
        :type collection_name: str
        :param lookup: The field that will match the input identifier in the collection.
        :type lookup: str
        :param field: The output identifier field that will be read out of matching documents.
        :type field: str
        :param weight: Weights are used to prefer one path over another. The path with the lowest weight is preferred. The default weight is 1.
        :type weight: int
        """

        super().__init__()
        # unpickleable attributes, grouped
        self.init_state()
        self.collection_name = collection_name
        self.lookup = lookup
        self.field = field
        self.weight = weight

    def init_state(self):
        self._state = {
            "collection": None,
            "logger": None
        }

    @property
    def collection(self):
        if not self._state["collection"]:
            try:
                self.prepare_collection()
            except Exception as e:
                # if accessed but not ready, then just ignore and return invalid value for a client
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
        if len(id_lst):
            find_lst = self.collection.find({self.lookup: {"$in": id_lst}}, {self.lookup: 1, self.field: 1})

            for d in find_lst:
                for orig_id in id_strct.find_right(nested_lookup(d, self.lookup)):
                    res_id_strct.add(orig_id, nested_lookup(d, self.field))
                    if debug:
                        res_id_strct.set_debug(orig_id, nested_lookup(d, self.field))
        return res_id_strct


class DataTransformMDB(DataTransform):
    """
    Convert document identifiers from one type to another.
    """
    # Constants
    batch_size = 1000
    default_source = '_id'

    def __init__(self, G, *args, **kwargs):
        """
        The DataTransform MDB module was written as a decorator class
        which should be applied to the load_data function of a
        Biothings Uploader.  The load_data function yields documents,
        which are then post processed by call and the 'id' key
        conversion is performed.

        :param G: nx.DiGraph (networkx 2.1) configuration graph
        :param input_types: A list of input types for the form (identifier, field) where identifier matches a node and field is an optional dotstring field for where the identifier should be read from (the default is ‘_id’).
        :param output_types: A priority list of identifiers to convert to. These identifiers should match nodes in the graph.
        :type output_types: list(str)
        :param skip_on_failure: If True, documents where identifier conversion fails will be skipped in the final document list.
        :type skip_on_failure: bool
        :param skip_w_regex: Do not perform conversion if the identifier matches the regular expression provided to this argument. By default, this option is disabled.
        :type skip_w_regex: bool
        :param idstruct_class: Override an internal data structure used by the this module (advanced usage)
        :type idstruct_class: class
        :param copy_from_doc: If true then an identifier is copied from the input source document regardless as to weather it matches an edge or not. (advanced usage)
        :type copy_from_doc: bool
        :param debug: Enable debugging information.  When enabled, debugging information
               will be retained in the 'dt_debug' field of each document.
        :type debug: bool
        """
        if not isinstance(G, nx.DiGraph):
            raise ValueError("key_lookup configuration error:  G must be of type nx.DiGraph")
        self._validate_graph(G)
        self.G = G
        self.logger,_ = get_logger('datatransform')

        super().__init__(*args,**kwargs)
        self._precompute_paths()

    def _valid_input_type(self, input_type):
        return input_type.lower() in self.G.nodes()

    def _valid_output_type(self, output_type):
        return output_type.lower() in self.G.nodes()

    def _validate_graph(self, G):
        """
        Check if the input configuration graph G has a valid structure.
        :param G: key_lookup configuration graph
        :return:
        """
        # all node names should be lowercase
        for n in G.nodes():
            if n != n.lower():
                raise ValueError("node object {} is not lowercase".format(n))
        for (v1, v2) in G.edges():
            if 'object' not in G.edges[v1, v2].keys():
                raise ValueError("edge_object for ({}, {}) is missing".format(v1, v2))
            edge_object = G.edges[v1, v2]['object']
            if not isinstance(edge_object, DataTransformEdge):
                raise ValueError("edge_object for ({}, {}) is of the wrong type".format(v1, v2))

    def _precompute_paths(self):
        """
        Precompute all paths from the given key_type to all target key types
        provided on initialization.
        :return:
        """
        self.paths = {}
        for output_type in self.output_types:
            for input_type in self.input_types:
                paths = [p for p in all_simple_paths(self.G, input_type[0], output_type)]
                if not paths:
                    try:
                        # this will try to find self-loops. all_shortest_paths() return one element,
                        # the self-lopped node, but we need an tuple so the "*2"
                        # also make sure those self-loops actually are defined in the graph
                        try:
                            self.G.edges[input_type[0],output_type] # this will raise a keyerror is edge for self-loop p-to-p isn't defined
                            paths = [p*2 for p in all_shortest_paths(self.G, input_type[0], output_type)]
                        except KeyError:
                            pass
                    except nx.NetworkXNoPath:
                        pass
                # Sort by path length - try the shortest paths first
                paths = sorted(paths, key=self._compute_path_weight)
                self.paths[(input_type[0], output_type)] = paths
        self.logger.debug("All Pre-Computed DataTransform Paths:  {}".format(self.paths))

    def key_lookup_batch(self, batchiter):
        """
        Look up all keys for ids given in the batch iterator (1 block)
        :param batchiter:  1 lock of records to look up keys for
        :return:
        """
        doc_lst = []
        for doc in batchiter:
            doc_lst.append(doc)

        miss_lst = []
        for doc in doc_lst:
            if self.skip_w_regex and self.skip_w_regex.match(doc['_id']):
                yield doc
            else:
                miss_lst.append(doc)

        for output_type in self.output_types:
            for input_type in self.input_types:
                if output_type == input_type[0]:
                    # the doc itself has the correct ID, 
                    # so either there's a self-loop avail to check this ID is valid
                    if self.G.has_edge(output_type,output_type):
                        (hit_lst, miss_lst) = self.travel(input_type, output_type, miss_lst)
                    # or if copy is allowed, we get the value from the doc
                    elif self.copy_from_doc:
                        (hit_lst, miss_lst) = self._copy(input_type, miss_lst)
                else:    
                    (hit_lst, miss_lst) = self.travel(input_type, output_type, miss_lst)

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
            else:
                miss_lst.append(doc)
        return (hit_lst, miss_lst)

    def _compute_path_weight(self, path):
        """
        Helper function to compute the weight of a path
        :param path:
        :return: computed weight
        """
        weight = 0
        for p in map(nx.utils.pairwise, [path]):
            for (v1, v2) in p:
                edge = self.G.edges[v1, v2]['object']
                weight = weight + edge.weight
        return weight

    def travel(self, input_type, target, doc_lst):
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
            for d in doc_lst:
                hit_flag = False
                value = nested_lookup(d, input_type[1])
                for lookup_id in id_strct.find_left(value):
                    new_doc = copy.deepcopy(d)
                    # ensure _id is always a str
                    new_doc['_id'] = str(lookup_id)
                    # capture debug information
                    if debug:
                        new_doc['dt_debug'] = id_strct.get_debug(value)
                    hit_lst.append(new_doc)
                    hit_flag = True
                if not hit_flag:
                    miss_lst.append(d)
            return hit_lst, miss_lst

        #self.logger.debug("Travel From '{}' To '{}'".format(input_type[0], target))

        # Keep a running list of all saved hits
        saved_hits = IDStruct()

        # Build the path structure, which will save results
        path_strct = _build_path_strct(input_type, doc_lst)

        for path in map(nx.utils.misc.pairwise, self.paths[(input_type[0], target)]):
            for (v1, v2) in path:
                edge = self.G.edges[v1, v2]['object']
                num_input_ids = len(path_strct)
                path_strct = self._edge_lookup(edge, path_strct)
                num_output_ids = len(path_strct)
                if num_input_ids:
                    # self.logger.debug("Edge {} - {}, {} searched returned {}".format(v1, v2, num_input_ids, num_output_ids))
                    self.histogram.update_edge(v1, v2, num_output_ids)

            if len(path_strct):
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
        self.histogram.update_io(input_type,target,len(hit_lst))
        return hit_lst, miss_lst

    def _edge_lookup(self, edge_obj, id_strct):
        """
        Follow an edge given a key.

        This method uses the data in the edge_object
        to find one key to another key using one of
        several types of lookup functions.
        """
        return edge_obj.edge_lookup(self, id_strct, self.debug)
