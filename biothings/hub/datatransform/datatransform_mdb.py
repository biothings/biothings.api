import biothings_client
import copy
import re

from networkx import all_simple_paths, nx
from biothings.utils.common import iter_n
from biothings.hub.datatransform.datatransform import DataTransform
from biothings.hub.datatransform.datatransform import DataTransformEdge
from biothings.hub.datatransform.datatransform import nested_lookup
import biothings.utils.mongo as mongo
from biothings.utils.loggers import get_logger
from biothings import config as btconfig
from biothings import config_for_app

# Configuration of collections from biothings config file
config_for_app(btconfig)

# Setup logger and logging level
kl_log = get_logger('datatransform', btconfig.LOG_FOLDER)


class MongoDBEdge(DataTransformEdge):
    """
    IDLookupEdge object for MongoDB queries
    """
    def __init__(self, collection, lookup, field, weight=1):
        super().__init__()
        # unpickleable attributes, grouped
        self.init_state()
        self.collection_name = collection
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

    def edge_lookup(self, keylookup_obj, id_strct):
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

        id_lst = id_strct.id_lst
        if len(id_lst):
            find_lst = self.collection.find({self.lookup: {"$in": id_lst}}, {self.lookup: 1, self.field: 1})

            for d in find_lst:
                for orig_id in id_strct.find_right(nested_lookup(d, self.lookup)):
                    res_id_strct.add(orig_id, nested_lookup(d, self.field))
            kl_log.debug("results for {} ids".format(res_id_strct))
        return res_id_strct


class BiothingsAPIEdge(DataTransformEdge):
    """
    APIEdge - IDLookupEdge object for API calls
    """
    def __init__(self, scope, field, weight=1):
        super().__init__()
        self.init_state()
        self.scope = scope
        self.field = field
        self.weight = weight

    def init_state(self):
        self._state = {
            "client": None,
            "logger": None
        }

    @property
    def client(self):
        if not self._state["client"]:
            try:
                self.prepare_client()
            except Exception as e:
                # if accessed but not ready, then just ignore and return invalid value for a client
                return None
        return self._state["client"]

    def prepare_client(self):
        """do initialization of biothings_client"""
        raise NotImplementedError("Define in subclass")

    def edge_lookup(self, keylookup_obj, id_strct):
        """
        Follow an edge given a key.

        This method uses the data in the edge_object
        to find one key to another key using an api.
        :param edge:
        :param key:
        :return:
        """
        qr = self._query_many(keylookup_obj, id_strct)
        new_id_strct = self._parse_querymany(keylookup_obj, qr, id_strct, self.field)
        return new_id_strct

    def _query_many(self, keylookup_obj, id_strct):
        """
        Call the biothings_client querymany function with a list of identifiers
        and output fields that will be returned.
        :param id_lst: list of identifiers to query
        :return:
        """
        if not isinstance(id_strct, IDStruct):
            raise TypeError("id_strct shouldb be of type IDStruct")
        id_lst = id_strct.id_lst
        return self.client.querymany(id_lst,
                                     scopes=self.scope,
                                     fields=self.field,
                                     as_generator=True,
                                     returnall=True,
                                     size=keylookup_obj.batch_size)

    def _parse_querymany(self, keylookup_obj, qr, id_strct, field):
        """
        Parse the querymany results from the biothings_client into a structure
        that will later be used for document key replacement.
        :param qr: querymany results
        :return:
        """
        self.logger.debug("QueryMany Structure:  {}".format(qr))
        qm_struct = IDStruct()
        for q in qr['out']:
            query = q['query']
            val = nested_lookup(q, field)
            if val:
                for (orig_id, curr_id) in id_strct:
                    if query == curr_id:
                        qm_struct.add(orig_id, val)
        return qm_struct


class MyChemInfoEdge(BiothingsAPIEdge):

    def __init__(self, scope, field, weight=1):
        super().__init__(scope, field, weight)

    def prepare_client(self):
        """
        Load the biothings_client for the class
        :return:
        """
        self._state["client"] = biothings_client.get_client('drug')
        self.logger.info("Registering biothings_client 'gene'")


class MyGeneInfoEdge(BiothingsAPIEdge):

    def __init__(self, scope, field, weight=1):
        super().__init__(scope, field, weight)

    def prepare_client(self):
        """
        Load the biothings_client for the class
        :return:
        """
        self._state["client"] = biothings_client.get_client('gene')
        self.logger.info("Registering biothings_client 'drug'")


class DataTransformMDB(DataTransform):
    # Constants
    batch_size = 1000
    default_source = '_id'

    def __init__(self, G, input_types, output_types, skip_on_failure=False, skip_w_regex=None):
        """
        Initialize the keylookup object and precompute paths from the
        start key to all target keys.

        The decorator is intended to be applied to the load_data function
        of an uploader.  The load_data function yields documents, which
        are then post processed by call and the 'id' key conversion is
        performed.

        :param G: nx.DiGraph (networkx 2.1) configuration graph
        :param collections: list of mongodb collection names
        :param input_type: key type to start key lookup from
        :param output_types: list of all output types to convert to
        """
        if not isinstance(G, nx.DiGraph):
            raise ValueError("key_lookup configuration error:  G must be of type nx.DiGraph")
        self._validate_graph(G)
        self.G = G

        super().__init__(input_types, output_types, skip_on_failure, skip_w_regex)

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
                kl_log.info("Compute Path From '{}' to '{}'".format(input_type[0], output_type))
                paths = \
                    all_simple_paths(self.G, input_type[0], output_type)
                # Sort by path length - try the shortest paths first
                paths = sorted(paths, key=self._compute_path_weight)
                self.paths[(input_type[0], output_type)] = paths
        kl_log.debug("All Travel Paths:  {}".format(self.paths))

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
                (hit_lst, miss_lst) = self.travel(input_type, output_type, miss_lst)
                # kl_log.debug("Output documents from travel:")
                for doc in hit_lst:
                    # kl_log.debug(doc) # too much information to be useful
                    yield doc

        # Keep the misses if we do not skip on failure
        if not self.skip_on_failure:
            for doc in miss_lst:
                yield doc

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
            return IDStruct(input_type[1], doc_lst)

        def _build_hit_miss_lsts(doc_lst, id_strct):
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
                    new_doc['_id'] = lookup_id
                    hit_lst.append(new_doc)
                    hit_flag = True
                if not hit_flag:
                    miss_lst.append(d)
            return hit_lst, miss_lst

        kl_log.debug("Travel From '{}' To '{}'".format(input_type[0], target))

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
                    kl_log.debug("Edge {} - {}, {} searched returned {}".format(v1, v2, num_input_ids, num_output_ids))

            if len(path_strct):
                saved_hits += path_strct

            # reset the state to lookup misses
            path_strct = IDStruct()
            for doc in doc_lst:
                val = nested_lookup(doc, input_type[1])
                if val:
                    if not saved_hits.left(val):
                        path_strct.add(val, val)

        # Return a list of documents that have had their identifiers replaced
        # also return a list of documents that were not changed
        hit_lst, miss_lst = _build_hit_miss_lsts(doc_lst, saved_hits)
        return hit_lst, miss_lst

    def _edge_lookup(self, edge_obj, id_strct):
        """
        Follow an edge given a key.

        This method uses the data in the edge_object
        to find one key to another key using one of
        several types of lookup functions.
        :param edge:
        :param key:
        :return:
        """
        return edge_obj.edge_lookup(self, id_strct)
