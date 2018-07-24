import copy
import re

from biothings.utils.keylookup import KeyLookup
from networkx import all_simple_paths, nx
import biothings.utils.mongo as mongo
from biothings.utils.loggers import get_logger
from biothings import config as btconfig
from biothings import config_for_app

# Configuration of collections from biothings config file
config_for_app(btconfig)

# Setup logger and logging level
kl_log = get_logger('keylookup', btconfig.LOG_FOLDER)


class KeyLookupEdge(object):
    def edge_lookup(self, keylookup_obj, id_strct):
        """
        virtual method for edge lookup.  Each edge class is
        responsible for its own lookup procedures given a
        keylookup_obj and an id_strct
        :param keylookup_obj:
        :param id_strct: - list of tuples (orig_id, current_id)
        :return:
        """
        pass


class MongoDBEdge(KeyLookupEdge):
    """
    KeyLookupEdge object for MongoDB queries
    """
    def __init__(self, collection, lookup, field, weight=1):
        self.col = collection
        self.lookup = lookup
        self.field = field
        self.weight = weight

    def edge_lookup(self, keylookup_obj, id_strct):
        """
        Follow an edge given a key.

        An edge represets a document and this method uses the data in the edge_object
        to find one key to another key using exactly one mongodb lookup.
        :param keylookup_obj:
        :param id_strct:
        :return:
        """
        if self.col not in keylookup_obj.get_collections().keys():
            return None

        # build id_lst
        id_set = set()
        for (orig_id, curr_id) in id_strct:
            id_set.add(curr_id)
        id_lst = list(id_set)

        find_lst = keylookup_obj.get_collections()[self.col].find({self.lookup: {"$in": id_lst}}, {self.lookup: 1, self.field: 1})

        # Build up a new_id_strct from the find_lst
        new_id_strct = []
        for d in find_lst:
            for (orig_id, curr_id) in id_strct:
                if curr_id == d[self.lookup]:
                    new_id_strct.append((orig_id, d[self.field]))
        return new_id_strct


class APIEdge(KeyLookupEdge):
    """
    APIEdge - KeyLookupEdge object for API calls
    """
    def __init__(self, client, scope, field, weight=1):
        self.client = client
        self.scope = scope
        self.field = field
        self.weight = weight

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
        id_lst = []
        for (orig_id, curr_id) in id_strct:
            id_lst.append(curr_id)

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
        kl_log.debug("QueryMany Structure:  {}".format(qr))
        qm_struct = []
        for q in qr['out']:
            query = q['query']
            val = keylookup_obj._nested_lookup(q, field)
            if val:
                for (orig_id, curr_id) in id_strct:
                    if query == curr_id:
                        qm_struct.append((orig_id, val))
        return qm_struct


class KeyLookupMDBBatch(KeyLookup):
    # Constants
    batch_size = 10
    default_source = '_id'

    def __init__(self, G, collections, input_types, output_types, skip_on_failure=False, skip_w_regex=None):
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
        self.collections = None
        self.collection_names = collections

        super().__init__(input_types, output_types, skip_on_failure, skip_w_regex)

        self._precompute_paths()

    def _valid_input_type(self, input_type):
        return input_type.lower() in self.G.nodes()

    def _valid_output_type(self, output_type):
        return output_type.lower() in self.G.nodes()

    def _load_collections(self, collections):
        """
        Load all mongodb collections specified in the configuration data structure col_keys.
        :return:
        """
        self.collections = {}
        for col in collections:
            collection = mongo.get_src_db()[col]
            if collection.count() > 0:
                self.collections[col] = collection
                kl_log.info("Registering collection:  {} (count:  {})".format(col, collection.count()))
        if not self.collections:
            raise ValueError("At least one configured collection is required for MongoDB key lookup.")

    def _validate_graph(self, G):
        """
        Check if the input configuration graph G has a valid structure.
        :param G: key_lookup configuration graph
        :return:
        """
        for (v1, v2) in G.edges():
            if 'object' not in G.edges[v1, v2].keys():
                raise ValueError("edge_object for ({}, {}) is missing".format(v1, v2))
            edge_object = G.edges[v1, v2]['object']
            if not isinstance(edge_object, KeyLookupEdge):
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
        kl_log.debug("INPUT DOCUMENTS:")
        for doc in batchiter:
            kl_log.debug(doc)
            doc_lst.append(doc)

        output_docs = []
        miss_lst = []
        for doc in doc_lst:
            if self.skip_w_regex and self.skip_w_regex.match(doc['_id']):
                output_docs.append(doc)
            else:
                miss_lst.append(doc)

        for output_type in self.output_types:
            for input_type in self.input_types:
                (tmp_hit_lst, miss_lst) = self.travel(input_type, output_type, miss_lst)
                output_docs += tmp_hit_lst
                kl_log.debug("Output documents from travel:")
                for doc in tmp_hit_lst:
                    kl_log.debug(doc)

        # Keep the misses if we do not skip on failure
        if not self.skip_on_failure:
            output_docs += miss_lst

        return output_docs

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
            path_strct = set()
            for doc in doc_lst:
                if input_type[1] in doc.keys():
                    path_strct.add((doc[input_type[1]], doc[input_type[1]]))
            path_strct = list(path_strct)
            return path_strct

        def _build_hit_miss_lsts(doc_lst, saved_hits):
            """
            Return a list of documents that have had their identifiers replaced
            also return a list of documents that were not changed
            :param doc_lst:
            :param saved_hits:
            :return:
            """
            hit_lst = []
            miss_lst = []
            for d in doc_lst:
                hit_flag = False
                for (orig_id, lookup_id) in saved_hits:
                    if input_type[1] in d.keys():
                        if orig_id == d[input_type[1]]:
                            new_doc = copy.deepcopy(d)
                            new_doc['_id'] = lookup_id
                            hit_lst.append(new_doc)
                            hit_flag = True
                if not hit_flag:
                    miss_lst.append(d)
            return hit_lst, miss_lst

        kl_log.debug("Travel From '{}' To '{}'".format(input_type[0], target))

        # Keep a running list of all saved hits
        saved_hits = []

        # Build the path structure, which will save results
        path_strct = _build_path_strct(input_type, doc_lst)

        for path in map(nx.utils.misc.pairwise, self.paths[(input_type[0], target)]):
            for (v1, v2) in path:
                kl_log.debug("travel_edge:  {} - {}".format(v1, v2))
                edge = self.G.edges[v1, v2]['object']
                path_strct = self._edge_lookup(edge, path_strct)

                kl_log.debug("Travel id_lst:  {}".format(path_strct))

            # save the hits from the path
            saved_hits += path_strct

            # reset the state to lookup misses
            path_strct = []
            for doc in doc_lst:
                if input_type[1] in doc.keys():
                    if doc[input_type[1]] not in [i[0] for i in saved_hits]:
                        path_strct.append((doc[input_type[1]], doc[input_type[1]]))

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

    def get_collections(self):
        """
        Standard 'getter' for self.collections objects.
        :return:
        """
        if self.collections:
            return self.collections
        else:
            self._load_collections(self.collection_names)
            return self.collections
