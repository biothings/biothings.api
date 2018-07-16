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


class KeyLookupMDB(KeyLookup):
    # Constants
    DEFAULT_WEIGHT = 1
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
            if 'col' not in edge_object.keys():
                raise ValueError("edge_object for ({}, {}) is missing the 'col' field".format(v1, v2))
            if 'lookup' not in edge_object.keys():
                raise ValueError("edge_object for ({}, {}) is missing the 'lookup' field".format(v1, v2))
            if 'field' not in edge_object.keys():
                raise ValueError("edge_object for ({}, {}) is missing the 'field' field".format(v1, v2))

    def _precompute_paths(self):
        """
        Precompute all paths from the given key_type to all target key types
        provided on initialization.
        :return:
        """
        self.paths = {}
        for output_type in self.output_types:
            kl_log.info("Target Key:  {}".format(output_type))
            for input_type in self.input_types:
                paths = \
                    all_simple_paths(self.G, input_type[0], output_type)
                # Sort by path length - try the shortest paths first
                paths = sorted(paths, key=self._compute_path_weight)
                self.paths[(input_type[0], output_type)] = paths

    def key_lookup_batch(self, batchiter):
        """
        Look up all keys for ids given in the batch iterator (1 block)
        :param batchiter:  1 lock of records to look up keys for
        :return:
        """
        output_docs = []
        for doc in batchiter:

            # Skip (regex) logic
            if self.skip_w_regex and self.skip_w_regex.match(doc['_id']):
                output_docs.append(doc)
                continue

            keys = None
            for output_type in self.output_types:
                for input_type in self.input_types:
                    keys = self.travel(input_type[0], output_type, KeyLookup._nested_lookup(doc, input_type[1]))
                    # Key(s) were found, create new documents
                    # and add them to the output list
                    if keys:
                        for k in keys:
                            new_doc = copy.deepcopy(doc)
                            new_doc['_id'] = k
                            output_docs.append(new_doc)
                        break
                # Break out of the outer loop if keys were found
                if keys:
                    break
            # No keys were found, keep the original (unless the skip_on_failure option is passed)
            if not keys and not self.skip_on_failure:
                output_docs.append(doc)
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
                edge = self.G.edges[v1, v2]
                if 'weight' in edge:
                    weight = weight + edge['weight']
                else:
                    weight = weight + KeyLookup.DEFAULT_WEIGHT
        return weight

    def travel(self, start, target, key):
        """
        Traverse a graph from a start key type to a target key type using
        precomputed paths.

        :param start: key type to start from
        :param target: key type to end at
        :param key: key value of type 'start'
        :return:
        """
        kl_log.debug("Travel Target:  {}".format(target))
        start_key = key
        for path in map(nx.utils.misc.pairwise, self.paths[(start, target)]):
            keys = [start_key]
            for (v1, v2) in path:
                kl_log.debug("travel_edge:  {} - {}".format(v1, v2))
                edge = self.G.edges[v1, v2]['object']
                new_keys = []
                for k in keys:
                    followed_keys = self._follow_edge(edge, k)
                    if followed_keys:
                        new_keys = new_keys + followed_keys
                keys = new_keys
                kl_log.debug("new_key ({})".format(keys))
                if not keys:
                    break

            # We have an answer
            if keys:
                return keys

    def _follow_edge(self, edge, key):
        """
        Follow an edge given a key.

        An edge represets a document and this method uses the data is the edge_object
        to find one key to another key using exactly one mongodb lookup.

        :param edge:
        :param key:
        :return:
        """
        # valid-state: key must be a string
        if not isinstance(key, str):
            return None

        col = edge['col']
        # valid-state: col must be a registered collection
        if col not in self.get_collections().keys():
            return None
        lookup = edge['lookup']
        # Apply the lookup regex if it exists
        if 'lookup_regex' in edge.keys():
            key = re.sub(edge['lookup_regex'][0], edge['lookup_regex'][1], key)
        field = edge['field']

        kl_log.debug("key_lookup:  {} - {} - {} - {}".format(col, lookup, field, key))

        keys = []
        for doc in self.get_collections()[col].find({lookup: key}):
            keys = keys + [KeyLookup._nested_lookup(doc, field)]
        return keys

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
