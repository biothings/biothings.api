import copy
import logging
import re

from networkx import all_simple_paths, nx
import biothings.utils.mongo as mongo
from biothings.utils.loggers import get_logger
from biothings import config as btconfig

# Setup logger and logging level
kl_log = get_logger('keylookup', btconfig.LOG_FOLDER)


class KeyLookup(object):
    # Constants
    DEFAULT_WEIGHT = 1
    default_source = '_id'

    def __init__(self, G, collections, input_types, output_types, skip_on_failure=False):
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

        self.input_types = []
        if isinstance(input_types, str):
            input_types = [input_types]
        if isinstance(input_types, list):
            for input_type in input_types:
                if isinstance(input_type, tuple) or isinstance(input_type, list):
                    if input_type[0].lower() not in self.G.nodes():
                        raise ValueError("input_type %s is not a node in the key_lookup graph" % repr(input_type[0]))
                    self.input_types.append((input_type[0].lower(), input_type[1]))
                elif isinstance(input_type, str):
                    if input_type.lower() not in self.G.nodes():
                        raise ValueError("input_type %s is not a node in the key_lookup graph" % repr(input_type))
                    self.input_types.append((input_type, self.default_source))
                else:
                    raise ValueError('Provided input_types is not of the correct type')
        else:
            raise ValueError('Provided input_types is not of the correct type')

        if not isinstance(output_types, list):
            raise ValueError("output_types should be of type list")
        for output_type in output_types:
            if output_type not in self.G.nodes():
                raise ValueError("output_type is not a node in the key_lookup graph")
        self.output_types = output_types

        if not isinstance(skip_on_failure, bool):
            raise ValueError("skip_on_failure should be of type bool")
        self.skip_on_failure = skip_on_failure

        self.collections = None
        self.collection_names = collections

        self._precompute_paths()


    def __call__(self, f):
        """
        Perform the key conversion and all lookups on call.

        :param f:
        :return:
        """
        def wrapped_f(*args):
            kl_log.info("Converting _id from (type,key) %s to type %s" % \
                    (repr(self.input_types),repr(self.output_types)))
            input_docs = f(*args)
            kl_log.debug("input: %s" % input_docs)
            output_docs = []
            for doc in input_docs:
                kl_log.debug("Decorator arguments:  {}".format(self.input_types))
                kl_log.debug("Input document:  {}".format(doc))
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
                                yield new_doc
                                #output_docs.append(new_doc)
                            break
                    # Break out of the outer loop if keys were found
                    if keys:
                        break

                # No keys were found, keep the original (unless the skip_on_failure option is passed)
                if not keys and not self.skip_on_failure:
                    yield doc
                    #output_docs.append(doc)

            #for odoc in output_docs:
            #    #kl_log.info("yield odoc: %s" % odoc)
            #    yield odoc

        return wrapped_f

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

    @staticmethod
    def _nested_lookup(doc, field):
        """
        Performs a nested lookup of doc using a period (.) delimited
        list of fields.  This is a nested dictionary lookup.
        :param doc: document to perform lookup on
        :param field: period delimited list of fields
        :return:
        """
        value = doc
        keys = field.split('.')
        try:
            for k in keys:
                value = value[k]
        except KeyError:
            return None

        return value

