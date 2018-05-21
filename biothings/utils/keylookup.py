import copy
import logging
import re
import types

from networkx import all_simple_paths, nx
import biothings.utils.mongo as mongo
from biothings import config_for_app

import config

# Configuration of collections from biothings config file
config_for_app(config)

# Setup logger and logging level
logging.basicConfig()
kl_log = logging.getLogger('keylookup_networkx')
kl_log.setLevel(logging.ERROR)


class KeyLookup(object):
    # Constants
    DEFAULT_WEIGHT = 1

    def __init__(self, G, collections, input_type, output_types, skip_on_failure=False):
        """
        Initialize the keylookup object and precompute paths from the
        start key to all target keys.

        The decorator is intended to be applied to the load_data function
        of an uploader.  The load_data function yields documents, which
        are then post processed by call and the 'id' key conversion is
        performed.

        :param G:
        :param collections:
        :param input_type:
        :param output_types:
        """
        self.G = G
        self.input_type = input_type
        self.output_types = output_types
        self.skip_on_failure = skip_on_failure

        self._load_collections(collections)
        self._precompute_paths()


    def __call__(self, f):
        """
        Perform the key conversion and all lookups on call.

        :param f:
        :return:
        """
        def wrapped_f(*args):
            input_docs = f(*args)
            kl_log.info("input: %s" % input_docs)
            output_docs = []
            for doc in input_docs:
                kl_log.info("Decorator arguments:  {}".format(self.input_type))
                kl_log.info("Input document:  {}".format(doc))
                keys = None
                for output_type in self.output_types:
                    keys = self.travel(self.input_type, output_type, doc['_id'])
                    # Key(s) were found, create new documents
                    # and add them to the output list
                    if keys:
                        for k in keys:
                            new_doc = copy.deepcopy(doc)
                            new_doc['_id'] = k
                            output_docs.append(new_doc)
                        break

                # No keys were found, keep the original (unless the skip_on_failure option is passed)
                if not keys and not self.skip_on_failure:
                    output_docs.append(doc)

            for odoc in output_docs:
                kl_log.info("yield odoc: %s" % odoc)
                yield odoc

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
                kl_log.info("Loading collection:  {} (count:  {})".format(col, collection.count()))

    def _precompute_paths(self):
        """
        Precompute all paths from the given key_type to all target key types
        provided on initialization.
        :return:
        """
        self.paths = {}
        for output_type in self.output_types:
            kl_log.info("Target Key:  {}".format(output_type))
            paths = \
                all_simple_paths(self.G, self.input_type, output_type)
            # Sort by path length - try the shortest paths first
            paths = sorted(paths, key=self._compute_path_weight)
            self.paths[(self.input_type, output_type)] = paths

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
        kl_log.info("Travel Target:  {}".format(target))
        start_key = key
        for path in map(nx.utils.misc.pairwise, self.paths[(start, target)]):
            keys = [start_key]
            for (v1, v2) in path:
                kl_log.info("travel_edge:  {} - {}".format(v1, v2))
                edge = self.G.edges[v1, v2]['object']
                new_keys = []
                for k in keys:
                    followed_keys = self._follow_edge(edge, k)
                    if followed_keys:
                        new_keys = new_keys + followed_keys
                keys = new_keys
                kl_log.info("new_key ({})".format(keys))
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
        kl_log.info(edge)

        col = edge['col']
        lookup = edge['lookup']
        # Apply the lookup regex if it exists
        if 'lookup_regex' in edge.keys():
            key = re.sub(edge['lookup_regex'][0], edge['lookup_regex'][1], key)
        field = edge['field']

        kl_log.info("key_lookup:  {} - {} - {} - {}".format(col, lookup, field, key))

        # Valid state checks
        if col not in self.collections.keys():
            return None

        keys = []
        for doc in self.collections[col].find({lookup: key}):
            kl_log.info("document retrieved - looking up value")
            keys = keys + [self._nested_lookup(doc, col, field)]
        return keys

    def _nested_lookup(self, d, col, field):
        """
        Performs a nested lookup of self.docs[col] using a period (.) delimited
        list of fields.  This is a nested dictionary lookup.
        :param col: collection to lookup (cached)
        :param field: period delimited list of fields
        :return:
        """

        value = d
        keys = field.split('.')
        try:
            for k in keys:
                value = value[k]
        except KeyError:
            return None

        return value
