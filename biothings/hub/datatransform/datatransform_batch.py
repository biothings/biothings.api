import biothings_client
import copy
import re

from networkx import all_simple_paths, nx
from biothings.utils.common import iter_n
from biothings.hub.datatransform.datatransform import DataTransform
import biothings.utils.mongo as mongo
from biothings.utils.loggers import get_logger
from biothings import config as btconfig
from biothings import config_for_app

# Configuration of collections from biothings config file
config_for_app(btconfig)

# Setup logger and logging level
kl_log = get_logger('keylookup', btconfig.LOG_FOLDER)


def nested_lookup(doc, field):
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

    return str(value)


class IDStruct(object):
    """
    IDStruct - id structure for use with the IDLookup classes.  The basic idea
    is to provide a structure that provides a list of (original_id, current_id)
    pairs.
    """
    def __init__(self, field=None, doc_lst=None):
        """
        Initialize the structure
        :param field: field for documents to use as an initial id (optional)
        :param doc_lst: list of documents to use when building an initial list (optional)
        """
        self._id_tuple_lst = []
        if field and doc_lst:
            self._id_tuple_lst = self._init_strct(field, doc_lst)

    def _init_strct(self, field, doc_lst):
        """initialze _id_tuple_lst"""
        strct = set()
        for doc in doc_lst:
            if field in doc.keys():
                strct.add((doc[field], doc[field]))
        return list(strct)

    def __iter__(self):
        """iterator overload function"""
        return iter(self._id_tuple_lst)

    def add(self, orig_id, curr_id):
        """add a (original_id, current_id) pair to the list"""
        self._id_tuple_lst.append((orig_id, curr_id))

    def __iadd__(self, other):
        """object += additional, which combines lists"""
        if not isinstance(other, IDStruct):
            raise TypeError("other is not of type IDStruct")
        self._id_tuple_lst += other._id_tuple_lst
        return self

    def __str__(self):
        """convert to a string, useful for debugging"""
        return str(self._id_tuple_lst)

    @property
    def id_lst(self):
        """Build up a list of current ids"""
        id_set = set()
        for (orig_id, curr_id) in self._id_tuple_lst:
            id_set.add(curr_id)
        return list(id_set)

    def find_left(self, id):
        """Find the first id by searching the (left, _) identifiers"""
        for (orig_id, curr_id) in self._id_tuple_lst:
            if id == orig_id:
                yield curr_id

    def find_right(self, id):
        """Find the first id founding by searching the (_, right) identifiers"""
        for (orig_id, curr_id) in self._id_tuple_lst:
            if id == curr_id:
                yield orig_id


class DataTransformEdge(object):
    def __init__(self):
        self.prepared = False
        self.init_state()

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

    def init_state(self):
        self._state = {
            "logger": None
        }

    @property
    def logger(self):
        if not self._state["logger"]:
            self.prepare()
        return self._state["logger"]

    @logger.setter
    def logger(self, value):
        self._state["logger"] = value

    def setup_log(self):
        self.logger = get_logger('keylookup', btconfig.LOG_FOLDER)

    def prepare(self, state={}):
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self.setup_log()

    def unprepare(self):
        """
        reset anything that's not picklable (so self can be pickled)
        return what's been reset as a dict, so self can be restored
        once pickled
        """
        state = {
            "logger": self._state["logger"],
        }
        for k in state:
            self._state[k] = None
        self.prepared = False
        return state


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

        id_lst = id_strct.id_lst
        find_lst = self.collection.find({self.lookup: {"$in": id_lst}}, {self.lookup: 1, self.field: 1})

        # Build up a new_id_strct from the find_lst
        new_id_strct = IDStruct()
        for d in find_lst:
            for orig_id in id_strct.find_right(d[self.lookup]):
                new_id_strct.add(orig_id, d[self.field])
        return new_id_strct


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


class DataTransformBatch(DataTransform):
    # Constants
    batch_size = 10
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
        kl_log.debug("INPUT DOCUMENTS:")
        for doc in batchiter:
            kl_log.debug(doc)
            doc_lst.append(doc)

        output_docs = []
        miss_lst = []
        for doc in doc_lst:
            if self.skip_w_regex and self.skip_w_regex.match(doc['_id']):
                yield doc
            else:
                miss_lst.append(doc)

        for output_type in self.output_types:
            for input_type in self.input_types:
                (hit_lst, miss_lst) = self.travel(input_type, output_type, miss_lst)
                kl_log.debug("Output documents from travel:")
                for doc in hit_lst:
                    kl_log.debug(doc)
                    yield doc

        # Keep the misses if we do not skip on failure
        if not self.skip_on_failure:
            for doc in miss_lst:
                yield doc

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
                if input_type[1] in d.keys():
                    for lookup_id in id_strct.find_left(d[input_type[1]]):
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
                kl_log.debug("travel_edge:  {} - {}".format(v1, v2))
                edge = self.G.edges[v1, v2]['object']
                path_strct = self._edge_lookup(edge, path_strct)

                kl_log.debug("Travel id_lst:  {}".format(path_strct))

            if not saved_hits:
                raise TypeError("saved_hits is None (checkpoint 1)")
            # save the hits from the path
            saved_hits += path_strct
            if not saved_hits:
                raise TypeError("saved_hits is None (checkpoint 2)")

            # reset the state to lookup misses
            path_strct = IDStruct()
            for doc in doc_lst:
                if input_type[1] in doc.keys():
                    val = saved_hits.find_left(doc[input_type[1]])
                    if not val:
                        path_strct.add(doc[input_type[1]], doc[input_type[1]])

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
