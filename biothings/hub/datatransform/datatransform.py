import re

from .histogram import Histogram
from biothings.utils.common import iter_n
from biothings.utils.loggers import get_logger


class IDStruct(object):
    """
    IDStruct - id structure for use with the DataTransform classes.  The basic idea
    is to provide a structure that provides a list of (original_id, current_id)
    pairs.
    """
    def __init__(self, field=None, doc_lst=None):
        """
        Initialize the structure
        :param field: field for documents to use as an initial id (optional)
        :param doc_lst: list of documents to use when building an initial list (optional)
        """
        self.forward = {}
        self.inverse = {}
        if field and doc_lst:
            self._init_strct(field, doc_lst)

    def _init_strct(self, field, doc_lst):
        """initialze _id_tuple_lst"""
        for doc in doc_lst:
            value = nested_lookup(doc, field)
            if value:
                self.add(value, value)

    def __iter__(self):
        """iterator overload function"""
        for k in self.forward.keys():
            for f in self.forward[k]:
                yield k, f

    def add(self, left, right):
        """add a (original_id, current_id) pair to the list"""
        if not left or not right:
            return  # identifiers cannot be None
        if self.lookup(left, right):
            return  # tuple already in the list
        if left not in self.forward.keys():
            self.forward[left] = [right]
        else:
            self.forward[left] = self.forward[left] + [right]
        if right not in self.inverse.keys():
            self.inverse[right] = [left]
        else:
            self.inverse[right] = self.inverse[right] + [left]

    def __iadd__(self, other):
        """object += additional, which combines lists"""
        if not isinstance(other, IDStruct):
            raise TypeError("other is not of type IDStruct")
        for (left, right) in other:
            self.add(left, right)
        return self

    def __len__(self):
        """Return the number of keys (forward direction)"""
        return len(self.forward.keys())

    def __str__(self):
        """convert to a string, useful for debugging"""
        lst = []
        for k in self.forward.keys():
            for f in self.forward[k]:
                lst.append((k, f))
        return str(lst)

    @property
    def id_lst(self):
        """Build up a list of current ids"""
        id_set = set()
        for k in self.forward.keys():
            for f in self.forward[k]:
                id_set.add(f)
        return list(id_set)

    def lookup(self, left, right):
        """Find if a (left, right) pair is already in the list"""
        for r in self.find_left(left):
            if right == r:
                return True
        return False

    def left(self, id):
        """Determine if the id (left, _) is registered"""
        return id in self.forward.keys()

    def find(self,where,ids):
        if not ids:
            return
        if not type(ids) in (list,tuple):
            ids = [ids]
        for id in ids:
            if id in where.keys():
                for i in where[id]:
                    yield i

    def find_left(self, ids):
        return self.find(self.forward,ids)

    def right(self, id):
        """Determine if the id (_, right) is registered"""
        return id in self.inverse.keys()

    def find_right(self, ids):
        """Find the first id founding by searching the (_, right) identifiers"""
        return self.find(self.inverse,ids)


class DataTransform(object):
    # Constants
    batch_size = 1000
    DEFAULT_WEIGHT = 1
    default_source = '_id'

    def __init__(self, input_types, output_types, skip_on_failure=False, skip_w_regex=None,
                 idstruct_class=IDStruct, copy_from_doc=False):
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
        :param id_struct_class: IDStruct used to manager/fetch IDs from docs
        :param copy_from_doc: if transform failed using the graph, try to get
               value from the document itself when output_type == input_type.
               No check is performed, it's a straight copy. If checks are needed
               (eg. check that an ID referenced in the doc actually exists in
               another collection, nodes with self-loops can be used, so
               ID resolution will be forced to go through these loops to ensure
               data exists)
        """

        self.input_types = self._parse_input_types(input_types)
        self.output_types = self._parse_output_types(output_types)

        if not isinstance(skip_on_failure, bool):
            raise ValueError("skip_on_failure should be of type bool")
        self.skip_on_failure = skip_on_failure

        if skip_w_regex and not isinstance(skip_w_regex, str):
            raise ValueError('skip_w_regex must be a string')
        elif not skip_w_regex:
            self.skip_w_regex = None
        else:
            self.skip_w_regex = re.compile(skip_w_regex)

        self.idstruct_class = idstruct_class
        self.copy_from_doc = copy_from_doc

        self.histogram = Histogram()
        # Setup logger and logging level
        self.logger,_ = get_logger('datatransform')

    def _parse_input_types(self, input_types):
        """
        Parse the input_types argument
        :return:
        """
        res_input_types = []
        if isinstance(input_types, str):
            input_types = [input_types]
        if isinstance(input_types, list):
            for input_type in input_types:
                if isinstance(input_type, tuple) or isinstance(input_type, list):
                    if not self._valid_input_type(input_type[0]):
                        raise ValueError("input_type '%s' is not a node in the key_lookup graph" % repr(input_type[0]))
                    res_input_types.append((input_type[0].lower(), input_type[1]))
                elif isinstance(input_type, str):
                    if not self._valid_input_type(input_type.lower()):
                        raise ValueError("input_type '%s' is not a node in the key_lookup graph" % repr(input_type))
                    res_input_types.append((input_type, self.default_source))
                else:
                    raise ValueError('Provided input_types is not of the correct type')
        else:
            raise ValueError('Provided input_types is not of the correct type')
        return res_input_types

    def _valid_input_type(self, input_type):
        pass

    def _parse_output_types(self, output_types):
        """
        Parse through output_types
        :param output_types:
        :return:
        """
        if not isinstance(output_types, list):
            raise ValueError("output_types should be of type list")
        for output_type in output_types:
            if not self._valid_input_type(output_type):
                raise ValueError("output_type is not a node in the key_lookup graph")
        return output_types

    def _valid_output_type(self, output_type):
        pass

    def __call__(self, f):
        """
        Perform the data transformation on all documents on call.
        :param f: function to apply to
        :return:
        """
        def wrapped_f(*args):
            input_docs = f(*args)
            output_doc_cnt = 0
            # split input_docs into chunks of size self.batch_size
            for batchiter in iter_n(input_docs, int(self.batch_size / len(self.input_types))):
                output_docs = self.key_lookup_batch(batchiter)
                for odoc in output_docs:
                    output_doc_cnt += 1
                    yield odoc
            self.logger.info("wrapped_f Num. output_docs:  {}".format(output_doc_cnt))
            self.logger.info("DataTransform.histogram:  {}".format(self.histogram))

        return wrapped_f

    def key_lookup_batch(self, batchiter):
        """
        Core method for looking up all keys in batch (iterator)
        :param batchiter:
        :return:
        """
        pass

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

        return str(value)


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
        yield NotImplemented("This method must be overridden by the base class.")

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
        self.logger,_ = get_logger('keylookup')

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


class RegExEdge(DataTransformEdge):
    def __init__(self, from_regex, to_regex, weight=1):
        self.from_regex = from_regex
        self.to_regex = to_regex
        self.weight = weight

    def edge_lookup(self, keylookup_obj, id_strct):
        """
        Transform identifiers using a regular expression substitution.
        """
        res_id_strct = IDStruct()
        for (left, right) in id_strct:
            res_id_strct.add(left, re.sub(self.from_regex, self.to_regex, right))
        return res_id_strct


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

    return value
