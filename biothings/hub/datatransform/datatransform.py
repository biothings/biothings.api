import re

from .histogram import Histogram
from biothings.utils.common import iter_n
from biothings.utils.common import is_str
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
        self.debug = {}
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
        # ensure it's hashable
        if not type(left) in [list,tuple]:
            left = [left]
        if not type(right) in [list,tuple]:
            right = [right]
        if type(left) == list:
            left = tuple(left)
        if type(right) == list:
            right = tuple(right)
        for v in left:
            if v not in self.forward.keys():
                self.forward[v] = right
            else:
                self.forward[v] = self.forward[v] + right
        for v in right:
            if v not in self.inverse.keys():
                self.inverse[v] = left
            else:
                self.inverse[v] = self.inverse[v] + left

    def __iadd__(self, other):
        """object += additional, which combines lists"""
        if not isinstance(other, IDStruct):
            raise TypeError("other is not of type IDStruct")
        for (left, right) in other:
            self.add(left, right)
            # retain debug information
            self.transfer_debug(left, other)
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

    def side(self,_id,where):
        if type(_id) == list:
            _id = tuple(_id)
        return _id in where.keys()

    def left(self, id):
        """Determine if the id (left, _) is registered"""
        return self.side(id,self.forward)

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
        return self.side(id,self.inverse)

    def find_right(self, ids):
        """Find the first id founding by searching the (_, right) identifiers"""
        return self.find(self.inverse,ids)

    def set_debug(self, left, label, right):
        # lowercase left and right keys
        if is_str(left):
            left = left.lower()
        if is_str(right):
            right = right.lower()
        # capture the label if it is used
        if label:
            right = (label, right)
        try:
            self.debug[left] = self.debug[left] + [right]
        except KeyError:
            self.debug[left] = [left, right]

    def get_debug(self, key):
        # lowercase key if possible
        if is_str(key):
            key = key.lower()
        # return debug information
        try:
            return self.debug[key]
        except KeyError:
            return 'not-available'

    def import_debug(self, other):
        """
        import debug information the entire IDStruct object
        """
        for id in other.debug.keys():
            self.transfer_debug(id, other)

    def transfer_debug(self, key, other):
        """
        transfer debug information for one key in the IDStruct object
        """
        # ensure lower case key
        if is_str(key):
            key = key.lower()
        # transfer debug information
        self.debug[key] = other.get_debug(key)


class DataTransform(object):
    # Constants
    batch_size = 1000
    DEFAULT_WEIGHT = 1
    default_source = '_id'
    debug = False

    def __init__(self, input_types, output_types, id_priority_list=[], skip_on_failure=False, skip_w_regex=None,
                 idstruct_class=IDStruct, copy_from_doc=False, debug=False):
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
        :param id_priority_list: A priority list of identifiers to to sort input and output types by.
        :type id_priority_list: list(str)
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
        self.id_priority_list = id_priority_list

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
        """In the base class, all input_types and output_types are valid."""
        return True

    def _parse_output_types(self, output_types):
        """
        Parse through output_types
        :param output_types:
        :return:
        """
        if not isinstance(output_types, list):
            raise ValueError("output_types should be of type list")
        for output_type in output_types:
            if not self._valid_output_type(output_type):
                raise ValueError("output_type is not a node in the key_lookup graph")
        return output_types

    def _valid_output_type(self, output_type):
        """In the base class, all input_types and output_types are valid."""
        return True

    def __call__(self, f, debug=None):
        """
        Perform the data transformation on all documents on call.
        :param f: function to apply to
        :param debug: Enable debugging information.
        :type debug: bool
        :return:
        """
        # additional handling for the debug option
        if not debug:
            self.debug = False
        elif isinstance(debug, list):
            self.logger.debug("DataTransform Debug Mode:  {}".format(debug))
            self.debug = debug

        def wrapped_f(*args):
            input_docs = f(*args)
            output_doc_cnt = 0
            # split input_docs into chunks of size self.batch_size
            for batchiter in iter_n(input_docs, int(self.batch_size / len(self.input_types))):
                output_docs = self.key_lookup_batch(batchiter)
                for odoc in output_docs:
                    # print debug information if available
                    if self.debug and 'dt_debug' in odoc:
                        self.logger.debug("DataTransform Debug doc['dt_debug']:  {}".format(odoc['dt_debug']))
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

    def lookup_one(self, doc):
        """
        KeyLookup on document.  This method is called as a function call instead of a
        decorator on a document iterator.
        """
        # special handling for the debug option
        self.debug = [doc['_id']]

        output_docs = self.key_lookup_batch([doc])
        for odoc in output_docs:
            # print debug information if available
            if self.debug and 'dt_debug' in odoc:
                self.logger.debug("DataTransform Debug doc['dt_debug']:  {}".format(odoc['dt_debug']))
            yield odoc
        self.logger.info("DataTransform.histogram:  {}".format(self.histogram))

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

    @property
    def id_priority_list(self):
        return self._id_priority_list

    @id_priority_list.setter
    def id_priority_list(self, value):
        self._id_priority_list = value
        self.input_types = self.sort_input_by_priority_list(self.input_types)
        self.output_types = self.sort_output_by_priority_list(self.output_types)

    def sort_input_by_priority_list(self, input_types):
        """
        Reorder the given input_types to follow a priority list.  Inputs not in the
        priority list should remain in their given order at the end of the list.
        """
        # construct temporary id_priority_list with extra elements at the end
        id_priority_list = self._expand_priority_order([x[0] for x in input_types])
        input_types = sorted(input_types, key=lambda e: self._priority_order(id_priority_list, e[0]))
        return input_types

    def sort_output_by_priority_list(self, output_types):
        """
        Reorder the given output_types to follow a priority list.  Outputs not in the
        priority list should remain in their given order at the end of the list.
        """
        # construct temporary id_priority_list with extra elements at the end
        id_priority_list = self._expand_priority_order(output_types)
        output_types = sorted(output_types, key=lambda e: self._priority_order(id_priority_list, e))
        return output_types

    def _expand_priority_order(self, id_list):
        """
        Expand the self.id_priority_list to also include elements in id_list that are not
        in the priority list.  These elements are added to the priority list in the order
        that they appear in the id_list.

        Example:
        > self.id_priority_list = ['a', 'c']
        > self._expand_priority_order(['a', 'd', 'e'])
        ['a', 'c', 'd', 'e']
        """
        res = self.id_priority_list.copy()
        for id in id_list:
            if id not in self.id_priority_list:
                res.append(id)
        return res

    def _priority_order(self, id_priority_list, elem):
        """
        Determine the priority order of an input_type following a id_priority_list.
        This list, first defined in DataTransformMDB is used to reorder the input_types
        so that their order matches the id types listed in id_priority_list.  If an id
        type is not in that list then the input_type will be placed at the end of the list
        in arbitrary order.
        """
        assert(isinstance(id_priority_list, list))
        # match id types with id priority
        for i, e in enumerate(id_priority_list):
            if elem  == e:
                return i
        # the id type is not in id_priority_list so it will be placed last
        return len(id_priority_list) + 1


class DataTransformEdge(object):
    def __init__(self, label=None):
        self.prepared = False
        self.label = label
        self.init_state()

    def edge_lookup(self, keylookup_obj, id_strct, debug=False):
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
        self.logger,_ = get_logger('datatransform')

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
    """
    The RegExEdge allows an identifier to be transformed using a regular expression. POSIX regular expressions are supported.
    """

    def __init__(self, from_regex, to_regex, weight=1):
        """
        :param from_regex: The first parameter of the regular expression substitution.
        :type from_regex: str
        :param to_regex: The second parameter of the regular expression substitution.
        :type to_regex: str
        :param weight: Weights are used to prefer one path over another. The path with the lowest weight is preferred. The default weight is 1.
        :type weight: int
        """
        self.from_regex = from_regex
        self.to_regex = to_regex
        self.weight = weight

    def edge_lookup(self, keylookup_obj, id_strct, debug=False):
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
            if type(value) in [list,tuple]:
                # assuming we have a list of dict with k as one of the keys
                stype = set([type(e) for e in value])
                if not stype:
                    return None
                assert len(stype) == 1 and stype == {dict}, "Expecting a list of dict, found types: %s" % stype
                value = [e[k] for e in value if e.get(k)]
                # can't go further ?
                return value
            else:
                value = value[k]
    except KeyError:
        return None

    return value
