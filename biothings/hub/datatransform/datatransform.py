"""
DataTransform Module
- IDStruct
- DataTransform (superclass)
"""
# pylint: disable=E0401, E0611
import re
from functools import wraps
from biothings.utils.common import iter_n
from biothings.utils.common import is_str
from biothings.utils.loggers import get_logger
from .histogram import Histogram


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
        for key in self.forward:
            for val in self.forward[key]:
                yield key, val

    def add(self, left, right):
        """add a (original_id, current_id) pair to the list"""
        if not left or not right:
            return  # identifiers cannot be None
        if self.lookup(left, right):
            return  # tuple already in the list
        # ensure it's hashable
        if not isinstance(left, (list, tuple)):
            left = [left]
        if not isinstance(right, (list, tuple)):
            right = [right]
        # These two blocks collapse duplicates in a list of keys
        if isinstance(left, list):
            left = set(left)
        if isinstance(right, list):
            right = set(right)
        for val in left:
            if val not in self.forward.keys():
                self.forward[val] = right
            else:
                self.forward[val] = self.forward[val] | right
        for val in right:
            if val not in self.inverse.keys():
                self.inverse[val] = left
            else:
                self.inverse[val] = self.inverse[val] | left

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
        for key in self.forward:
            for val in self.forward[key]:
                lst.append((key, val))
        return str(lst)

    @property
    def id_lst(self):
        """Build up a list of current ids"""
        id_set = set()
        for key in self.forward:
            for val in self.forward[key]:
                id_set.add(val)
        return list(id_set)

    def lookup(self, left, right):
        """Find if a (left, right) pair is already in the list"""
        for val in self.find_left(left):
            if right == val:
                return True
        return False

    @staticmethod
    def side(_id, where):
        """Find if an _id is a key in where"""
        if isinstance(_id, list):
            _id = tuple(_id)
        return _id in where.keys()

    def left(self, key):
        """Determine if the id (left, _) is registered"""
        return self.side(key, self.forward)

    @staticmethod
    def find(where, ids):
        """Find all ids in dictionary where"""
        if not ids:
            return
        if not isinstance(ids, (set, list, tuple)):
            ids = [ids]
        for key in ids:
            if key in where:
                for i in where[key]:
                    yield i

    def find_left(self, ids):
        """Find left values given a list of ids"""
        return self.find(self.forward, ids)

    def right(self, key):
        """Determine if the id (_, right) is registered"""
        return self.side(key, self.inverse)

    def find_right(self, ids):
        """Find the first id founding by searching the (_, right) identifiers"""
        return self.find(self.inverse, ids)

    def set_debug(self, left, label, right):
        """Set debug (left, right) debug values for the structure"""
        # lowercase left and right keys
        if is_str(left):
            left = left.lower()
        if is_str(right):
            right = right.lower()
        # remove duplicates in the debug structure
        # - duplicates in the structure itself are
        # - handled elsewhere
        if isinstance(right, list):
            right = list(set(right))
            # if there is only one element in the list, collapse
            if len(right) == 1:
                right = right.pop()
        # capture the label if it is used
        if label:
            right = (label, right)
        try:
            self.debug[left] = self.debug[left] + [right]
        except KeyError:
            self.debug[left] = [left, right]

    def get_debug(self, key):
        """Get debug information for a given key"""
        # lowercase key if possible
        if is_str(key):
            key = key.lower()
        # return debug information
        if isinstance(key, list):
            return 'type(list)'
        try:
            return self.debug[key]
        except KeyError:
            return 'not-available'

    def import_debug(self, other):
        """
        import debug information the entire IDStruct object
        """
        for key in other.debug:
            self.transfer_debug(key, other)

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
    """DataTransform class.  This class is the public interface for
    the DataTransform module.  Much of the core logic is
    in the subclass."""
    # pylint: disable=R0902
    # Constants
    batch_size = 1000
    DEFAULT_WEIGHT = 1
    default_source = '_id'
    debug = False

    def __init__(self, input_types, output_types, id_priority_list=[],
                 skip_on_failure=False, skip_w_regex=None, idstruct_class=IDStruct,
                 copy_from_doc=False, debug=False):
        # pylint: disable=R0913, W0102
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
        :param id_priority_list: A priority list of identifiers to to
               sort input and output types by.
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
        self.logger, _ = get_logger('datatransform')

        self.debug = debug

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
                if isinstance(input_type, (tuple, list)):
                    if not self._valid_input_type(input_type[0]):
                        raise ValueError("input_type '%s' is not a node in the key_lookup graph" \
                                % repr(input_type[0]))
                    res_input_types.append((input_type[0].lower(), input_type[1]))
                elif isinstance(input_type, str):
                    if not self._valid_input_type(input_type.lower()):
                        raise ValueError("input_type '%s' is not a node in the key_lookup graph" \
                                % repr(input_type))
                    res_input_types.append((input_type, self.default_source))
                else:
                    raise ValueError('Provided input_types is not of the correct type')
        else:
            raise ValueError('Provided input_types is not of the correct type')
        return res_input_types

    def _valid_input_type(self, input_type):
        # pylint: disable=W0613, R0201
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
        # pylint: disable=W0613, R0201
        """In the base class, all input_types and output_types are valid."""
        return True

    def __call__(self, func, debug=None):
        """
        Perform the data transformation on all documents on call.
        :param func: function to apply to
        :param debug: Enable debugging information.
        :type debug: bool
        :param debug: Enable debugging information.  When enabled, debugging information
                      will be retained in the 'dt_debug' field of each document.  This parameter
                      can be either list of original id's to retain debugging information for or
                      a True, which will retain debugging information for all documents.
        :type debug: bool or list(str)
        :return:
        """
        # additional handling for the debug option
        if not debug:
            self.debug = False
        elif debug is True:
            self.debug = True
            self.logger.debug("DataTransform Debug Mode Enabled for all documents.")
        elif isinstance(debug, list):
            self.logger.debug("DataTransform Debug Mode:  {}".format(debug))
            self.debug = debug

        @wraps(func)
        def wrapped_f(*args):
            """This is a wrapped function which will be called by the decorator method."""
            input_docs = func(*args)
            output_doc_cnt = 0
            # split input_docs into chunks of size self.batch_size
            for batchiter in iter_n(input_docs, int(self.batch_size / len(self.input_types))):
                output_docs = self.key_lookup_batch(batchiter)
                for odoc in output_docs:
                    # print debug information if the original id is the in the debug list
                    if 'dt_debug' in odoc:
                        if isinstance(self.debug, list) and \
                                odoc['dt_debug']['orig_id'] in self.debug:
                            self.logger.debug(
                                "DataTransform Debug doc['dt_debug']:  {}"\
                                        .format(odoc['dt_debug']))
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
                self.logger.debug("DataTransform Debug doc['dt_debug']:  {}"\
                        .format(odoc['dt_debug']))
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
        """Property method for getting id_priority_list"""
        return self._id_priority_list

    @id_priority_list.setter
    def id_priority_list(self, value):
        # pylint: disable=W0201
        """Property method for setting id_priority_list and
        sorting input_types and output_types."""
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
        input_types = sorted(input_types,
                             key=lambda e: self._priority_order(id_priority_list, e[0]))
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
        for key in id_list:
            if key not in self.id_priority_list:
                res.append(key)
        return res

    @staticmethod
    def _priority_order(id_priority_list, elem):
        """
        Determine the priority order of an input_type following a id_priority_list.
        This list, first defined in DataTransformMDB is used to reorder the input_types
        so that their order matches the id types listed in id_priority_list.  If an id
        type is not in that list then the input_type will be placed at the end of the list
        in arbitrary order.
        """
        assert isinstance(id_priority_list, list)
        # match id types with id priority
        for index, id_elem in enumerate(id_priority_list):
            if elem == id_elem:
                return index
        # the id type is not in id_priority_list so it will be placed last
        return len(id_priority_list) + 1


class DataTransformEdge(object):
    """
    DataTransformEdge.  This class contains information needed to
    transform one key to another.
    """
    def __init__(self, label=None):
        """
        Initialize the class
        :param label:  A label can be used for debugging purposes.
        """
        self.prepared = False
        self.label = label
        self.init_state()

    def edge_lookup(self, keylookup_obj, id_strct, debug=False):
        # pylint: disable=E1102, R0201, W0613
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
        """initialize the state of pickleable objects"""
        self._state = {
            "logger": None
        }

    @property
    def logger(self):
        """getter for the logger property"""
        if not self._state["logger"]:
            self.prepare()
        return self._state["logger"]

    @logger.setter
    def logger(self, value):
        """setter for the logger variable"""
        self._state["logger"] = value

    def setup_log(self):
        """setup the logger member variable"""
        self.logger, _ = get_logger('datatransform')

    def prepare(self, state={}):
        # pylint: disable=W0102
        """Prepare class state objects (pickleable objects)"""
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
    The RegExEdge allows an identifier to be transformed using a
    regular expression. POSIX regular expressions are supported.
    """

    def __init__(self, from_regex, to_regex, weight=1, label=None):
        """
        :param from_regex: The first parameter of the regular expression substitution.
        :type from_regex: str
        :param to_regex: The second parameter of the regular expression substitution.
        :type to_regex: str
        :param weight: Weights are used to prefer one path over another. The path
                       with the lowest weight is preferred. The default weight is 1.
        :type weight: int
        """
        super(RegExEdge, self).__init__(label)
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
            if isinstance(value, (list, tuple)):
                # assuming we have a list of dict with k as one of the keys
                stype = set([type(e) for e in value])
                if not stype:
                    return None
                assert len(stype) == 1 and stype == {dict},\
                    "Expecting a list of dict, found types: %s" % stype
                value = [e[k] for e in value if e.get(k)]
                # can't go further ?
                return value
            else:
                value = value[k]
    except KeyError:
        return None

    return value
