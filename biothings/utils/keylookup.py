import re

from biothings.utils.common import iter_n
from biothings.utils.loggers import get_logger
from biothings import config as btconfig
from biothings import config_for_app

# Configuration of collections from biothings config file
config_for_app(btconfig)

# Setup logger and logging level
kl_log = get_logger('keylookup', btconfig.LOG_FOLDER)


class KeyLookup(object):
    # Constants
    batch_size = 1000
    DEFAULT_WEIGHT = 1
    default_source = '_id'

    def __init__(self, input_types, output_types, skip_on_failure=False, skip_w_regex=None):
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
                if isinstance(input_type, tuple):
                    if not self._valid_input_type(input_type[0]):
                        raise ValueError("input_type is not a node in the key_lookup graph")
                    res_input_types.append((input_type[0].lower(), input_type[1]))
                elif isinstance(input_type, str):
                    if not self._valid_input_type(input_type.lower()):
                        raise ValueError("input_type is not a node in the key_lookup graph")
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
        Perform the key conversion and all lookups on call.
        :param f:
        :return:
        """
        def wrapped_f(*args):
            input_docs = f(*args)
            kl_log.debug("input: %s" % input_docs)
            # split input_docs into chunks of size self.batch_size
            for batchiter in iter_n(input_docs, int(self.batch_size / len(self.input_types))):
                output_docs = self.key_lookup_batch(batchiter)
                odoc_cnt = 0
                for odoc in output_docs:
                    odoc_cnt += 1
                    kl_log.debug("yield odoc: %s" % odoc)
                    yield odoc
                kl_log.info("wrapped_f Num. output_docs:  {}".format(odoc_cnt))

        return wrapped_f

    def key_lookup_batch(self, batchiter):
        """
        Core method for looking up all keys in batch (iterator)
        :param batchiter:
        :return:
        """
        pass

    def _build_cache(self, batchiter):
        """
        Build an id list and document cache for documents read from the
        batch iterator.
        :param batchiter:  an iterator for a batch of documents.
        :return:
        """
        id_lst = []
        doc_cache = []
        for doc in batchiter:

            # handle skip logic
            if self.skip_w_regex and self.skip_w_regex.match(doc['_id']):
                pass
            else:
                for input_type in self.input_types:
                    val = KeyLookup._nested_lookup(doc, input_type[1])
                    if val:
                        id_lst.append('"{}"'.format(val))

            # always place the document in the cache
            doc_cache.append(doc)
        return list(set(id_lst)), doc_cache

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
