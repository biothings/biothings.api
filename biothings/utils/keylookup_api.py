import biothings_client
import copy
from itertools import islice, chain
import logging
from biothings.utils.common import iter_n
from biothings.utils.loggers import get_logger
from biothings import config as btconfig

# Setup logger and logging level
lg = get_logger('keylookup_api', btconfig.LOG_FOLDER)
lg.setLevel(logging.INFO)


class KeyLookupAPI(object):
    """
    Perform key lookup or key conversion from one key type to another using
    an API endpoint as a data source.

    This class uses biothings apis to conversion from one key type to another.
    Base classes are used with the decorator syntax shown below:

    @KeyLookupMyChemInfo(input_types, output_types)
    def load_document(doc_lst):
        for d in doc_lst:
            yield d

    Lookup fields are configured in the 'lookup_fields' object, examples of which
    can be found in 'KeyLookupMyGeneInfo' and 'KeyLookupMyChemInfo'.

    Required Options:
    - input_types
        - 'type'
        - ('type': 'nested.source_field')
        - [('type1': 'nested.source_field1'), ('type2': 'nested.source_field2')]
    - output_types:
        - 'type'
        - ['type1', 'type2']

    Additional Options:
    - skip_on_failure:  Do not include a document where key lookup fails in the results
    """
    batch_size = 10
    default_source = '_id'
    lookup_fields = {}

    def __init__(self, input_types, output_types, skip_on_failure=False):
        """
        Initialize the KeyLookupAPI object.
        """
        self._generate_return_fields()

        self.input_types = []
        if isinstance(input_types, str):
            input_types = [input_types]
        if isinstance(input_types, list):
            for input_type in input_types:
                if isinstance(input_type, tuple):
                    self.input_types.append((input_type[0].lower(), input_type[1]))
                else:
                    if input_type in self.lookup_fields.keys():
                        self.input_types.append((input_type.lower(), self.default_source))
                    else:
                        raise ValueError('Provided input_types is not configured in lookup_fields')
        else:
            raise ValueError('Provided input_types is not of the correct type')

        if not isinstance(output_types, list):
            raise ValueError('Provided output_types is not a list')
        self.output_types = []
        for output_type in output_types:
            if not isinstance(output_type, str):
                raise ValueError('output_types provided is not a string')
            output_type_l = output_type.lower()
            if output_type_l in self.lookup_fields.keys():
                self.output_types.append(output_type_l)
        if not self.output_types:
            raise ValueError('output_types provided do not contain any values in lookup_fields')

        if not isinstance(skip_on_failure, bool):
            raise ValueError('skip_on_failure must be a boolean value')
        self.skip_on_failure = skip_on_failure

        # default value of None for client
        self.client = None

        self.one_to_many_cnt = 0

    def __call__(self, f):
        """
        Perform the key conversion and all lookups on call.
        :param f:
        :return:
        """
        def wrapped_f(*args):
            input_docs = f(*args)
            lg.debug("input: %s" % input_docs)
            # split input_docs into chunks of size self.batch_size
            for batchiter in iter_n(input_docs, int(self.batch_size / len(self.input_types))):
                output_docs = self.key_lookup_batch(batchiter)
                odoc_cnt = 0
                for odoc in output_docs:
                    odoc_cnt += 1
                    lg.debug("yield odoc: %s" % odoc)
                    yield odoc
                lg.info("wrapped_f Num. output_docs:  {}".format(odoc_cnt))

        return wrapped_f

    def _generate_return_fields(self):
        """
        Generate the return_fields member variable from the lookup_fields dictionary.
        :return:
        """
        self.return_fields = ''
        for k in self.lookup_fields:
            for field in self._get_lookup_field(k):
                self.return_fields += field + ','
        lg.debug("KeyLookupAPI return_fields:  {}".format(self.return_fields))

    def key_lookup_batch(self, batchiter):
        """
        Look up all keys for ids given in the batch iterator (1 block)
        :param batchiter:  1 lock of records to look up keys for
        :return:
        """

        id_lst, doc_cache = self._build_cache(batchiter)
        lg.info("key_lookup_batch num. id_lst items:  {}".format(len(id_lst)))
        qr = self._query_many(id_lst)
        qm_struct = self._parse_querymany(qr)
        return self._replace_keys(qm_struct, doc_cache)

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
            for input_type in self.input_types:
                val = self._nested_lookup(doc, input_type[1])
                if val:
                    id_lst.append('"{}"'.format(val))
            # always place the document in the cache
            doc_cache.append(doc)
        return list(set(id_lst)), doc_cache

    def _query_many(self, id_lst):
        """
        Call the biothings_client querymany function with a list of identifiers
        and output fields that will be returned.
        :param id_lst: list of identifiers to query
        :return:
        """
        # Query MyGene.info
        # lg.debug("query_many scopes:  {}".format(self.lookup_fields[self.input_type]))
        scopes = []
        for input_type in self.input_types:
            for field in self._get_lookup_field(input_type[0]):
                scopes.append(field)
        client = self._get_client()

        return client.querymany(id_lst,
                                scopes=scopes,
                                fields=self.return_fields,
                                as_generator=True,
                                returnall=True,
                                size=self.batch_size)

    def _parse_querymany(self, qr):
        """
        Parse the querymany results from the biothings_client into a structure
        that will later be used for document key replacement.
        :param qr: querymany results
        :return:
        """
        lg.debug("QueryMany Structure:  {}".format(qr))
        qm_struct = {}
        for q in qr['out']:
            query = q['query']
            val = self._parse_h(q)
            if val:
                if query not in qm_struct.keys():
                    qm_struct[query] = [val]
                else:
                    self.one_to_many_cnt += 1
                    qm_struct[query] = qm_struct[query] + [val]
        lg.debug("parse_querymany num qm_struct keys: {}".format(len(qm_struct.keys())))
        lg.info("parse_querymany running one_to_many_cnt: {}".format(self.one_to_many_cnt))
        lg.debug("parse_querymany qm_struct: {}".format(qm_struct.keys()))
        return qm_struct

    def _parse_h(self, h):
        """
        Parse a single hit from the API.
        :param h:
        :return: dictionary of keys
        """
        for output_type in self.output_types:
            for doc_field in self._get_lookup_field(output_type):
                val = self._nested_lookup(h, doc_field)
                if val:
                    return val

    def _replace_keys(self, qm_struct, doc_cache):
        """
        Build a new list of documents to return that have their keys
        replaced by answers specified in the qm_structure which
        was built earlier.
        :param qm_struct: structure of keys from _parse_querymany
        :param doc_cache: cache of documents that will have keys replaced.
        :return:
        """
        # Replace the keys and build up a new result list
        res_lst = []
        for doc in doc_cache:
            new_doc = None
            for input_type in self.input_types:
                # doc[input_type[1]] must be typed to a string because qm_struct.keys are always strings
                if self._nested_lookup(doc, input_type[1]) in qm_struct.keys():
                    for key in qm_struct[self._nested_lookup(doc, input_type[1])]:
                        new_doc = copy.deepcopy(doc)
                        new_doc['_id'] = key
                        res_lst.append(new_doc)
            if not new_doc and not self.skip_on_failure:
                res_lst.append(doc)

        lg.info("_replace_keys:  Num of documents yielded:  {}".format(len(res_lst)))
        # Yield the results
        for r in res_lst:
            yield r

    def _get_lookup_field(self, field):
        """
        Getter for lookup fields which may be either a string or a list of string fields.
        :param field: the name of the field to lookup
        :return:
        """
        if field not in self.lookup_fields.keys():
            raise KeyError('provided field {} is not in self.lookup_fields'.format(field))
        if isinstance(self.lookup_fields[field], str):
            return [self.lookup_fields[field]]
        else:
            return self.lookup_fields[field]

    @staticmethod
    def _nested_lookup(doc, field):
        """
        Helper function for nested key lookup of a document.
        For example field = 'pharmgkb.xref.drugbank' would return
        the value of doc['pharmgkb']['xref']['drugbank'].
        None is returned if the lookup fails.
        :param doc:
        :param field:
        :return:
        """
        fields = field.split('.')
        t = doc
        for f in fields:
            if f not in t.keys():
                return None
            t = t[f]
        return str(t)


class KeyLookupMyChemInfo(KeyLookupAPI):
    lookup_fields = {
        'chebi': 'chebi.chebi_id',
        'unii': 'unii.unii',
        'drugbank': 'drugbank.drugbank_id',
        'chembl': 'chembl.molecule_chembl_id',
        'pubchem': 'pubchem.cid',
        'inchi': [
            'drugbank.inchi',
            'chembl.inchi',
            'pubchem.inchi'
            ],
        'inchikey': [
            'drugbank.inchi_key',
            'chembl.inchi_key',
            'pubchem.inchi_key'
        ]
    }

    def __init__(self, input_types,
                 output_types=['inchikey', 'drugbank', 'chembl', 'pubchem'],
                 skip_on_failure=False):
        """
        Initialize the class by seting up the client object.
        """
        super(KeyLookupMyChemInfo, self).__init__(input_types, output_types, skip_on_failure)

    def _get_client(self):
        """
        Get Client - return a client appropriate for KeyLookup

        This method must be defined in the child class.  It is an artifact
        of multithreading.
        :return:
        """
        if not self.client:
            self.client = biothings_client.get_client('drug')
        return self.client


class KeyLookupMyGeneInfo(KeyLookupAPI):
    lookup_fields = {
        'ensembl': 'ensembl.gene',
        'entrezgene': 'entrezgene',
        'symbol': 'symbol',
        'uniprot': 'uniprot.Swiss-Prot'
    }

    def __init__(self, input_types,
                 output_types=['entrezgene'],
                 skip_on_failure=False):
        """
        Initialize the class by seting up the client object.
        """
        super(KeyLookupMyGeneInfo, self).__init__(input_types, output_types, skip_on_failure)

    def _get_client(self):
        """
        Get Client - return a client appropriate for KeyLookup

        This method must be defined in the child class.  It is an artifact
        of multithreading.
        :return:
        """
        if not self.client:
            self.client = biothings_client.get_client('gene')
        return self.client
