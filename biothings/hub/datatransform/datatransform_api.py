import biothings_client
import copy
from itertools import islice, chain
import logging
import re
from biothings.hub.datatransform.datatransform import DataTransform
from biothings.utils.loggers import get_logger
from biothings import config as btconfig

# Setup logger and logging level
lg = get_logger('keylookup_api', btconfig.LOG_FOLDER)
lg.setLevel(logging.INFO)


class DataTransformAPI(DataTransform):
    """
    Perform key lookup or key conversion from one key type to another using
    an API endpoint as a data source.

    This class uses biothings apis to conversion from one key type to another.
    Base classes are used with the decorator syntax shown below:

    @IDLookupMyChemInfo(input_types, output_types)
    def load_document(doc_lst):
        for d in doc_lst:
            yield d

    Lookup fields are configured in the 'lookup_fields' object, examples of which
    can be found in 'IDLookupMyGeneInfo' and 'IDLookupMyChemInfo'.

    Required Options:
    - input_types
        - 'type'
        - ('type', 'nested_source_field')
        - [('type1', 'nested.source_field1'), ('type2', 'nested.source_field2'), ...]
    - output_types:
        - 'type'
        - ['type1', 'type2']

    Additional Options:
    - skip_on_failure:  Do not include a document where key lookup fails in the results
    - skip_w_regex:  skip key lookup if the provided regex matches
    """
    batch_size = 10
    default_source = '_id'
    lookup_fields = {}

    def __init__(self, input_types, output_types, skip_on_failure=False, skip_w_regex=None):
        """
        Initialize the IDLookupAPI object.
        """
        self._generate_return_fields()
        super().__init__(input_types, output_types, skip_on_failure, skip_w_regex)

        # default value of None for client
        self.client = None

        # Keep track of one_to_many relationships
        self.one_to_many_cnt = 0

    def _valid_input_type(self, input_type):
        """
        Check if the input_type is valid
        :param input_type:
        :return:
        """
        if not isinstance(input_type, str):
            return False
        return input_type.lower() in self.lookup_fields.keys()

    def _valid_output_type(self, output_type):
        """
        Check if the output_type is valid
        :param output_type:
        :return:
        """
        if not isinstance(output_type, str):
            return False
        return output_type.lower() in self.lookup_fields.keys()

    def _generate_return_fields(self):
        """
        Generate the return_fields member variable from the lookup_fields dictionary.
        :return:
        """
        self.return_fields = ''
        for k in self.lookup_fields:
            for field in self._get_lookup_field(k):
                self.return_fields += field + ','
        lg.debug("IDLookupAPI return_fields:  {}".format(self.return_fields))

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

            # handle skip logic
            if self.skip_w_regex and self.skip_w_regex.match(doc['_id']):
                pass
            else:
                for input_type in self.input_types:
                    val = DataTransformAPI._nested_lookup(doc, input_type[1])
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
                val = DataTransformAPI._nested_lookup(h, doc_field)
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
                if DataTransformAPI._nested_lookup(doc, input_type[1]) in qm_struct.keys():
                    for key in qm_struct[DataTransformAPI._nested_lookup(doc, input_type[1])]:
                        new_doc = copy.deepcopy(doc)
                        new_doc['_id'] = key
                        res_lst.append(new_doc)
                # Break out if an input type was used.
                if new_doc:
                    break
            if not new_doc and ((self.skip_w_regex and self.skip_w_regex.match(doc['_id'])) or not self.skip_on_failure):
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


class DataTransformMyChemInfo(DataTransformAPI):
    lookup_fields = {
        'unii': 'unii.unii',
        'rxnorm': [
            'unii.rxcui'
        ],        
        'drugbank': 'drugbank.drugbank_id',
        'chebi': 'chebi.chebi_id',
        'chembl': 'chembl.molecule_chembl_id',
        'pubchem': 'pubchem.cid',
        'drugname': [
            'drugbank.name',
            'unii.preferred_term',
            'chebi.chebi_name',
            'chembl.pref_name'
        ],
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
    # The order of output_types decides the priority of the key types we used to get _id value
    output_types=['inchikey', 'unii', 'rxnorm', 'drugbank', 'chebi', 'chembl', 'pubchem', 'drugname'],
    
    def __init__(self, input_types,
                 output_types=None,
                 skip_on_failure=False,
                 skip_w_regex=None):
        """
        Initialize the class by seting up the client object.
        """
        _output_types = output_types or self.output_types
        super(DataTransformMyChemInfo, self).__init__(input_types, _output_types, skip_on_failure, skip_w_regex)

    def _get_client(self):
        """
        Get Client - return a client appropriate for IDLookup

        This method must be defined in the child class.  It is an artifact
        of multithreading.
        :return:
        """
        if not self.client:
            self.client = biothings_client.get_client('drug')
        return self.client


class DataTransformMyGeneInfo(DataTransformAPI):
    lookup_fields = {
        'ensembl': 'ensembl.gene',
        'entrezgene': 'entrezgene',
        'symbol': 'symbol',
        'uniprot': 'uniprot.Swiss-Prot'
    }

    def __init__(self, input_types,
                 output_types=['entrezgene'],
                 skip_on_failure=False,
                 skip_w_regex=None):
        """
        Initialize the class by seting up the client object.
        """
        super(DataTransformMyGeneInfo, self).__init__(input_types, output_types, skip_on_failure, skip_w_regex)

    def _get_client(self):
        """
        Get Client - return a client appropriate for IDLookup

        This method must be defined in the child class.  It is an artifact
        of multithreading.
        :return:
        """
        if not self.client:
            self.client = biothings_client.get_client('gene')
        return self.client
