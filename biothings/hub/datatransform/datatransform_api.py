import biothings_client
import copy
from itertools import islice, chain
import logging
import re
from biothings.hub.datatransform.datatransform import DataTransform, DataTransformEdge, IDStruct, nested_lookup
from biothings.utils.loggers import get_logger
from biothings import config as btconfig


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

        self.logger,_ = get_logger('keylookup_api')

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
        self.logger.debug("IDLookupAPI return_fields:  {}".format(self.return_fields))

    def key_lookup_batch(self, batchiter):
        """
        Look up all keys for ids given in the batch iterator (1 block)
        :param batchiter:  1 lock of records to look up keys for
        :return:
        """

        id_lst, doc_cache = self._build_cache(batchiter)
        self.logger.info("key_lookup_batch num. id_lst items:  {}".format(len(id_lst)))
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
        # self.logger.debug("query_many scopes:  {}".format(self.lookup_fields[self.input_type]))
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
        # self.logger.debug("QueryMany Structure:  {}".format(qr))
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
        # self.logger.debug("parse_querymany num qm_struct keys: {}".format(len(qm_struct.keys())))
        # self.logger.info("parse_querymany running one_to_many_cnt: {}".format(self.one_to_many_cnt))
        # self.logger.debug("parse_querymany qm_struct: {}".format(qm_struct.keys()))
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

        self.logger.info("_replace_keys:  Num of documents yielded:  {}".format(len(res_lst)))
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


class BiothingsAPIEdge(DataTransformEdge):
    """
    APIEdge - IDLookupEdge object for API calls
    """
    # define in subclass
    client_name = None
    url = None

    def __init__(self, lookup, fields, weight=1, label=None):
        super().__init__(label)
        self.init_state()
        if isinstance(lookup, str):
            self.scopes = [lookup]
        elif isinstance(lookup, list):
            self.scopes = lookup
        else:
            raise TypeError("scopes argument must be str or list")
        if isinstance(fields, str):
            self.fields = [fields]
        elif isinstance(fields, list):
            self.fields = fields
        else:
            raise TypeError("fields argument must be str or list")
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
        """
        Load the biothings_client for the class
        :return:
        """
        if not self.client_name:
            raise NotImplementedError("Define client_name in subclass")
        if self.url:
            self._state["client"] = biothings_client.get_client(self.client_name, url=self.url)
        else:
            self._state["client"] = biothings_client.get_client(self.client_name)
        self.logger.info("Registering biothings_client {}".format(self.client_name))

    def edge_lookup(self, keylookup_obj, id_strct, debug=False):
        """
        Follow an edge given a key.

        This method uses the data in the edge_object
        to find one key to another key using an api.
        :param edge:
        :param key:
        :return:
        """
        # If no keys were passed return an empty idstruct_class
        if not len(id_strct):
            return keylookup_obj.idstruct_class()
        # query the api
        qr = self._query_many(keylookup_obj, id_strct)
        new_id_strct = self._parse_querymany(keylookup_obj, qr, id_strct, self.fields, debug)
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
        id_lst = []
        for id in id_strct.id_lst:
            id_lst.append('"{}"'.format(id))
        return self.client.querymany(id_lst,
                                     scopes=self.scopes,
                                     fields=self.fields,
                                     as_generator=True,
                                     returnall=True,
                                     size=keylookup_obj.batch_size)

    def _parse_querymany(self, keylookup_obj, qr, id_strct, fields, debug):
        """
        Parse the querymany results from the biothings_client into a structure
        that will later be used for document key replacement.
        :param qr: querymany results
        :return:
        """
        # self.logger.debug("QueryMany Structure:  {}".format(qr))
        qm_struct = IDStruct()

        # Keep the old debug information
        if debug:
            qm_struct.import_debug(id_strct)

        for q in qr['out']:
            query = q['query']
            for field in fields:
                val = nested_lookup(q, field)
                if val:
                    for (orig_id, curr_id) in id_strct:
                        # query is always a string, so this check requires conversion
                        if query == str(curr_id):
                            qm_struct.add(orig_id, val)
                            # save debug information in the option is set
                            if debug:
                                qm_struct.set_debug(orig_id, self.label, val)
        return qm_struct


class MyChemInfoEdge(BiothingsAPIEdge):
    """
    The MyChemInfoEdge uses the MyChem.info API to convert identifiers.
    """
    client_name = "drug"

    def __init__(self, lookup, field, weight=1, label=None):
        """
        :param lookup: The field in the API to search with the input identifier.
        :type lookup: str
        :param field: The field in the API to convert to.
        :type field: str
        :param weight: Weights are used to prefer one path over another. The path with the lowest weight is preferred. The default weight is 1.
        :type weight: int
        """
        super().__init__(lookup, field, weight, label)


class MyGeneInfoEdge(BiothingsAPIEdge):
    """
    The MyGeneInfoEdge uses the MyGene.info API to convert identifiers.
    """
    client_name = "gene"

    def __init__(self, lookup, field, weight=1, label=None):
        """
        :param lookup: The field in the API to search with the input identifier.
        :type lookup: str
        :param field: The field in the API to convert to.
        :type field: str
        :param weight: Weights are used to prefer one path over another. The path with the lowest weight is preferred. The default weight is 1.
        :type weight: int
        """
        super().__init__(lookup, field, weight, label)


####################

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
