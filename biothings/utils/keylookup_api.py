import biothings_client
import copy
from itertools import islice, chain
import logging
import types

# Setup logger and logging level
logging.basicConfig()
lg = logging.getLogger('keylookup_api')
lg.setLevel(logging.ERROR)


class KeyLookupAPI(object):
    """
    Perform key lookup or key conversion from one key type to another using
    an API endpoint as a data source.

    This class uses biothings apis to conversion from one key type to another.
    Base classes are used with the decorator syntax shown below:

    @KeyLookupMyChemInfo(input_type, output_types)
    def load_document(doc_lst):
        for d in doc_lst:
            yield d
    """
    batch_size = 10000
    lookup_fields = {}

    def __init__(self, input_type, output_types, skip_on_failure=False):
        """
        Initialize the KeyLookupAPI object.
        """
        self._generate_return_fields()

        if input_type in self.lookup_fields.keys():
            self.input_type = input_type
        else:
            raise ValueError('Provided input_type is not configured in lookup_fields')

        if not isinstance(output_types, list):
            raise ValueError('Provided output_types is not a list')
        self.output_types = []
        for output_type in output_types:
            if output_type in self.lookup_fields.keys():
                self.output_types.append(output_type)
        if not self.output_types:
            raise ValueError('output_types provided do not contain any values in lookup_fields')

        if not isinstance(skip_on_failure, bool):
            raise ValueError('skip_on_failure must be a boolean value')
        self.skip_on_failure = skip_on_failure

    def __call__(self, f):
        """
        Perform the key conversion and all lookups on call.
        :param f:
        :return:
        """
        def wrapped_f(*args):
            input_docs = f(*args)
            lg.info("input: %s" % input_docs)

            # split input_docs into chunks of size self.batch_size
            for batchiter in KeyLookupAPI.batch(input_docs, self.batch_size):
                output_docs = self.key_lookup_batch(batchiter)
                for odoc in output_docs:
                    lg.info("yield odoc: %s" % odoc)
                    yield odoc

        return wrapped_f

    @staticmethod
    def batch(iterable, size):
        sourceiter = iter(iterable)
        while True:
            batchiter = islice(sourceiter, size)
            yield chain([next(batchiter)], batchiter)

    def _generate_return_fields(self):
        """
        Generate the return_fields member variable from the lookup_fields dictionary.
        :return:
        """
        self.return_fields = ''
        for k in self.lookup_fields:
            self.return_fields += self.lookup_fields[k] + ','
        lg.info("KeyLookupAPI return_fields:  {}".format(self.return_fields))

    def key_lookup_batch(self, batchiter):
        """
        Look up all keys for ids given in the batch iterator (1 block)
        :param batchiter:  1 lock of records to look up keys for
        :return:
        """

        id_lst, doc_cache = self._build_cache(batchiter)
        qr = self._query_many(id_lst)
        return self.parse_querymany(qr, doc_cache)

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
            if '_id' in doc.keys():
                id_lst.append(doc['_id'])
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
        lg.info("query_many scopes:  {}".format(self.lookup_fields[self.input_type]))
        return self.client.querymany(id_lst,
                                     scopes=self.lookup_fields[self.input_type],
                                     fields=self.return_fields,
                                     as_generator=True,
                                     returnall=True)

    def parse_querymany(self, qr, doc_lst):
        """
        Parse the querymany results from the biothings_client and return results
        in the id fields of the document list
        :param qr: querymany results
        :param doc_lst: list of documents (cached) used for a return structure.
        :return:
        """
        lg.info("QueryMany Structure:  {}".format(qr))
        res_lst = []
        for i, q in enumerate(qr['out']):
            val = self._parse_h(q)
            if val:
                for d in doc_lst:
                    if q['query'] == d['_id']:
                        new_doc = copy.deepcopy(d)
                        new_doc['_id'] = val
                        res_lst.append(new_doc)
        lg.info("parse_querymany doc_lst: {}".format(doc_lst))
        return res_lst

    def _parse_h(self, h):
        """
        Parse a single hit from the API.
        :param h:
        :return: dictionary of keys
        """
        for output_type in self.output_types:
            print(output_type)
            fields = self.lookup_fields[output_type].split('.')
            val = self._parse_field(h, fields)
            print(val)
            if val:
                return val

    @staticmethod
    def _parse_field(h, fields):
        """
        Parse a single field from a query hit.
        Essentially parse a nested dictionary structure using
        a list of keys.
        For example fields = ['key1', 'key2'] would return
        the value of h[key1][key2].  None is returned if the
        lookup fails.
        :param h: query hit
        :param fields: list of fields to traverse
        :return:
        """
        t = h
        for f in fields:
            if f not in t.keys():
                return None
            t = t[f]
        return t


class KeyLookupMyChemInfo(KeyLookupAPI):
    lookup_fields = {
        'inchikey': '_id',
        'chebi': 'chebi.chebi_id',
        'unii': 'unii.unii',
        'drugbank': 'drugbank.drugbank_id',
        'chembl': 'chembl.molecule_chembl_id',
        'pubchem': 'pubchem.cid'
    }

    def __init__(self, input_type, output_types, skip_on_failure=False):
        """
        Initialize the class by seting up the client object.
        """
        super(KeyLookupMyChemInfo, self).__init__(input_type, output_types, skip_on_failure)
        self.client = biothings_client.get_client('drug')


class KeyLookupMyGeneInfo(KeyLookupAPI):
    lookup_fields = {
        'ensembl': 'ensembl.gene',
        'entrezgene': 'entrezgene',
        'symbol': 'symbol',
        'uniprot': 'uniprot.Swiss-Prot'
    }

    def __init__(self, input_type, output_types, skip_on_failure=False):
        """
        Initialize the class by seting up the client object.
        """
        super(KeyLookupMyGeneInfo, self).__init__(input_type, output_types, skip_on_failure)
        self.client = biothings_client.get_client('gene')
