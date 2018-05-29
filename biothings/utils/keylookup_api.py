import biothings_client
import copy
import logging
import pprint

# Setup logger and logging level
logging.basicConfig()
lg = logging.getLogger('keylookup_api')
lg.setLevel(logging.ERROR)


class KeyLookupAPI(object):
    """
    Base KeyLookupAPI class to be inherited by children classes and
    configured for individual APIs.
    """
    lookup_fields = {}

    def __init__(self, input_type, output_types, skip_on_failure=False):
        """
        Initialize the KeyLookupAPI object.

        """
        self._generate_return_fields()
        self.input_type = input_type
        self.output_types = output_types
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
            output_docs = []
            for doc in input_docs:
                lg.info("Decorator arguments:  {}".format(self.input_type))
                lg.info("Input document:  {}".format(doc))
                hits = self.key_lookup(doc['_id'], self.input_type)
                for output_type in self.output_types:
                    # Key(s) were found, create new documents
                    # and add them to the output list
                    hits_found = False
                    for h in hits:
                        if output_type in h.keys():
                            new_doc = copy.deepcopy(doc)
                            new_doc['_id'] = h[output_type]
                            output_docs.append(new_doc)
                            hits_found = True
                    if hits_found:
                        break

                # No keys were found, keep the original (unless the skip_on_failure option is passed)
                if not hits_found and not self.skip_on_failure:
                    output_docs.append(doc)

            for odoc in output_docs:
                lg.info("yield odoc: %s" % odoc)
                yield odoc

        return wrapped_f

    def _generate_return_fields(self):
        """
        Generate the return_fields member variable from the lookup_fields dictionary.

        :return:
        """
        self.return_fields = ''
        for k in self.lookup_fields:
            self.return_fields += self.lookup_fields[k] + ','
        lg.info("KeyLookupAPI return_fields:  {}".format(self.return_fields))

    def key_lookup(self, orig_id, id_type):
        """
        Virtual method of key_lookup to be over-ridden by child classes

        :param orig_id:
        :param id_type:
        :return:
        """
        pass

    def _parse_query_results(self, qr):
        """
        Parse the query results from one API call.  Multiple hits may be returned.
        If no hits are found then None is returned.

        :param qr:
        :return:
        """
        if len(qr['hits']) == 0:
            return None
        r = []
        for h in qr['hits']:
            r.append(self._parse_hit(h))
        return r

    def _parse_hit(self, h):
        """
        Parse a single hit from the API.

        :param h:
        :return: dictionary of keys
        """
        r = {}
        for k in self.lookup_fields.keys():
            fields = self.lookup_fields[k].split('.')
            val = self._parse_field(h, fields)
            if val:
                r[k] = val
        return r

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

    def _print_result(self, r):
        """
        Print results for a single hit (used in testing).
        :param r:
        :return:
        """
        pp = pprint.PrettyPrinter(indent=2)
        pp.pprint(r)


class KeyLookupMyChemInfo(KeyLookupAPI):
    """
    KeyLookupAPI class for the MyChem.Info API service.
    """

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

    def key_lookup(self, orig_id, id_type):
        """
        Lookup a key and return results in a dictionary structure.
        :param orig_id:
        :param id_type:
        :return:
        """
        lg.info('%s:%s' % (id_type, orig_id))
        q = "{}:{}".format(self.lookup_fields[id_type], orig_id)
        qr = self.client.query(q, fields=self.return_fields)
        return self._parse_query_results(qr)


class KeyLookupMyGeneInfo(KeyLookupAPI):
    """
    KeyLookupAPI class for the MyGene.Info API service.
    """

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

    def key_lookup(self, orig_id, id_type):
        """
        Lookup a key and return results in a dictionary structure.
        :param orig_id:
        :param id_type:
        :return:
        """
        lg.info('%s:%s' % (id_type, orig_id))
        q = "{}:{}".format(self.lookup_fields[id_type], orig_id)
        qr = self.client.query(q, fields=self.return_fields)
        return self._parse_query_results(qr)
