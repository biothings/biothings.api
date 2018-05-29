import unittest
from biothings.utils.keylookup_api import KeyLookupMyChemInfo
from biothings.utils.keylookup_api import KeyLookupMyGeneInfo


class TestKeyLookupSimple(unittest.TestCase):

    def test_mycheminfo(self):
        """
        Test of the internal key_lookup method for KeyLookupMyChemInfo
        :return:
        """

        klmychem = KeyLookupMyChemInfo('inchikey', ['inchikey'])

        # Examples - paracetamol (acetaminophen)
        r = klmychem.key_lookup('RZVAJINKPMORJF-UHFFFAOYSA-N', 'inchikey')
        self.assertEqual(r[0]['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        r = klmychem.key_lookup('CHEBI\\:46195', 'chebi')
        self.assertEqual(r[0]['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        r = klmychem.key_lookup('362O9ITL9D', 'unii')
        self.assertEqual(r[0]['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        r = klmychem.key_lookup('DB00316', 'drugbank')
        self.assertEqual(r[0]['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        r = klmychem.key_lookup('CHEMBL112', 'chembl')
        self.assertEqual(r[0]['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        r = klmychem.key_lookup('CID1983', 'pubchem')
        self.assertEqual(r[0]['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')

        # # Other examples
        r = klmychem.key_lookup('CHEBI\\:63599', 'chebi')
        self.assertEqual(r[0]['inchikey'], 'GIUYCYHIANZCFB-FJFJXFQQSA-N')

        r = klmychem.key_lookup('ATBDZSAENDYQDW-UHFFFAOYSA-N', 'inchikey')
        self.assertEqual(r[0]['pubchem'], 'CID4080429')
        self.assertEqual(r[0]['unii'], '18MXK3D6DB')

    def test_mychem_decorator(self):
        """
        Test of the decorator interface for KeyLookupMyChemInfo
        :return:
        """

        test_doc = {
            '_id': 'RZVAJINKPMORJF-UHFFFAOYSA-N'
        }

        @KeyLookupMyChemInfo('inchikey', ['undefined', 'pubchem'], skip_on_failure=True)
        def load_document(data_folder):
            doc_lst = [test_doc]
            for d in doc_lst:
                yield d

        # Test a list being passed with 3 documents, 2 are returned, 1 is skipped
        res_lst = load_document('data/folder/')
        res1 = next(res_lst)
        self.assertEqual(res1['_id'], 'CID1983')

        # Verify that the generator is out of documents
        with self.assertRaises(StopIteration):
            next(res_lst)

    def test_mygeneinfo(self):
        """
        Test of the internal key_lookup method for KeyLookupMyGeneInfo
        :return:
        """

        klmygene = KeyLookupMyGeneInfo('symbol', ['symbol'])
        # "CDK2", "NM_052827", "204639_at", "chr1:151,073,054-151,383,976", "hg19.chr1:151073054-151383976".

        r = klmygene.key_lookup('CDK2', 'symbol')
        self.assertEqual(r[0]['symbol'], 'CDK2')
        r = klmygene.key_lookup('ENSG00000123374', 'ensembl')
        self.assertEqual(r[0]['symbol'], 'CDK2')
        r = klmygene.key_lookup('1017', 'entrezgene')
        self.assertEqual(r[0]['symbol'], 'CDK2')
        r = klmygene.key_lookup('P24941', 'uniprot')
        self.assertEqual(r[0]['symbol'], 'CDK2')

    def test_mygene_decorator(self):
        """
        Test of the decorator interface for KeyLookupMyGeneInfo
        :return:
        """

        test_doc = {
            '_id': 'ENSG00000123374'
        }

        @KeyLookupMyGeneInfo('ensembl', ['undefined', 'symbol'], skip_on_failure=True)
        def load_document(data_folder):
            doc_lst = [test_doc]
            for d in doc_lst:
                yield d

        # Test a list being passed with 3 documents, 2 are returned, 1 is skipped
        res_lst = load_document('data/folder/')
        res1 = next(res_lst)
        self.assertEqual(res1['_id'], 'CDK2')

        # Verify that the generator is out of documents
        with self.assertRaises(StopIteration):
            next(res_lst)

    def test_mygene_one2many(self):
        """
        Test the one-to-many relationship for key conversion

        :return:
        """

        test_doc = {
            '_id': 'CDK2'
        }

        @KeyLookupMyGeneInfo('symbol', ['undefined', 'ensembl'], skip_on_failure=True)
        def load_document(data_folder):
            doc_lst = [test_doc]
            for d in doc_lst:
                yield d

        # Test a list being passed with 3 documents, 2 are returned, 1 is skipped
        res_lst = load_document('data/folder/')
        res_cnt = sum(1 for _ in res_lst)
        # assert that at least 5 elements are returned
        self.assertGreater(res_cnt, 5)
