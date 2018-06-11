import unittest
from biothings.utils.keylookup_api import KeyLookupMyChemInfo
from biothings.utils.keylookup_api import KeyLookupMyGeneInfo


class TestKeyLookupAPI(unittest.TestCase):

    def test_mycheminfo(self):
        """
        Test of KeyLookupMyChemInfo
        :return:
        """

        def _MyChemInfoSingleDoc(input_type, output_types, question, answer):
            @KeyLookupMyChemInfo(input_type, output_types)
            def load_document(doc_lst):
                for d in doc_lst:
                    yield d
            doc_lst = [{'_id': question}]
            res_lst = load_document(doc_lst)
            res = next(res_lst)
            self.assertEqual(res['_id'], answer)

        # Examples - paracetamol (acetaminophen)
        _MyChemInfoSingleDoc('inchikey', ['inchikey'], 'RZVAJINKPMORJF-UHFFFAOYSA-N', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('chebi', ['inchikey'], 'CHEBI:46195', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('unii', ['inchikey'], '362O9ITL9D', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('drugbank', ['inchikey'], 'DB00316', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('chembl', ['inchikey'], 'CHEMBL112', 'RZVAJINKPMORJF-UHFFFAOYSA-N')
        _MyChemInfoSingleDoc('pubchem', ['inchikey'], 'CID1983', 'RZVAJINKPMORJF-UHFFFAOYSA-N')

        # Other examples
        _MyChemInfoSingleDoc('chebi', ['inchikey'], 'CHEBI:63599', 'GIUYCYHIANZCFB-FJFJXFQQSA-N')
        _MyChemInfoSingleDoc('inchikey', ['pubchem'], 'ATBDZSAENDYQDW-UHFFFAOYSA-N', 'CID4080429')
        _MyChemInfoSingleDoc('inchikey', ['unii'], 'ATBDZSAENDYQDW-UHFFFAOYSA-N', '18MXK3D6DB')

    def test_mygeneinfo(self):
        """
        Test of KeyLookupMyGeneInfo
        :return:
        """

        def _MyGeneInfoSingleDoc(input_type, output_types, question, answer):
            @KeyLookupMyGeneInfo(input_type, output_types)
            def load_document(doc_lst):
                for d in doc_lst:
                    yield d
            doc_lst = [{'_id': question}]
            res_lst = load_document(doc_lst)
            res = next(res_lst)
            self.assertEqual(res['_id'], answer)

        _MyGeneInfoSingleDoc('ensembl', ['symbol'], 'ENSG00000123374', 'CDK2')
        _MyGeneInfoSingleDoc('entrezgene', ['symbol'], '1017', 'CDK2')

        # TODO:  uniprot.Swiss-Prot doesn't with query_many
        # _MyGeneInfoSingleDoc('uniprot', ['symbol'], 'P24941', 'CDK2')

    def test_mygene_one2many(self):
        """
        Test the one-to-many relationship for key conversion

        :return:
        """

        doc_lst = [{'_id': 'CDK2'}]
        @KeyLookupMyGeneInfo('symbol', ['ensembl'], skip_on_failure=True)
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = sum(1 for _ in res_lst)
        # assert that at least 5 elements are returned
        self.assertGreater(res_cnt, 5)

    def test_batch_queries(self):
        """
        Test converting a long-ish list of entrezgenes to symbols.  The
        purpose of this test is to exercise the query_many behavior of
        the class which will break the list into batches.
        :return:
        """

        # Build up document list
        input = [
            51300,
            54958,
            57829,
            100526772,
            6836,
            84910,
            644672,
            643382,
            348013,
            2707400000 # broken on purpose
        ]
        doc_lst = []
        for e in input:
            doc_lst.append({'_id': e})

        answers = [
            'TIMMDC1',
            'TMEM160',
            'ZP4',
            'TMEM110-MUSTN1',
            'SURF4',
            'TMEM87B',
            'CLDN25',
            'TMEM253',
            'TMEM255B',
            # The last key was not converted
            2707400000
        ]

        # Test a list being passed with 10 documents
        @KeyLookupMyGeneInfo('entrezgene', ['symbol'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = 0
        for res in res_lst:
            res_cnt += 1
            self.assertTrue(res['_id'] in answers)
        self.assertEqual(res_cnt, 9)

    def test_strangecases(self):

        doc_lst = [{'_id': 'CDK2'}]

        # with self.assertRaises(ValueError):
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo('entrezgene', ['undefined'])
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

        # Non-string input-type
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo(None, ['undefined'])
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

        # Non-list output-type
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo('entrezgene', 'symbol')
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

        # output-type with a non-string
        with self.assertRaises(ValueError):
            @KeyLookupMyGeneInfo('entrezgene', [None])
            def load_document(data_folder):
                for d in doc_lst:
                    yield d

    def test_invalid_record(self):
        """
        Test an invalid record in the document set.
        :return:
        """

        doc_lst = [{'_id': 'CID1983'}, {'_id': None}, {'id': 'CID1983'}]
        @KeyLookupMyChemInfo('pubchem', ['inchikey'], skip_on_failure=True)
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = sum(1 for _ in res_lst)
        self.assertEqual(res_cnt, 1)
