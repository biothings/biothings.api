import config, biothings
biothings.config_for_app(config)

from biothings.hub.datatransform import DataTransform
from biothings.hub.datatransform import DataTransformMDB as KeyLookup
from biothings.hub.datatransform import CIIDStruct
from biothings.tests.keylookup_graphs import graph_simple, \
    graph_weights, graph_one2many, graph_invalid, graph_mix, \
    graph_mychem, graph_regex, graph_pubchem, graph_ci
import unittest
import biothings.utils.mongo as mongo


class TestDataTransform(unittest.TestCase):

    def setUp(self):
        """
        Setup the mongodb structure for the tests
        :return:
        """

        # Collections for the first test
        self.db = mongo.get_src_db()
        self.db.create_collection('a')
        self.db.create_collection('b')
        self.db.create_collection('c')
        self.db.create_collection('d')
        self.db.create_collection('e')

        self.db['b'].insert({'b_id': 'b:1234', 'a_id': 'a:1234'})
        self.db['c'].insert({'c_id': 'c:1234', 'b_id': 'b:1234', 'e_id': 'e:1234'})
        self.db['d'].insert({'d_id': 'd:1234', 'c_id': 'c:1234'})
        self.db['e'].insert({'e_id': 'e:1234', 'd_id': 'd:1234'})

        # Collections for the second test (one2many)
        self.db.create_collection('aa')
        self.db.create_collection('bb')
        self.db.create_collection('cc')

        self.db['bb'].insert({'b_id': 'b:1234', 'a_id': 'a:1234'})
        self.db['bb'].insert({'b_id': 'b:5678', 'a_id': 'a:1234'})
        self.db['cc'].insert({'c_id': 'c:1234', 'b_id': 'b:1234'})
        self.db['cc'].insert({'c_id': 'c:01', 'b_id': 'b:5678'})
        self.db['cc'].insert({'c_id': 'c:02', 'b_id': 'b:5678'})

        # Collections for the path weight test
        self.db = mongo.get_src_db()
        self.db.create_collection('aaa')
        self.db.create_collection('bbb')
        self.db.create_collection('ccc')
        self.db.create_collection('ddd')
        self.db.create_collection('eee')

        self.db['bbb'].insert({'b_id': 'b:1234', 'a_id': 'a:1234', 'e_id': 'e:5678'})
        self.db['ccc'].insert({'c_id': 'c:1234', 'b_id': 'b:1234'})
        self.db['ddd'].insert({'d_id': 'd:1234', 'c_id': 'c:1234'})
        self.db['eee'].insert({'e_id': 'e:1234', 'd_id': 'd:1234'})

        # Collections for the mix mongodb and api test
        self.db = mongo.get_src_db()
        self.db.create_collection('mix1')
        self.db.create_collection('mix3')

        self.db['mix1'].insert({'ensembl': 'ENSG00000123374', 'start_id': 'start1'})
        self.db['mix3'].insert({'end_id': 'end1', 'entrez': '1017'})

        # Collections for lookup failure
        self.db['b'].insert({'b_id': 'b:f1', 'a_id': 'a:f1'})
        self.db['c'].insert({'c_id': 'c:f1', 'b_id': 'b:f1'})
        self.db['d'].insert({'d_id': 'd:fail1', 'c_id': 'c:f1'})
        self.db['e'].insert({'e_id': 'e:f1', 'd_id': 'd:f1'})

    def tearDown(self):
        """
        Reset the mongodb structure after the tests
        :return:
        """
        # Collections for the first test
        self.db.drop_collection('a')
        self.db.drop_collection('b')
        self.db.drop_collection('c')
        self.db.drop_collection('d')
        self.db.drop_collection('e')

        # Collections for the second test (one2many)
        self.db.drop_collection('aa')
        self.db.drop_collection('bb')
        self.db.drop_collection('cc')

        # Collections for the weighted test
        self.db.drop_collection('aaa')
        self.db.drop_collection('bbb')
        self.db.drop_collection('ccc')
        self.db.drop_collection('ddd')
        self.db.drop_collection('eee')

        # Collections for the mix mongodb and api test
        self.db.drop_collection('mix1')
        self.db.drop_collection('mix3')

    def test_simple(self):
        """
        Simple test for key lookup - artificial document.

        The network contains a cycle that is avoided by the networkx algorithm.
        :return:
        """

        @KeyLookup(graph_simple, 'a', ['d', 'e'])
        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        # Initial Test Case
        doc_lst = [{'_id': 'a:1234'}]
        res_lst = load_document(doc_lst)

        res = next(res_lst)
        self.assertEqual(res['_id'], 'd:1234')

        # Verify that the generator is out of documents
        with self.assertRaises(StopIteration):
            next(res_lst)

    def test_one2many(self):
        """
        test for one to many key lookup - artificial document.
        :return:
        """
        doc_lst = [{'input_key': 'a:1234'}]

        @KeyLookup(graph_one2many, [('aa', 'input_key')], ['cc'])
        def load_document():
            for d in doc_lst:
                yield d

        # Initial Test Case
        res_lst = [d for d in load_document()]

        # Check for expected keys
        # There are 2 branches along the document path
        answer_lst = []
        answer_lst.append(res_lst[0]['_id'])
        answer_lst.append(res_lst[1]['_id'])
        answer_lst.append(res_lst[2]['_id'])

        self.assertTrue('c:1234' in answer_lst)
        self.assertTrue('c:01' in answer_lst)
        self.assertTrue('c:02' in answer_lst)

    def test_input_types(self):
        """
        test for input_types - artificial documents.
        :return:
        """
        # Initial Test Case
        doc_lst = [
            {'a': 'a:1234'},
            {'b': 'b:1234'}
        ]
        @KeyLookup(graph_simple, [('a', 'a'), ('b', 'b')], ['d', 'e'])
        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        res_lst = load_document(doc_lst)

        for res in res_lst:
            print(res)
            # Check for expected keys
            self.assertEqual(res['_id'], 'd:1234')

    def test_weights(self):
        """
        Simple test for key lookup - artificial document.

        The network contains a shortcut path with a high weight
        that should be avoided.
        :return:
        """
        @KeyLookup(graph_weights, 'aaa', ['eee'])
        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        # Initial Test Case
        doc_lst = [{'_id': 'a:1234'}]
        res_lst = load_document(doc_lst)

        for res in res_lst:
            # Check for expected key
            self.assertEqual(res['_id'], 'e:1234')

    def test_interface(self):
        """
        Simple test for key lookup - artificial document.

        This test is intended to test multiple douments being passed.
        :return:
        """
        @KeyLookup(graph_simple, 'a', ['d', 'e'])
        def load_document(data_folder):
            doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:1234'}, {'_id': 'a:1234'}]
            for d in doc_lst:
                yield d

        # Test a list being passed with three documents
        res_lst = load_document('data/folder/')
        res1 = next(res_lst)
        res2 = next(res_lst)
        res3 = next(res_lst)
        self.assertEqual(res1['_id'], 'd:1234')
        self.assertEqual(res2['_id'], 'd:1234')
        self.assertEqual(res3['_id'], 'd:1234')

    def test_skip_on_failure(self):
        """
        Simple test for key lookup skip_on_failure.

        This test tests the skip_on_failure option which skips documents
        where lookup was unsuccessful.

        :return:
        """
        @KeyLookup(graph_simple, 'a', ['d', 'e'], skip_on_failure=True)
        def load_document(data_folder):
            doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
            for d in doc_lst:
                yield d

        # Test a list being passed with 3 documents, 2 are returned, 1 is skipped
        res_lst = load_document('data/folder/')
        res1 = next(res_lst)
        res2 = next(res_lst)
        self.assertEqual(res1['_id'], 'd:1234')
        self.assertEqual(res2['_id'], 'd:1234')

        # Verify that the generator is out of documents
        with self.assertRaises(StopIteration):
            next(res_lst)

    def test_strangecases(self):
        """
        Test invalid input that should generate exceptions.
        :return:
        """
        # invalid input-type
        with self.assertRaises(ValueError):
            @KeyLookup(graph_simple, 'a-invalid', ['d', 'e'], skip_on_failure=True)
            def load_document(data_folder):
                doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
                for d in doc_lst:
                    yield d

        # Invalid output-type
        with self.assertRaises(ValueError):
            @KeyLookup(graph_simple, 'a', ['d-invalid', 'e'], skip_on_failure=True)
            def load_document(data_folder):
                doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
                for d in doc_lst:
                    yield d

        # Invalid graph
        with self.assertRaises(ValueError):
            @KeyLookup(graph_invalid, 'a', ['d-invalid', 'e'], skip_on_failure=True)
            def load_document(data_folder):
                doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
                for d in doc_lst:
                    yield d

    def test_skip_w_regex(self):
        """
        Test the skip_w_regex option.
        :return:
        """
        doc_lst = [{'_id': 'a:1234'}]

        @KeyLookup(graph_simple, 'a', ['d'], skip_w_regex='a:')
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res = next(res_lst)
        self.assertEqual(res['_id'], 'a:1234')

    def test_mix_mdb_api(self):
        """
        Test with mixed lookups between MongoDB and API
        :return:
        """
        doc_lst = [{'_id': 'start1'}]

        @KeyLookup(graph_mix, 'mix1', ['mix3'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res = next(res_lst)
        self.assertEqual(res['_id'], 'end1')

    def test_pubchem_api(self):
        """
        Test 'inchi' to 'inchikey' conversion using mychem.info
        :return:
        """
        doc_lst = [{'_id': 'InChI=1S/C8H9NO2/c1-6(10)9-7-2-4-8(11)5-3-7/h2-5,11H,1H3,(H,9,10)'}]

        @KeyLookup(graph_pubchem, 'inchi', ['inchikey'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res = next(res_lst)
        self.assertEqual(res['_id'], 'RZVAJINKPMORJF-UHFFFAOYSA-N')

    def test_input_source_fields(self):
        """
        Test input source field options.  These are complicated tests with input source field
        of varying depth and complexity.  Multiple documents are converted.
        Conversion to InchiKey is performed.
        :return:
        """
        doc_lst = [
            {
                '_id': 'test2_drugbank',
                'pharmgkb': {
                    'xref': {
                        'drugbank_id': 'a:1234'
                    }
                }
            }
        ]

        @KeyLookup(graph_simple, [('a', 'pharmgkb.xref.drugbank_id')], ['d', 'e'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        r = next(res_lst)
        self.assertEqual(r['_id'], 'd:1234')

    @unittest.skip("Broken test - stale, perhaps a data issue")
    def test_long_doc_lst(self):
        """
        Test a document list containing 12 entries.  Verify that the correct
        number of documents are returned.
        :return:
        """

        # Long document list - created manually for a unique test
        doc_lst = [
            {
                '_id': 'test1',
                'chebi': 'CHEBI:1391',
            },
            {
                '_id': 'test2',
                'pubchem': '178014',
            },
            {
                # this test document should still be returned
                '_id': 'test3',
            },
            {
                '_id': 'test4',
                'drugbank': 'DB11940',
            },
            {
                '_id': 'test5',
                'chebi': 'CHEBI:28689',
            },
            {
                '_id': 'test6',
                'pubchem': '164045',
            },
            {
                '_id': 'test7',
                'drugbank': 'DB01076'
            },
            {
                '_id': 'test8',
                'drugbank': 'DB03510',
            },
            {
                '_id': 'test9',
                'pubchem': '40467070',
            },
            {
                '_id': 'test10',
                'chebi': 'CHEBI:135847',
            },
            {
                '_id': 'test11',
                'pubchem': '10484732',
            },
            {
                '_id': 'test12',
                'pubchem': '23305354',
            },
        ]

        answers = [
            'SHXWCVYOXRDMCX-UHFFFAOYSA-N',
            'CXHDSLQCNYLQND-XQRIHRDZSA-N',
            'test3',
            'XMYKNCNAZKMVQN-NYYWCZLTSA-N',
            'FMGSKLZLMKYGDP-USOAJAOKSA-N',
            'YAFGHMIAFYQSCF-UHFFFAOYSA-N',
            'XUKUURHRXDUEBC-KAYWLYCHSA-N',
            'RXRZOKQPANIEDW-KQYNXXCUSA-N',
            'BNQDCRGUHNALGH-ZCFIWIBFSA-N',
            'CGVWPQOFHSAKRR-NDEPHWFRSA-N',
            'PCZHWPSNPWAQNF-LMOVPXPDSA-N',
            'FABUFPQFXZVHFB-CFWQTKTJSA-N',
        ]

        # Test a list being passed with 12 documents
        @KeyLookup(graph_mychem, [('chebi', 'chebi'), ('drugbank', 'drugbank'), ('pubchem', 'pubchem')], ['inchikey'])
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res_cnt = 0
        for res in res_lst:
            res_cnt += 1
            if not res['_id'] in answers:
                print(res)
            self.assertTrue(res['_id'] in answers)
        self.assertEqual(res_cnt, 12)

    def test_regex(self):
        """
        Test the RegExEdge in a network.
        """
        @KeyLookup(graph_regex, 'a', ['bregex'])
        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        # Initial Test Case
        doc_lst = [{'_id': 'a:1234'}]
        res_lst = load_document(doc_lst)

        res = next(res_lst)
        self.assertEqual(res['_id'], 'bregex:1234')

    def test_failure1(self):
        """
        Test behavior on lookup failure
        """
        @KeyLookup(graph_simple, 'a', ['e'])
        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        # Failure Test Case
        doc_lst = [{'_id': 'a:f1'}]
        res_lst = load_document(doc_lst)

        res = next(res_lst)
        self.assertEqual(res['_id'], 'a:f1')

    # TODO: this test should be reactivated once we have a "CopyEdge" class implemented
    #def test_copyid(self):
    #    """
    #    Test behavior on lookup lookup copy.
    #    Lookup fails, second identifier value is copied over.
    #    """
    #    @KeyLookup(graph_simple, ['a', ('b', 'b_id')], ['e', 'b'])
    #    def load_document(doc_lst):
    #        for d in doc_lst:
    #            yield d

    #    # Copy from second field
    #    doc_lst = [{'_id': 'a:f1', 'b_id': 'b:f1'}]
    #    res_lst = load_document(doc_lst)

    #    res = next(res_lst)
    #    self.assertEqual(res['_id'], 'b:f1')

    def test_debug_mode(self):
        """
        Test debug mode 'a' to 'e' conversion using the simple test
        :return:
        """
        # the 'debug' parameter was moved from __init__ to __call__
        keylookup = KeyLookup(graph_simple, 'a', ['e'])

        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        # Initial Test Case
        doc_lst = [
            {'_id': 'a:1234'},
            {'_id': 'skip_me'}
        ]

        # Apply the KeyLookup decorator
        res_lst = keylookup(load_document, debug=['a:1234'])(doc_lst)

        res = next(res_lst)
        self.assertEqual(res['_id'], 'e:1234')

        # Verify that the debug information is actually inside of the resulting document
        self.assertTrue('dt_debug' in res)

        # Verify that the generator is out of documents
        with self.assertRaises(StopIteration):
            next(res_lst)

    def test_case_insensitive(self):
        """
        Case insensitive test for key lookup - artificial document.
        :return:
        """
        @KeyLookup(graph_ci, 'a', ['b'], idstruct_class=CIIDStruct)
        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        # Test Case - upper case A in id
        doc_lst = [{'_id': 'A:1234'}]
        res_lst = load_document(doc_lst)

        res = next(res_lst)
        self.assertEqual(res['_id'], 'b:1234')

        # Verify that the generator is out of documents
        with self.assertRaises(StopIteration):
            next(res_lst)

    def test_id_priority_list(self):
        """
        Unit test for id_priority_list and related methods.
        """
        input_types = [('1', 'doc.a'), ('5', 'doc.b'), ('10', 'doc.c'), ('15', 'doc.d')]
        output_types = ['1', '5', '10', '15']
        keylookup = DataTransform(input_types, output_types)

        # set th id_priority_list using the setter and verify that
        # that input_types and output_types are in the correct order.
        keylookup.id_priority_list = ['10', '1']

        # the resulting order for both lists should be 10, 1, 5, 15
        # - 10, and 1 are brought to the beginning of the list
        # - and the order of 5 and 15 remains the same
        self.assertEqual(keylookup.input_types[0][0], '10')
        self.assertEqual(keylookup.input_types[1][0], '1')
        self.assertEqual(keylookup.input_types[2][0], '5')
        self.assertEqual(keylookup.input_types[3][0], '15')
        self.assertEqual(keylookup.output_types[0], '10')
        self.assertEqual(keylookup.output_types[1], '1')
        self.assertEqual(keylookup.output_types[2], '5')
        self.assertEqual(keylookup.output_types[3], '15')

