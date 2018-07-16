from biothings import config as btconfig
from biothings import config_for_app
config_for_app(btconfig)

from biothings.utils.keylookup_mdb import KeyLookupMDB as KeyLookup
from biothings.tests.keylookup_graphs import graph_simple, \
    graph_weights, graph_one2many, graph_invalid
import unittest
import biothings.utils.mongo as mongo


class TestKeyLookup(unittest.TestCase):

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

    def test_simple(self):
        """
        Simple test for key lookup - artificial document.

        The network contains a cycle that is avoided by the networkx algorithm.
        :return:
        """

        self.collections = ['b', 'c', 'd', 'e']

        @KeyLookup(graph_simple, self.collections, 'a', ['d', 'e'])
        def load_document(doc_lst):
            for d in doc_lst:
                yield d

        # Initial Test Case
        doc_lst = [{'_id': 'a:1234'}]
        res_lst = load_document(doc_lst)

        for res in res_lst:
            # Check for expected key
            self.assertEqual(res['_id'], 'd:1234')

    def test_one2many(self):
        """
        test for one to many key lookup - artificial document.
        :return:
        """

        self.collections = ['bb', 'cc']

        doc_lst = [{'input_key': 'a:1234'}]

        @KeyLookup(graph_one2many, self.collections, [('aa', 'input_key')], ['cc'])
        def load_document():
            for d in doc_lst:
                yield d

        # Initial Test Case
        res_lst = [d for d in load_document()]

        # Check for expected keys
        # There are 2 branches along the document path
        self.assertEqual(res_lst[0]['_id'], 'c:1234')
        self.assertEqual(res_lst[1]['_id'], 'c:01')
        self.assertEqual(res_lst[2]['_id'], 'c:02')

    def test_input_types(self):
        """
        test for input_types - artificial documents.
        :return:
        """

        self.collections = ['b', 'c', 'd', 'e']

        # Initial Test Case
        doc_lst = [
            {'a': 'a:1234'},
            {'b': 'b:1234'}
        ]
        @KeyLookup(graph_simple, self.collections, [('a', 'a'), ('b', 'b')], ['d', 'e'])
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

        self.collections = ['bbb', 'ccc', 'ddd', 'eee']

        @KeyLookup(graph_weights, self.collections, 'aaa', ['eee'])
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

        self.collections = ['b', 'c', 'd', 'e']

        @KeyLookup(graph_simple, self.collections, 'a', ['d', 'e'])
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
        Simple test for key lookup - artificial document.

        This test tests the skip_on_failure option which skips documents
        where lookup was unsuccessful.

        :return:
        """

        self.collections = ['b', 'c', 'd', 'e']

        @KeyLookup(graph_simple, self.collections, 'a', ['d', 'e'], skip_on_failure=True)
        def load_document(data_folder):
            doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
            for d in doc_lst:
                yield d

        # Test a list being passed with 3 documents, 2 are returned, 1 is skipped
        res_lst = load_document('data/folder/')
        res1 = next(res_lst)
        res3 = next(res_lst)
        self.assertEqual(res1['_id'], 'd:1234')
        self.assertEqual(res3['_id'], 'd:1234')

        # Verify that the generator is out of documents
        with self.assertRaises(StopIteration):
            next(res_lst)

    def test_strangecases(self):
        """
        Test invalid input that should generate exceptions.
        :return:
        """

        self.collections = ['b', 'c', 'd', 'e']

        # Null collections
        with self.assertRaises(ValueError):
            @KeyLookup(graph_simple, ['a-invalid'], 'a-invalid', ['d', 'e'], skip_on_failure=True)
            def load_document(data_folder):
                doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
                for d in doc_lst:
                    yield d

        # invalid input-type
        with self.assertRaises(ValueError):
            @KeyLookup(graph_simple, self.collections, 'a-invalid', ['d', 'e'], skip_on_failure=True)
            def load_document(data_folder):
                doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
                for d in doc_lst:
                    yield d

        # Invalid output-type
        with self.assertRaises(ValueError):
            @KeyLookup(graph_simple, self.collections, 'a', ['d-invalid', 'e'], skip_on_failure=True)
            def load_document(data_folder):
                doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
                for d in doc_lst:
                    yield d

        # Invalid graph
        with self.assertRaises(ValueError):
            @KeyLookup(graph_invalid, self.collections, 'a', ['d-invalid', 'e'], skip_on_failure=True)
            def load_document(data_folder):
                doc_lst = [{'_id': 'a:1234'}, {'_id': 'a:invalid'}, {'_id': 'a:1234'}]
                for d in doc_lst:
                    yield d

    def test_skip_w_regex(self):
        """
        Test the skip_w_regex option.
        :return:
        """

        collections = ['b', 'c', 'd', 'e']
        doc_lst = [{'_id': 'a:1234'}]

        @KeyLookup(graph_simple, collections, 'a', ['d'], skip_w_regex='a:')
        def load_document(data_folder):
            for d in doc_lst:
                yield d

        res_lst = load_document('data/folder/')
        res = next(res_lst)
        self.assertEqual(res['_id'], 'a:1234')
