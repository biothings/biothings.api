import unittest
from biothings.utils.dataload import merge_struct
from biothings.utils.dataload import merge_root_keys


class TestDataTransform(unittest.TestCase):

    def setUp(self):
        """
        Setup test documents.
        """
        self.doc1 = {
                '_id': 'ALELTFCQZDXAMQ-UHFFFAOYSA-N',
                'unii': {
                    'preferred_term': 'drugnameA'
                    }
                }
        self.doc2 = {
                '_id': 'ALELTFCQZDXAMQ-UHFFFAOYSA-N',
                'unii': {
                    'preferred_term': 'drugnameB'
                    }
                }

    def test_merge_struct(self):
        """
        Test the merge_struct utility function.

        A 'deep' merge is performed.
        """
        res = merge_struct(self.doc1, self.doc2)
        self.assertEquals(res['_id'], 'ALELTFCQZDXAMQ-UHFFFAOYSA-N')
        # res = unii.preferred_term = ['drugnameA', 'drugnameB']
        self.assertTrue(isinstance(res['unii']['preferred_term'], list))

    def test_merge_root_keys(self):
        """
        Test the merge_root_keys utility function
        """
        res = merge_root_keys(self.doc1, self.doc2, exclude=['_id'])
        self.assertEquals(res['_id'], 'ALELTFCQZDXAMQ-UHFFFAOYSA-N')
        # res = unii = [{'preferred_term': 'drugnameA'}, {'preferred_term': 'drugnameB'}]
        self.assertTrue(isinstance(res['unii'], list))
        self.assertEquals(res['unii'][0]['preferred_term'], 'drugnameA')
        self.assertEquals(res['unii'][1]['preferred_term'], 'drugnameB')

