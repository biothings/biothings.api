import unittest

from biothings.utils.dataload import unlist

class TestUnlistFunction(unittest.TestCase):
    def test_single_element_list(self):
        """
        Test unlisting a single element list.
        :return:
        """
        data = {'key': ['value']}
        expected = {'key': 'value'}
        self.assertEqual(unlist(data), expected)

    def test_single_element_nested_list(self):
        """
        Test unlisting a single element nested list.
        :return:
        """
        data = {'key': [{'subkey': ['subvalue']}]}
        expected = {'key': {'subkey': 'subvalue'}}
        self.assertEqual(unlist(data), expected)

    def test_multiple_element_list(self):
        """
        Test that a multiple element list is not unlisted.
        :return:
        """
        data = {'key': ['value1', 'value2']}
        expected = {'key': ['value1', 'value2']}
        self.assertEqual(unlist(data), expected)

    def test_nested_dict_with_single_element_list(self):
        """
        Test unlisting a nested dictionary with single element list.
        :return:
        """
        data = {'gene': [{'id': 1017, 'source': ['clinvar']}, {'id': 1018, 'source': ['bg']}]}
        expected = {'gene': [{'id': 1017, 'source': 'clinvar'}, {'id': 1018, 'source': 'bg'}]}
        self.assertEqual(unlist(data), expected)

    def test_nested_dict_with_multiple_element_list(self):
        """
        Test nested dictionary with multiple element list, ensuring no unlisting.
        :return:
        """
        data = {'gene': [{'id': 1017, 'source': ['clinvar', 'dbsnp']}, {'id': 1018, 'source': ['bg']}]}
        expected = {'gene': [{'id': 1017, 'source': ['clinvar', 'dbsnp']}, {'id': 1018, 'source': 'bg'}]}
        self.assertEqual(unlist(data), expected)

    def test_empty_list(self):
        """
        Test that an empty list remains unchanged.
        :return:
        """
        data = {'key': []}
        expected = {'key': []}
        self.assertEqual(unlist(data), expected)

    def test_no_list(self):
        """
        Test that a value without a list remains unchanged.
        :return:
        """
        data = {'key': 'value'}
        expected = {'key': 'value'}
        self.assertEqual(unlist(data), expected)

    def test_deeply_nested_single_element_list(self):
        """
        Test unlisting a deeply nested single element list.
        :return:
        """
        data = {'key': {'subkey': [{'subsubkey': ['subsubvalue']}]} }
        expected = {'key': {'subkey': {'subsubkey': 'subsubvalue'}} }
        self.assertEqual(unlist(data), expected)

    def test_no_change_needed(self):
        """
        Test that no changes are made if no unlisting is needed.
        :return:
        """
        data = {'key': {'subkey': 'subvalue'}}
        expected = {'key': {'subkey': 'subvalue'}}
        self.assertEqual(unlist(data), expected)


if __name__ == '__main__':
    unittest.main()
