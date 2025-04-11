import unittest

from biothings.utils.dataload import unlist_incexcl


class TestUnlistIncExclFunction(unittest.TestCase):
    def test_single_element_list(self):
        """
        Test unlisting a single element list.
        :return:
        """
        data = {"key": ["value"]}
        expected = {"key": "value"}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_single_element_nested_list(self):
        """
        Test unlisting a single element nested list.
        :return:
        """
        data = {"key": [{"subkey": ["subvalue"]}]}
        expected = {"key": {"subkey": "subvalue"}}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_multiple_element_list(self):
        """
        Test that a multiple element list is not unlisted.
        :return:
        """
        data = {"key": ["value1", "value2"]}
        expected = {"key": ["value1", "value2"]}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_nested_dict_with_single_element_list(self):
        """
        Test unlisting a nested dictionary with single element list.
        :return:
        """
        data = {"gene": [{"id": 1017, "source": ["clinvar"]}, {"id": 1018, "source": ["bg"]}]}
        expected = {"gene": [{"id": 1017, "source": "clinvar"}, {"id": 1018, "source": "bg"}]}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_nested_dict_with_multiple_element_list(self):
        """
        Test nested dictionary with multiple element list, ensuring no unlisting.
        :return:
        """
        data = {"gene": [{"id": 1017, "source": ["clinvar", "dbsnp"]}, {"id": 1018, "source": ["bg"]}]}
        expected = {"gene": [{"id": 1017, "source": ["clinvar", "dbsnp"]}, {"id": 1018, "source": "bg"}]}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_empty_list(self):
        """
        Test that an empty list remains unchanged.
        :return:
        """
        data = {"key": []}
        expected = {"key": []}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_no_list(self):
        """
        Test that a value without a list remains unchanged.
        :return:
        """
        data = {"key": "value"}
        expected = {"key": "value"}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_deeply_nested_single_element_list(self):
        """
        Test unlisting a deeply nested single element list.
        :return:
        """
        data = {"key": {"subkey": [{"subsubkey": ["subsubvalue"]}]}}
        expected = {"key": {"subkey": {"subsubkey": "subsubvalue"}}}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_no_change_needed(self):
        """
        Test that no changes are made if no unlisting is needed.
        :return:
        """
        data = {"key": {"subkey": "subvalue"}}
        expected = {"key": {"subkey": "subvalue"}}
        self.assertEqual(unlist_incexcl(data), expected)

    def test_include_keys_single_element_list(self):
        """
        Test unlisting a single element list with include_keys specified.
        :return:
        """
        data = {"key1": ["value1"], "key2": ["value2"]}
        expected = {"key1": "value1", "key2": ["value2"]}
        self.assertEqual(unlist_incexcl(data, include_keys=["key1"]), expected)

    def test_include_keys_nested_single_element_list(self):
        """
        Test unlisting a nested single element list with include_keys specified.
        :return:
        """
        data = {"key": {"subkey1": ["subvalue1"], "subkey2": ["subvalue2"]}}
        expected = {"key": {"subkey1": "subvalue1", "subkey2": ["subvalue2"]}}
        self.assertEqual(unlist_incexcl(data, include_keys=["key.subkey1"]), expected)

    def test_include_keys_no_unlist(self):
        """
        Test that no unlisting occurs when include_keys doesn't match the path.
        :return:
        """
        data = {"key1": ["value1"], "key2": ["value2"]}
        expected = {"key1": ["value1"], "key2": ["value2"]}
        self.assertEqual(unlist_incexcl(data, include_keys=["key3"]), expected)

    def test_exclude_keys_single_element_list(self):
        """
        Test unlisting a single element list with exclude_keys specified.
        :return:
        """
        data = {"key1": ["value1"], "key2": ["value2"]}
        expected = {"key1": "value1", "key2": ["value2"]}
        self.assertEqual(unlist_incexcl(data, exclude_keys=["key2"]), expected)

    def test_exclude_keys_nested_single_element_list(self):
        """
        Test unlisting a nested single element list with exclude_keys specified.
        :return:
        """
        data = {"key": {"subkey1": ["subvalue1"], "subkey2": ["subvalue2"]}}
        expected = {"key": {"subkey1": ["subvalue1"], "subkey2": "subvalue2"}}
        self.assertEqual(unlist_incexcl(data, exclude_keys=["key.subkey1"]), expected)

    def test_exclude_keys_no_unlist(self):
        """
        Test that no unlisting occurs when exclude_keys matches the path.
        :return:
        """
        data = {"key1": ["value1"], "key2": ["value2"]}
        expected = {"key1": ["value1"], "key2": "value2"}
        self.assertEqual(unlist_incexcl(data, exclude_keys=["key1"]), expected)

    def test_include_and_exclude_keys(self):
        """
        Test both include_keys and exclude_keys are used together.
        :return:
        """
        data = {"key1": ["value1"], "key2": ["value2"], "key3": ["value3"]}
        expected = {"key1": "value1", "key2": ["value2"], "key3": ["value3"]}
        self.assertEqual(unlist_incexcl(data, include_keys=["key1", "key2"], exclude_keys=["key2"]), expected)

    def test_exclude_and_include_with_nested_structure(self):
        """
        Test both include_keys and exclude_keys are used together in a nested structure.
        :return:
        """
        data = {"key": {"subkey1": ["subvalue1"], "subkey2": ["subvalue2"]}}
        expected = {"key": {"subkey1": "subvalue1", "subkey2": ["subvalue2"]}}
        self.assertEqual(unlist_incexcl(data, include_keys=["key.subkey1"], exclude_keys=["key.subkey2"]), expected)


if __name__ == "__main__":
    unittest.main()
