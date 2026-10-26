"""
Tests for multidict in biothings.utils.jsonpatch, the object_pairs_hook
that turns the values of duplicate JSON keys into lists.
"""

import json

from biothings.utils.jsonpatch import multidict


class TestMultidict:
    def test_unique_keys(self):
        result = multidict([("b", 1), ("a", [2]), ("c", {"d": None})])
        assert result == {"b": 1, "a": [2], "c": {"d": None}}
        assert type(result) is dict
        assert list(result) == ["b", "a", "c"]

    def test_duplicate_keys_become_lists(self):
        result = multidict([("a", 1), ("b", 2), ("a", [3]), ("c", 4), ("a", 5), ("b", 6)])
        assert result == {"a": [1, [3], 5], "b": [2, 6], "c": 4}
        assert list(result) == ["a", "b", "c"]

    def test_no_pairs(self):
        assert multidict([]) == {}

    def test_as_object_pairs_hook(self):
        doc = json.loads('{"a": 1, "b": {"c": 2, "c": null}, "a": [4]}', object_pairs_hook=multidict)
        assert doc == {"a": [1, [4]], "b": {"c": [2, None]}}
