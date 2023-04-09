import json
from os.path import *

import biothings.utils.jsondiff as jsondiff
import biothings.utils.jsonpatch as jsonpatch


class JsonDiffTest:
    __test__ = True

    def test_scalar(self):
        left = {"one": 1, "ONE": "111"}
        right = {"two": 2, "TWO": "222"}
        patch = jsondiff.make(left, right)
        new_right = jsonpatch.apply_patch(left, patch)
        assert right == new_right
        # do it again, it's a "remove"/"add" op, so we need to ignore
        # conflicts but make sure the result is the one we expect
        new_new_right = jsonpatch.apply_patch(new_right, patch, ignore_conflicts=True, verify=True)
        assert right == new_new_right

    def test_array(self):
        left = {"a": [1, 2, 3]}
        right = {"a": [1, 2, 3, 4, 5]}
        patch = jsondiff.make(left, right)
        new_right = jsonpatch.apply_patch(left, patch)
        assert right == new_right
        # do it again, it's a "replace" op so it can be re-patched safely
        new_new_right = jsonpatch.apply_patch(new_right, patch)
        assert right == new_new_right
        # smaller list on right
        left = {"a": [1, 2, 3, 4, 5]}
        right = {"a": [6, 7]}
        patch = jsondiff.make(left, right)
        new_right = jsonpatch.apply_patch(left, patch)
        assert right == new_right

    def test_object(self):
        left = {"c": {"1": 1, "2": 2, "3": 3}}
        right = {"c": {"1": 1, "4": 4, "5": 5}}
        patch = jsondiff.make(left, right)
        new_right = jsonpatch.apply_patch(left, patch)
        assert right == new_right
        # patch contains "add" and "remove" ops, so it cannot be re-patched that easy...
        # use ignore and verify
        new_new_right = jsonpatch.apply_patch(new_right, patch, ignore_conflicts=True, verify=True)
        assert right == new_new_right

    def test_smalldoc(self):
        left = {"a": [9, 8, 3], "b": "B", "c": {"1": 1, "2": 2, "3": 3}}
        right = {"c": {"5": 5, "4": 4, "1": 1}, "B": "capitalB", "a": [1, 2, 3, 4, 5], "b": "bbb"}
        patch = jsondiff.make(left, right)
        new_right = jsonpatch.apply_patch(left, patch)
        new_new_right = jsonpatch.apply_patch(new_right, patch, ignore_conflicts=True, verify=True)
        assert right == new_new_right

    def test_bigdoc(self):
        v2_path = join(dirname(__file__), "v2.json")
        v3_path = join(dirname(__file__), "v3.json")
        v2 = json.load(open(v2_path))
        v3 = json.load(open(v3_path))
        patch = jsondiff.make(v2, v3)
        new_v3 = jsonpatch.apply_patch(v2, patch)
        assert v3 == new_v3
