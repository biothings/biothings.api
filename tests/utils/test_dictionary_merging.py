"""
Tests our merging function in the biothings.utils.common module
for handling merging of different dictionary records
"""

from biothings.utils.common import merge


def test_merge_0():
    x = {}
    y = {}
    merge_result = merge(x, y)
    assert not merge_result


def test_merge_1():
    x = {
        "index": {
            "name1": {
                "doc_type": "news",
                "happy": False,
            }
        }
    }
    y = {
        "index": {
            "name1": {
                "happy": True,
                "count": 100,
            }
        }
    }
    expected_merge_result = {"index": {"name1": {"doc_type": "news", "happy": True, "count": 100}}}
    merge_result = merge(x, y)
    assert expected_merge_result == merge_result


def test_merge_2():
    x = {"a": {"b": "c"}}
    y = {
        "a": {
            "__REPLACE__": True,
            "B": {
                "__REPLACE__": False,
                "c": "d",
            },
        }
    }
    expected_merge_result = {"a": {"B": {"c": "d"}}}

    merge_result = merge(x, y)
    assert expected_merge_result == merge_result


def test_merge_3():
    x = {"a": "b"}
    y = {"a": {"b": "c"}}
    expected_merge_result = {"a": {"b": "c"}}

    merge_result = merge(x, y)
    assert expected_merge_result == merge_result


def test_merge_4():
    x = {"a": {"__REPLACE__": True, "b": "c"}, "__REPLACE__": True}
    y = {"a": {"b": "d"}}
    expected_merge_result = {"a": {"__REPLACE__": True, "b": "d"}, "__REPLACE__": True}

    merge_result = merge(x, y)
    assert expected_merge_result == merge_result


def test_merge_5():
    x = {"index": {"X": {"env": "local"}, "Y": {"env": "local"}}}
    y = {"index": {"X": {"__REMOVE__": True}}}
    expected_merge_result = {"index": {"Y": {"env": "local"}}}

    merge_result = merge(x, y)
    assert expected_merge_result == merge_result
