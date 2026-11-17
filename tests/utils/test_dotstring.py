from biothings.utils.dotstring import key_value, list_length, remove_key, set_key_value


def test_key_value_skips_plain_list_items():
    doc = {"a": ["x", {"b": 1}]}
    assert list(key_value(doc, "a.b")) == [1]


def test_set_key_value_skips_plain_list_items():
    doc = {"a": ["x", {"b": 1}]}
    assert set_key_value(doc, "a.b", 2) == {"a": ["x", {"b": 2}]}


def test_remove_key_skips_plain_list_items():
    doc = {"a": ["x", {"b": 1, "c": 2}]}
    assert remove_key(doc, "a.b") == {"a": ["x", {"c": 2}]}


def test_list_length_with_plain_list_items():
    doc = {"a": ["x", {"b": [1, 2, 3]}]}
    assert list_length(doc, "a.b") == 3
