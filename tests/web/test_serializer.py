from biothings.web.handlers import serializer


def test_unified_api_dispatch():
    """Test that unified API uses correct implementation based on Python version and free-threading"""
    import sys

    # Check which implementation should be active
    python_version = sys.version_info
    # Use sys.flags.nogil for Python 3.13+ to detect free-threading
    is_free_threaded = (
        python_version >= (3, 14) and
        hasattr(sys.flags, 'nogil') and
        sys.flags.nogil
    )
    use_msgspec = (
        python_version >= (3, 14) and
        is_free_threaded
    )

    # Verify unified API functions exist
    assert hasattr(serializer, 'load_json')
    assert hasattr(serializer, 'to_json')
    assert hasattr(serializer, 'to_json_file')
    assert hasattr(serializer, 'json_loads')
    assert hasattr(serializer, 'json_dumps')

    # Verify dispatch is correct
    assert serializer._USE_MSGSPEC == use_msgspec

    if use_msgspec:
        assert serializer.to_json == serializer.to_json_msgspec
        assert serializer.load_json == serializer.load_json_msgspec
    else:
        assert serializer.to_json == serializer.to_json_orjson
        assert serializer.load_json == serializer.load_json_orjson


def test_unified_api_roundtrip_simple():
    """Test unified API roundtrip with simple data"""
    payload = {
        "key1": "val1",
        "key2": {"nested": [1, 2, 3], "ok": True, "none": None},
        "key3": ["a", "b", {"c": 1.2}],
    }

    json_str = serializer.to_json(payload)
    restored = serializer.load_json(json_str)

    assert restored == payload


def test_unified_api_roundtrip_complex():
    """Test unified API roundtrip with UserDict, UserList, and non-string keys"""
    from collections import UserDict, UserList

    payload = {
        "key1": "val1",
        "user_dict": UserDict({"key3.1": "val3.1", 2: "int-key"}),
        "user_list": UserList(["val4.1", "val4.2"]),
        100: "value_with_int_key",
    }

    json_str = serializer.to_json(payload)
    restored = serializer.load_json(json_str)

    # JSON converts non-string keys to strings and UserDict/UserList to dict/list
    assert restored["user_dict"] == {"key3.1": "val3.1", "2": "int-key"}
    assert restored["user_list"] == ["val4.1", "val4.2"]
    assert restored["100"] == "value_with_int_key"


def test_unified_api_roundtrip_with_options():
    """Test unified API roundtrip with indent and sort_keys options"""
    payload = {"b": 1, "a": 2}

    # Test with indent
    json_indented = serializer.to_json(payload, indent=True)
    assert "\n" in json_indented
    restored = serializer.load_json(json_indented)
    assert restored == payload

    # Test with sort_keys
    json_sorted = serializer.to_json(payload, sort_keys=True)
    assert json_sorted.index('"a"') < json_sorted.index('"b"')
    restored = serializer.load_json(json_sorted)
    assert restored == payload

    # Test return_bytes
    json_bytes = serializer.to_json(payload, return_bytes=True)
    assert isinstance(json_bytes, bytes)
    restored = serializer.load_json(json_bytes)
    assert restored == payload


def test_unified_api_aliases():
    """Test that json_loads and json_dumps aliases work"""
    payload = {"key": "value"}

    json_str = serializer.json_dumps(payload)
    restored = serializer.json_loads(json_str)

    assert restored == payload


def test_yaml_01():
    import yaml

    obj = {
        "key1": "val1",
        "key2": ["val2", "val3"],
    }
    yaml_str = serializer.to_yaml(obj)
    assert yaml.load(yaml_str, Loader=yaml.SafeLoader) == obj


def test_url_01():
    url = serializer.URL("http://www.mygene.info/v1/gene/1017?fields=symbol&format=html")
    assert url.remove() == "http://www.mygene.info/v1/gene/1017?fields=symbol"

