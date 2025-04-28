from biothings.web.handlers import serializer


def test_json_01():
    import json
    from collections import UserDict, UserList
    from datetime import datetime

    obj = {
        "key1": "val1",
        "key2": datetime.now().astimezone(),
        "key3": UserDict({"key3.1": "val3.1"}),
        "key4": UserList(["val4.1", "val4.2"]),
    }
    json_str = serializer.to_json(obj)
    obj2 = json.loads(json_str)
    obj2["key2"] = datetime.fromisoformat(obj2["key2"])
    assert obj2 == obj


def test_json_02():
    serializer.to_json(1)
    serializer.to_json("")
    serializer.to_json([])
    serializer.to_json([{}])


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
