from biothings.web.handlers import serializer

def test_json_01():
    from datetime import datetime
    from collections import UserDict, UserList
    return serializer.to_json({
        "key1": "val1",
        "key2": datetime.now(),
        "key3": UserDict({"key3.1": "val3.1"}),
        "key4": UserList(["val4.1", "val4.2"])
    })

def test_json_02():
    serializer.to_json(1)
    serializer.to_json("")
    serializer.to_json([])
    serializer.to_json([{}])

def test_yaml_01():
    return serializer.to_yaml({
        "key1": "val1",
        "key2": ["val2", "val3"]
    })

def test_url_01():
    url = serializer.URL("http://www.mygene.info/v1/gene/1017?fields=symbol&format=html")
    assert url.remove() == "http://www.mygene.info/v1/gene/1017?fields=symbol"


if __name__ == "__main__":
    print(test_url_01())
