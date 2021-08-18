from biothings.utils.common import merge

def test_merge_0():
    x = {}
    y = {}
    merge(x, y)
    print(x)

def test_merge_1():
    x = {
        "index": {
            "name1": {
                "doc_type": "news",
                "happy": False
            }
        }
    }
    y = {
        "index": {
            "name1": {
                "happy": True,
                "count": 100
            }
        }
    }
    merge(x, y)
    print(x)

def test_merge_2():
    x = {"a": {"b": "c"}}
    y = {"a": {
        "__REPLACE__": True,
        "b'": {
            "__REPLACE__": False,
            "c": "d"
        }
    }}
    merge(x, y)
    print(x)

def test_merge_3():
    x = {"a": "b"}
    y = {"a": {"b": "c"}}
    merge(x, y)
    print(x)

def test_merge_4():
    x = {"a": {"__REPLACE__": True, "b": "c"}, "__REPLACE__": True}
    y = {"a": {"b": "d"}}
    merge(x, y)
    print(x)
