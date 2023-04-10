from concurrent.futures import ThreadPoolExecutor

from biothings.utils.common import get_loop_with_max_workers, merge


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
    merge(x, y)
    print(x)


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


def test_merge_5():
    x = {"index": {"X": {"env": "local"}, "Y": {"env": "local"}}}
    y = {"index": {"X": {"__REMOVE__": True}}}
    merge(x, y)
    print(x)


def test_get_loop():
    # Given
    max_workers = 2

    # Action
    loop = get_loop_with_max_workers(max_workers=max_workers)

    # Asserts
    assert isinstance(loop._default_executor, ThreadPoolExecutor)
    assert loop._default_executor._max_workers == max_workers


if __name__ == "__main__":
    test_merge_0()
    test_merge_1()
    test_merge_2()
    test_merge_3()
    test_merge_4()
    test_merge_5()
    test_get_loop()


# ANS
# {}
# {'index': {'name1': {'doc_type': 'news', 'happy': True, 'count': 100}}}
# {'a': {"B": {'c': 'd'}}}
# {'a': {'b': 'c'}}
# {'a': {'__REPLACE__': True, 'b': 'd'}, '__REPLACE__': True}
# {'index': {'Y': {'env': 'local'}}}
