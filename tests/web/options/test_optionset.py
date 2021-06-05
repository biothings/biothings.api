import pytest
from biothings.web.options import OptionError, OptionSet


def test_01():
    ans = OptionSet({
        "*": {
            "raw": {"type": bool, "default": False, "group": "a"},
            "size": {"type": int, "max": 1000, "alias": "limit"}},
        "GET": {
            "q": {"type": str, "default": "__all__", "group": "a"},
            "from_": {"type": int, "max": 1000, "group": ("a", "b")}}
    }).parse("GET", (None, {
        "limit": "999",
        "raw": "true"
    }))
    assert ans.a.raw is True
    assert ans.a.q == "__all__"
    assert ans.a.from_ is None
    assert ans.b.from_ is None
    assert ans.size == 999

def test_02():
    optionset = OptionSet({
        "*": {"raw": {"type": bool, "default": False, "location": "query"}},
        "POST": {"ids": {"type": list, "required": True, "location": "body"}}
    })

    ans = optionset.parse("POST", (None, None, {
        "raw": "true",
        "ids": "1,2,3"
    }))
    assert ans.raw is False
    assert ans.ids == ["1", "2", "3"]

    ans = optionset.parse("POST", (None, {
        "raw": "true"
    }, None, {
        "ids": [1, 2, 3]
    }))
    assert ans.raw is True
    assert ans.ids == [1, 2, 3]

    with pytest.raises(OptionError) as err:
        optionset.parse("POST", ())
    assert err.value.info["missing"] == "ids"
    assert "alias" not in err.value.info
    assert "keyword" not in err.value.info
    assert "reason" not in err.value.info

def test_03():
    commondef = {
        "*": {"format": {"type": str, "default": "json"}},
        "GET": {"raw": {"type": bool, "default": False}}
    }
    optionset = OptionSet(commondef)
    optionset["GET"]["raw"]["default"] = True
    optionset["POST"] = {"dev": {"type": bool}}
    optionset.setup()

    assert optionset.parse("DELETE", (None, {"format": "yaml"})).format == "yaml"
    assert optionset.parse("POST", (None, {"dev": "true"})).dev is True
    assert optionset.parse("GET", ()).raw is True

def test_04():
    option = OptionSet({
        "*": {"p1": {"group": "a"}},
        "PUT": {"p2": {"group": "b"}},
        "GET": {"p3": {"group": "c"}},
        "DELETE": {"p4": {"group": "d"}},
    })
    assert option.groups == {"a", "b", "c", "d"}
    assert "a" in option.parse("PUT", ())
    assert "b" in option.parse("PUT", ())

    option["*"]["p5"] = {"group": "e"}
    option.setup()
    assert option.groups == {"a", "b", "c", "d", "e"}
