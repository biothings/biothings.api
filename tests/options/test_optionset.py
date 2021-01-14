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
