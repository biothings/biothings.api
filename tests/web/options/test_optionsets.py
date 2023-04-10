from biothings.web.options import OptionsManager


def test_01():
    optionsets = OptionsManager()
    optionsets.add(
        "test",
        {
            "*": {"p1": {"group": "a"}},
            "PUT": {"p2": {"group": "b"}},
        },
        ("c", "d"),
    )
    args = optionsets.get("test").parse("GET", ())
    assert "a" in args
    assert "b" in args
    assert "c" in args
    assert "d" in args
