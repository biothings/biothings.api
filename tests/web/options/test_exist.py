import pytest

from biothings.web.options import Existentialist, OptionError


def test_01():
    assert (
        Existentialist(
            {
                "keyword": "q",
                "required": True,
            }
        ).inquire("cdk2")
        == "cdk2"
    )


def test_02():
    with pytest.raises(OptionError):
        assert Existentialist(
            {
                "keyword": "q",
                "required": True,
            }
        ).inquire(None)


def test_03():
    assert (
        Existentialist(
            {
                "keyword": "size",
                "default": 10,
            }
        ).inquire(None)
        == 10
    )


def test_04():
    assert (
        Existentialist(
            {
                "keyword": "raw",
                "default": False,
            }
        ).inquire(None)
        is False
    )


def test_05():
    assert (
        Existentialist(
            {
                "keyword": "raw",
                "default": True,
            }
        ).inquire(False)
        is False
    )
