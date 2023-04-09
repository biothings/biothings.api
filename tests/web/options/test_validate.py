import pytest

from biothings.web.options import OptionError, Validator


def test_01():
    vld = Validator({"enum": ("json", "html", "yaml")})
    vld.validate("json")
    vld.validate("html")
    with pytest.raises(OptionError):
        vld.validate("json-ld")


def test_02():
    vld = Validator({})
    vld.validate("json")
    vld.validate("json-ld")
    vld.validate("json-ld-ld")


def test_03():
    vld = Validator({"max": 3})
    vld.validate(3)
    vld.validate(-3)
    vld.validate((1, 2, 3))
    with pytest.raises(OptionError):
        vld.validate(5)
    with pytest.raises(OptionError):
        vld.validate((1, 2, 3, 4))
    with pytest.raises(OptionError):
        vld.validate([1, 2, 3, 4, 5])
