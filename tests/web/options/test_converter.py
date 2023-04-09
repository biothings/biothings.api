import pytest

from biothings.web.options import Converter, FormArgCvter, JsonArgCvter, OptionError, QueryArgCvter

cvt = Converter()
itt = Converter(strict=False)

# ONLY TEST A FEW BASIC DATA TYPES

# --------------
#    Common
# --------------


def test_common_to_str():
    assert cvt("test", str) == "test"


def test_common_to_bool():
    assert cvt("true", bool) is True
    assert cvt("True", bool) is True
    assert cvt("t", bool) is True
    assert cvt("y", bool) is True
    assert cvt("1", bool) is True
    assert cvt("false", bool) is False
    assert cvt("False", bool) is False
    assert cvt("n", bool) is False
    assert cvt("0", bool) is False
    assert cvt("", bool) is False  # MARK A

    assert itt("-1.2", bool) is False
    assert itt("2.0", bool) is True


def test_common_to_int():
    assert cvt("0", int) == 0
    assert cvt("1", int) == 1
    assert cvt("-1", int) == -1
    # no down casting
    with pytest.raises(OptionError):
        cvt("1.23", int)
    # allow down casting
    assert itt("1.23", int) == 1
    with pytest.raises(OptionError):
        cvt("not_a_number", int)


def test_common_to_float():
    one = cvt("1.0", float)
    assert isinstance(one, float)
    assert one == 1.0
    one = cvt("1", float)
    assert isinstance(one, float)
    assert one == 1.0


def test_common_to_list():
    # may need more comprehensive testing
    assert cvt("CDK2 CDK3", list) == ["CDK2", "CDK3"]
    assert cvt('"CDK2 CDK3"\n CDK4', list) == ["CDK2 CDK3", "CDK4"]


# --------------
#   PathArgs
# --------------

# No additional tests necessary.

# --------------
#   QueryArgs
# --------------


def test_query_to_bool():
    assert QueryArgCvter()("", bool)  # MARK A


# --------------
#   BodyArgs
# --------------


def test_body_with_jsoninput():
    cvt = FormArgCvter(jsoninput=True)
    assert cvt("null", object) is None
    assert cvt("123", int) == 123
    assert cvt("123", list) == ["123"]
    assert cvt('{"a":"b"}', dict) == {"a": "b"}
    assert cvt("[1,2,3]", list) == [1, 2, 3]


def test_body_without_jsoninput():
    cvt = FormArgCvter()
    itt = FormArgCvter(strict=False)
    assert cvt("123", list) == ["123"]
    assert itt("123", list) == ["123"]


# --------------
#   JsonArgs
# --------------


def test_json_strict():
    cvt = JsonArgCvter()
    assert cvt(3, int) == 3
    with pytest.raises(OptionError):
        cvt(3.3, int)
    with pytest.raises(OptionError):
        cvt("3", int)
    with pytest.raises(OptionError):
        cvt("{}", dict)
    with pytest.raises(OptionError):
        cvt("abc", list)  # MARK B


def test_json_nonstrict():
    itt = JsonArgCvter(strict=False)
    assert itt(3, int) == 3
    assert itt("abc", list) == ["abc"]  # MARK B
