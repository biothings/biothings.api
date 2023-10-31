import jmespath
import pytest
from jmespath.exceptions import UnknownFunctionError

from biothings.utils.jmespath import options as jmp_options


def test_customfunction_unique():
    doc = {
        "foo": ["a", "b", "c", "e", "e", "c", "d", "a"],
    }
    # without passing jmp_options, it should raise UnknownFunctionError
    # this test should tell us if we accidentally override a build-in function
    with pytest.raises(UnknownFunctionError):
        jmespath.search("foo|unique(@)", doc)

    assert jmespath.search("foo|unique(@)", doc, options=jmp_options) == ["a", "b", "c", "d", "e"]


def test_customfunction_unique_count():
    doc = ["a", "b", "c", "e", "e", "c", "d", "a"]
    # without passing jmp_options, it should raise UnknownFunctionError
    # this test should tell us if we accidentally override a build-in function
    with pytest.raises(UnknownFunctionError):
        jmespath.search("unique_count(@)", doc)

    assert jmespath.compile("unique_count(@)").search(doc, options=jmp_options) == 5
