import pytest

from biothings.web.options import Converter, OptionError


def test_01():
    cvt = Converter(
        translations={
            "refseq:": "refseq_agg:",
            "reporter:": "reporter.*:",
            "hgnc:": "HGNC:",
        }
    )
    assert cvt.translate("refseq:123") == "refseq_agg:123"
    assert cvt.translate("reporter:123") == "reporter.*:123"
    assert cvt.translate("hgnc:123") == "HGNC:123"


def test_02():
    cvt = Converter(
        translations=(
            (("human", 2), "9606"),
            ("mouse", "10090"),
            ("^rat$", "10116"),
        )
    )
    assert cvt.translate("human") == "9606"
    assert cvt.translate("HUMAN") == "9606"
    assert cvt.translate("mouse") == "10090"
    assert cvt.translate("mouses") == "10090s"  # COMPARISON A
    assert cvt.translate("MOUSE") == "MOUSE"
    assert cvt.translate("rat") == "10116"
    assert cvt.translate("rats") == "rats"  # COMPARISON A


def test_03():
    cvt = Converter(
        translations={
            "hello": "foo",
            "world": "bar",
        }
    )
    assert cvt.translate("hello world") == "foo bar"
    assert cvt.translate("hello world!") == "foo bar!"


def test_jmespath():
    import jmespath

    cvt = Converter(keyword="jmespath")

    # a simple example
    parent_path, target_field, jmes_query = cvt.translate("tags|[?name=='Metadata']")
    assert parent_path == ""
    assert target_field == "tags"
    assert isinstance(jmes_query, jmespath.parser.ParsedResult)
    assert jmes_query.expression == "[?name=='Metadata']"

    # a more complex example
    parent_path, target_field, jmes_query = cvt.translate(
        "aaa.bbb|[?(sub_a=='val_a'||sub_a=='val_aa')&&sub_b=='val_b']"
    )
    assert parent_path == "aaa"
    assert target_field == "bbb"
    assert isinstance(jmes_query, jmespath.parser.ParsedResult)

    # target_field_path can be empty if it operates on the root object
    parent_path, target_field, jmes_query = cvt.translate("|b")
    assert parent_path == ""
    assert target_field == ""
    assert isinstance(jmes_query, jmespath.parser.ParsedResult)
    assert jmes_query.expression == "b"

    # target_field_path can also be . if it operates on the root object
    parent_path, target_field, jmes_query = cvt.translate(".|b")
    assert parent_path == ""
    assert target_field == ""
    assert isinstance(jmes_query, jmespath.parser.ParsedResult)
    assert jmes_query.expression == "b"

    with pytest.raises(OptionError):
        cvt.translate("tags")

    with pytest.raises(OptionError):
        cvt.translate("tags|[?name='Metadata']")
