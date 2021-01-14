from biothings.web.options import Converter

def test_01():
    cvt = Converter(translations={
        "refseq:": "refseq_agg:",
        "reporter:": "reporter.*:",
        "hgnc:": "HGNC:",
    })
    assert cvt.translate("refseq:123") == "refseq_agg:123"
    assert cvt.translate("reporter:123") == "reporter.*:123"
    assert cvt.translate("hgnc:123") == "HGNC:123"


def test_02():
    cvt = Converter(translations=(
        (("human", 2), "9606"),
        ("mouse", "10090"),
        ("^rat$", "10116")
    ))
    assert cvt.translate("human") == "9606"
    assert cvt.translate("HUMAN") == "9606"
    assert cvt.translate("mouse") == "10090"
    assert cvt.translate("mouses") == "10090s"  # COMPARISON A
    assert cvt.translate("MOUSE") == "MOUSE"
    assert cvt.translate("rat") == "10116"
    assert cvt.translate("rats") == "rats"  # COMPARISON A

def test_03():
    cvt = Converter(translations={
        "hello": "foo",
        "world": "bar"
    })
    assert cvt.translate("hello world") == "foo bar"
    assert cvt.translate("hello world!") == "foo bar!"
