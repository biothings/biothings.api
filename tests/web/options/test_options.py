import pytest

from biothings.web.options import Option, OptionError, ReqArgs


def test_01():
    reqargs = (
        ReqArgs.Path(
            args=("gene", "1017"),
            kwargs={"host": "mygene.info", "version": "v3"},
        ),
        {
            "size": "10",
            "dotfield": "true",
            "format": "json",
        },
    )

    opt = Option({"keyword": "doc_type", "path": 0})
    assert opt.parse(reqargs) == "gene"

    opt = Option({"keyword": "gene_id", "type": int, "path": 1})
    assert opt.parse(reqargs) == 1017

    opt = Option({"keyword": "host", "path": "host"})
    assert opt.parse(reqargs) == "mygene.info"

    opt = Option({"keyword": "host", "path": 100})
    assert opt.parse(reqargs) is None

    opt = Option({"keyword": "size", "type": int})
    assert opt.parse(reqargs) == 10

    opt = Option({"keyword": "dotfield", "type": bool, "default": False})
    assert opt.parse(reqargs) is True

    opt = Option({"keyword": "from", "type": int, "default": 0})
    assert opt.parse(reqargs) == 0

    opt = Option({"keyword": "userquery", "type": str})
    assert opt.parse(reqargs) is None


def test_02():
    reqargs = ReqArgs(
        query={
            "size": "10",
            "format": "html",
        },
        json_={
            "q": "cdk2",
            "scopes": ["ensembl", "entrez"],
            "format": "json",
        },
    )

    opt = Option({"keyword": "size", "type": int})
    assert opt.parse(reqargs) == 10

    with pytest.raises(OptionError):
        opt = Option({"keyword": "size", "type": int, "max": 3})
        opt.parse(reqargs)

    opt = Option({"keyword": "q"})
    assert opt.parse(reqargs) == "cdk2"

    opt = Option({"keyword": "q", "type": str})
    assert opt.parse(reqargs) == "cdk2"

    with pytest.raises(OptionError):
        opt = Option({"keyword": "q", "type": list})
        opt.parse(reqargs)

    opt = Option({"keyword": "q", "type": list, "strict": False})
    assert opt.parse(reqargs) == ["cdk2"]

    opt = Option({"keyword": "format", "type": str})
    assert opt.parse(reqargs) == "html"

    opt = Option({"keyword": "out_format", "alias": "format"})
    assert opt.parse(reqargs) == "html"

    opt = Option({"keyword": "format", "location": ("json", "query")})
    assert opt.parse(reqargs) == "json"

    opt = Option({"keyword": "format", "location": "json"})
    assert opt.parse(reqargs) == "json"

    with pytest.raises(OptionError):
        opt = Option({"keyword": "scopes", "type": str})
        opt.parse(reqargs)

    opt = Option({"keyword": "scopes", "type": str, "strict": False})
    assert opt.parse(reqargs) == "['ensembl', 'entrez']"

    opt = Option({"keyword": "scopes", "type": list})
    assert opt.parse(reqargs) == ["ensembl", "entrez"]


def test_03():
    reqargs = ReqArgs(
        form={
            "ids": "cdk,cdk2",
            "scopes": "symbol",
            "size": 10,
        }
    )

    opt = Option({"keyword": "scopes", "type": list})
    assert opt.parse(reqargs) == ["symbol"]

    opt = Option({"keyword": "ids", "type": list})
    assert opt.parse(reqargs) == ["cdk", "cdk2"]

    opt = Option({"keyword": "size", "type": int})
    assert opt.parse(reqargs) == 10
