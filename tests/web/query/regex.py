import re

from biothings.web.query.builder import QStringParser as Parser


def test_01():
    parser = Parser()
    print(parser.parse("x"))


def test_02():  # mygene
    parser = Parser(("_id", "*"), [((r"^\d+$"), ["entrezgene", "retired"])])
    print(parser.parse("12345"))
    print(parser.parse("x"))


def test_03():  # mychem
    parser = Parser(
        patterns=[
            (re.compile(r"db[0-9]+", re.I), "drugbank.id"),
            (re.compile(r"chembl[0-9]+", re.I), "chembl.molecule_chembl_id"),
            (re.compile(r"chebi\:[0-9]+", re.I), ["chebi.id", "chebi.secondary_chebi_id"]),
            (re.compile(r"[A-Z0-9]{10}"), "unii.unii"),
            (re.compile(r"((cid\:(?P<term>[0-9]+))|([0-9]+))", re.I), "pubchem.cid"),
        ]
    )
    print(parser.parse("x"))
    print(parser.parse("db123"))
    print(parser.parse("chembl123"))
    print(parser.parse("chebi:123"))
    print(parser.parse("34F916N28Z"))
    print(parser.parse("118415428"))
    print(parser.parse("cid:118415428"))


def test_04():  # generic
    parser = Parser()
    print(parser.parse("x"))
    print(parser.parse("a:b"))
    print(parser.parse("_id:123"))
    print(parser.parse("a:b:c"))


if __name__ == "__main__":
    test_04()
