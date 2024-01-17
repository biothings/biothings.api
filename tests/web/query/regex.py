"""
Tests the QStringParser parsing capabilities for various different queries
"""

import re

import pytest

from biothings.web.query.builder import ESQueryBuilder, QStringParser, Query


class TestDefaultQStringParser:
    @classmethod
    def setup_class(cls):
        cls.parser = QStringParser()
        cls.builder = ESQueryBuilder()

    @pytest.mark.parametrize(
        "query, expected_result",
        [
            ("x", Query(term="x", scopes=("_id",))),
            ("a:b", Query(term="b", scopes=["a"])),
            ("_id:123", Query(term="123", scopes=["_id"])),
            ("a:b:c", Query(term="a:b:c", scopes=("_id",))),
        ],
    )
    def test_generic_entries(self, query: str, expected_result: Query):
        """
        Tests miscellaneous entries with the default parser instance
        """
        parser_result = self.parser.parse(query)
        assert isinstance(parser_result, Query)
        assert parser_result == expected_result
        assert parser_result.term == expected_result.term
        assert parser_result.scopes == expected_result.scopes

    @pytest.mark.parametrize(
        "query, expected_result",
        [
            ("x", Query(term="x", scopes=("_id",))),
            ("a:b", Query(term="b", scopes=["a"])),
            ("_id:123", Query(term="123", scopes=["_id"])),
            ("a:b:c", Query(term="a:b:c", scopes=("_id",))),
        ],
    )
    def test_default_builder_parser(self, query:str, expected_result: Query):
        """
        Comparison between the default parser for the ESQueryBuilder and the QStringParser
        itself to ensure we get similar results from the defaults
        """
        external_parser_result = self.parser.parse(query)
        internal_parser_result = self.builder.parser.parse(query)

        assert isinstance(external_parser_result, Query)
        assert isinstance(internal_parser_result, Query)

        assert internal_parser_result == expected_result
        assert internal_parser_result.term == expected_result.term
        assert internal_parser_result.scopes == expected_result.scopes

        assert external_parser_result == expected_result
        assert external_parser_result.term == expected_result.term
        assert external_parser_result.scopes == expected_result.scopes


class TestSingularPatternQStringParser:
    @classmethod
    def setup_class(cls):
        cls.parser = QStringParser(
            default_scopes=("_id", "*"),
            patterns=[((r"^\d+$"), ["entrezgene", "retired"])],
            gpnames=("term", "scope"),
        )

    @pytest.mark.parametrize(
        "query, expected_result",
        [("x", Query(term="x", scopes=("_id", "*"))), ("12345", Query(term="12345", scopes=["entrezgene", "retired"]))],
    )
    def test_generic_entries(self, query: str, expected_result: Query):
        """
        Tests miscellaneous entries with a singular pattern set for the parser instance
        Representative of mygene queries
        """
        parser_result = self.parser.parse(query)
        assert isinstance(parser_result, Query)
        assert parser_result == expected_result
        assert parser_result.term == expected_result.term
        assert parser_result.scopes == expected_result.scopes


class TestMultiplerPatternQStringParser:
    @classmethod
    def setup_class(cls):
        cls.parser = QStringParser(
            default_scopes=("_id",),
            patterns=[
                (re.compile(r"db[0-9]+", re.I), "drugbank.id"),
                (re.compile(r"chembl[0-9]+", re.I), "chembl.molecule_chembl_id"),
                (re.compile(r"chebi\:[0-9]+", re.I), ["chebi.id", "chebi.secondary_chebi_id"]),
                (re.compile(r"[A-Z0-9]{10}"), "unii.unii"),
                (re.compile(r"((cid\:(?P<term>[0-9]+))|([0-9]+))", re.I), "pubchem.cid"),
            ],
            gpnames=("term", "scope"),
        )

    @pytest.mark.parametrize(
        "query, expected_result",
        [
            ("x", Query(term="x", scopes=("_id",))),
            ("db12", Query(term="db12", scopes=["drugbank.id"])),
            ("chembl123", Query(term="chembl123", scopes=["chembl.molecule_chembl_id"])),
            ("chebi:123", Query(term="chebi:123", scopes=["chebi.id", "chebi.secondary_chebi_id"])),
            ("34F916N28Z", Query(term="34F916N28Z", scopes=["unii.unii"])),
            ("118415428", Query(term="118415428", scopes=["pubchem.cid"])),
            ("cid:118415428", Query(term="118415428", scopes=["pubchem.cid"])),
        ],
    )
    def test_generic_entries(self, query: str, expected_result: Query):
        """
        Tests miscellaneous entries with multiple patterns set for the parser instance
        Representative of mychem queries
        """
        parser_result = self.parser.parse(query)
        assert isinstance(parser_result, Query)
        assert parser_result == expected_result
        assert parser_result.term == expected_result.term
        assert parser_result.scopes == expected_result.scopes
