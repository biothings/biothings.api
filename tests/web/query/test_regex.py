"""
Tests the QStringParser parsing capabilities for various different queries
"""

import logging
import re
from typing import Union

import pytest

from biothings.web.query.builder import ESQueryBuilder, QStringParser, Query
from biothings.web.settings.default import ANNOTATION_DEFAULT_REGEX_PATTERN


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
            ("a:b:c", Query(term="b:c", scopes=["a"])),
        ],
    )
    def test_generic_entries(self, query: str, expected_result: Query):
        """
        Tests miscellaneous entries with the default parser instance
        """
        parser_result = self.parser.parse(query, None)
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
            ("a:b:c", Query(term="b:c", scopes=["a"])),
        ],
    )
    def test_default_builder_parser(self, query: str, expected_result: Query):
        """
        Comparison between the default parser for the ESQueryBuilder and the QStringParser
        itself to ensure we get similar results from the defaults
        """
        external_parser_result = self.parser.parse(query, None)
        internal_parser_result = self.builder.parser.parse(query, None)

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
        parser_result = self.parser.parse(query, None)
        assert isinstance(parser_result, Query)
        assert parser_result == expected_result
        assert parser_result.term == expected_result.term
        assert parser_result.scopes == expected_result.scopes


class TestGeneCurieQuery:
    @classmethod
    def setup_class(cls):
        BIOLINK_MODEL_PREFIX_BIOTHINGS_GENE_MAPPING = {
            "NCBIGene": {"type": "gene", "field": ["entrezgene", "retired"]},
            "ENSEMBL": {"type": "gene", "field": "ensembl.gene"},
            "UniProtKB": {"type": "gene", "field": "uniprot.Swiss-Prot"},
        }
        parser_patterns = []
        for (
            biolink_prefix,
            mapping,
        ) in BIOLINK_MODEL_PREFIX_BIOTHINGS_GENE_MAPPING.items():
            expression = re.compile(rf"({biolink_prefix}):(?P<term>[^:]+)", re.I)
            field_match = mapping["field"]
            pattern = (expression, field_match)
            parser_patterns.append(pattern)

        fallback_pattern = (re.compile(r"^\d+$"), ["entrezgene", "retired"])
        parser_patterns.append(fallback_pattern)
        default_pattern = (re.compile(r"(?P<scope>[\w\W]+):(?P<term>[^:]+)"), [])
        parser_patterns.append(default_pattern)

        cls.parser = QStringParser(
            default_scopes=("_id",),
            patterns=parser_patterns,
            gpnames=("term", "scope"),
        )

    @pytest.mark.parametrize(
        "query, expected_result",
        [
            ("entrezgene:1017", Query(term="1017", scopes=["entrezgene"])),
            ("NCBIGENE:1017", Query(term="1017", scopes=["entrezgene", "retired"])),
            ("ncbigene:1017", Query(term="1017", scopes=["entrezgene", "retired"])),
            ("ensembl.gene:ENSG00000123374", Query(term="ENSG00000123374", scopes=["ensembl.gene"])),
            ("ENSEMBL:ENSG00000123374", Query(term="ENSG00000123374", scopes=["ensembl.gene"])),
            ("uniprot.Swiss-Prot:P47804", Query(term="P47804", scopes=["uniprot.Swiss-Prot"])),
            ("UniProtKB:P47804", Query(term="P47804", scopes=["uniprot.Swiss-Prot"])),
        ],
    )
    def test_curie_id_queries(self, query: str, expected_result: Query):
        """
        Tests various CURIE ID based queries targetting the types of queries we'd expect
        to see with the mygene instance
        """
        parser_result = self.parser.parse(query, None)
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
        parser_result = self.parser.parse(query, None)
        assert isinstance(parser_result, Query)
        assert parser_result == expected_result
        assert parser_result.term == expected_result.term
        assert parser_result.scopes == expected_result.scopes


class TestRegexPatternOrdering:
    @classmethod
    def setup_class(cls):
        cls.parser = QStringParser(default_scopes=None, patterns=None, gpnames=None)

    @pytest.mark.parametrize(
        "regex_patterns",
        [
            ((r"fake_regex", ["fake_field"]),),
            ((re.compile(r"fake_regex"), "fake_field"),),
            [(re.compile(r"fake_regex"), ["fake_field"])],
            ((re.compile(r"^\d+$"), ["entrezgene", "retired"]),),
            [(re.compile(r"^\d+$"), ["entrezgene", "retired"])],
            [
                (re.compile(r"db[0-9]+", re.I), "drugbank.id"),
                ANNOTATION_DEFAULT_REGEX_PATTERN,
                (re.compile(r"chembl[0-9]+", re.I), "chembl.molecule_chembl_id"),
                (re.compile(r"chebi\:[0-9]+", re.I), ["chebi.id", "chebi.secondary_chebi_id"]),
                (re.compile(r"[A-Z0-9]{10}"), "unii.unii"),
                (re.compile(r"((cid\:(?P<term>[0-9]+))|([0-9]+))", re.I), "pubchem.cid"),
            ],
            [
                (r"db[0-9]+", "drugbank.id"),
                ANNOTATION_DEFAULT_REGEX_PATTERN,
                (r"chembl[0-9]+", "chembl.molecule_chembl_id"),
                (re.compile(r"chebi\:[0-9]+", re.I), ["chebi.id", "chebi.secondary_chebi_id"]),
                (r"[A-Z0-9]{10}", "unii.unii"),
                (re.compile(r"((cid\:(?P<term>[0-9]+))|([0-9]+))", re.I), "pubchem.cid"),
            ],
            [
                (re.compile(r"rs[0-9]+", re.I), "dbsnp.rsid"),
                (re.compile(r"rcv[0-9\.]+", re.I), "clinvar.rcv.accession"),
                (re.compile(r"var_[0-9]+", re.I), "uniprot.humsavar.ftid"),
            ],
            [
                ANNOTATION_DEFAULT_REGEX_PATTERN,
                (re.compile(r"fake_regex"), ["fake_field"]),
                ANNOTATION_DEFAULT_REGEX_PATTERN,
            ],
            [
                ANNOTATION_DEFAULT_REGEX_PATTERN,
                (r"(?P<scope>\W\w+):(?P<term>[^:]+)", []),
                ANNOTATION_DEFAULT_REGEX_PATTERN,
            ],
        ],
    )
    def test_default_ordering(self, regex_patterns: Union[list, tuple]):
        """
        Ensure that no matter the type of ordering we pass we always have the default
        regex pattern as the last one in the ordering
        """
        logger.info(f"Verifying regex pattern collection: {regex_patterns}")
        processed_regex_patterns = self.parser._build_regex_pattern_collection(patterns=regex_patterns)

        default_regex_pattern = ANNOTATION_DEFAULT_REGEX_PATTERN[0]
        for regex_pattern, regex_fields in processed_regex_patterns[:-1]:
            assert isinstance(regex_pattern, re.Pattern)
            assert isinstance(regex_fields, (list, tuple))
            assert not regex_pattern == default_regex_pattern

        last_regex_pattern = processed_regex_patterns[-1]
        assert isinstance(last_regex_pattern[0], re.Pattern)
        assert isinstance(last_regex_pattern[1], (list, tuple))
        assert last_regex_pattern[0] == default_regex_pattern
        assert last_regex_pattern[1] == []
