import pprint

import pytest

from biothings.web.query.builder import ESQueryBuilder, MongoQueryBuilder, SQLQueryBuilder


def test_sqlite3_querybuilder():
    builder = SQLQueryBuilder(
        {
            "album": "album",
            "track": "track",
        }
    )
    pprint.pprint(
        builder.build(
            "term",
            scopes=["fieldA", "fieldB"],
            biothing_type="track",
        )
    )
    pprint.pprint(
        builder.build(
            "term",
            scopes=["fieldA"],
            _source=["id", "fieldA"],
        )
    )
    pprint.pprint(builder.build("term", size=10, from_=10))
    pprint.pprint(builder.build("fieldA:termB"))


def test_mongodb_query_builder():
    builder = MongoQueryBuilder()
    pprint.pprint(builder.build("term"))
    pprint.pprint(builder.build("fieldA:term"))
    pprint.pprint(builder.build("term", scopes=["fieldA", "fieldB"]))
    pprint.pprint(builder.build("term", scopes=["fieldA"], _source=["_id", "fieldA"]))


def test_elasticsearch_querybuilder():
    builder = ESQueryBuilder()

    pprint.pprint(builder.build().to_dict())  # match_all
    pprint.pprint(builder.build("").to_dict())  # match_none
    pprint.pprint(builder.build("term").to_dict())  # query_string
    pprint.pprint(builder.build("term", scopes=None).to_dict())  # query_string, same as above
    pprint.pprint(builder.build("term", scopes=[]).to_dict())  # query_string, same as above
    pprint.pprint(builder.build(["A"]).to_dict())  # mutlisearch, query_string
    pprint.pprint(builder.build(["A", "B"]).to_dict())  # multisearch, query_string

    pprint.pprint(builder.build("A", scopes=["scope1"]).to_dict())  # match
    pprint.pprint(builder.build("A", scopes="scope1").to_dict())  # match
    pprint.pprint(builder.build(["A"], scopes=["scope1"]).to_dict())  # multisearch, match
    pprint.pprint(builder.build(["A"], scopes="scope1").to_dict())  # multisearch, match
    pprint.pprint(builder.build([["A"]], scopes=[["scope1"]]).to_dict())  # multisearch, one query, match
    pprint.pprint(builder.build(["A", "B"], scopes=["scope1", "scope2"]).to_dict())  # multisearch, match
    pprint.pprint(
        builder.build([["A", "B"], ["C", "D"]], scopes=["scope1", ["S2", "S3"]]).to_dict()
    )  # multisearch, compound match
    query = builder.build("A", scopes=["scope1"], _source=["_id", "fieldA", "-fieldB", "-*.description"]).to_dict()
    pprint.pprint(query)
    assert "fieldB" in query["_source"]["excludes"]
    assert "*.description" in query["_source"]["excludes"]
    assert "_id" in query["_source"]["includes"]
    assert "fieldA" in query["_source"]["includes"]


def test_elasticsearch_querybuilder_sort():
    builder = ESQueryBuilder()

    # valid: plain (ascending) and '-' prefixed (descending) fields
    query = builder.build("term", sort=["symbol", "-taxid", "-ensembl.gene"]).to_dict()
    assert query["sort"] == ["symbol", {"taxid": {"order": "desc"}}, {"ensembl.gene": {"order": "desc"}}]

    # invalid: the Elasticsearch-style 'field:order' syntax is rejected with a
    # helpful message instead of surfacing an opaque ES shard error.
    with pytest.raises(ValueError) as exc_info:
        builder.build("term", sort=["_score:desc"])
    message = str(exc_info.value)
    assert "field:order" in message
    # '_score' must not be suggested with a '-' prefix, that form is rejected too
    assert "sort=_score" in message

    with pytest.raises(ValueError) as exc_info:
        builder.build("term", sort=["symbol:asc"])
    assert "sort=symbol" in str(exc_info.value)

    # invalid: a lone '-' with no field name
    with pytest.raises(ValueError):
        builder.build("term", sort=["-"])

    # '-_score' is rejected by _validate_sort before elasticsearch-dsl sees it,
    # so no IllegalOperation (and no ERROR-level log) is produced.
    with pytest.raises(ValueError) as exc_info:
        builder.build("term", sort=["-_score"])
    message = str(exc_info.value)
    assert "descending order" in message
    assert "sort=_score" in message

    # plain '_score' remains valid (ES sorts relevance descending by default)
    query = builder.build("term", sort=["_score", "-taxid"]).to_dict()
    assert query["sort"] == ["_score", {"taxid": {"order": "desc"}}]

    # sorting on '_id' is rejected up front: ES disallows fielddata on _id, so
    # this would otherwise surface as an opaque shard error. Reject '_id',
    # '-_id', '_id:desc', and '_id' anywhere in the list.
    for sort in (["_id"], ["-_id"], ["_id:desc"], ["taxid", "-_id"]):
        with pytest.raises(ValueError) as exc_info:
            builder.build("term", sort=sort)
        assert "_id" in str(exc_info.value)
        assert "not available for sorting or aggregation" in str(exc_info.value)


def test_elasticsearch_querybuilder_aggs_reject_id():
    # aggregating on '_id' requires fielddata, which ES disallows on _id.
    # Reject it up front (both top-level and nested aggregations).
    builder = ESQueryBuilder()
    with pytest.raises(ValueError) as exc_info:
        builder.build("term", aggs=["_id"])
    assert "not available for sorting or aggregation" in str(exc_info.value)

    nested_builder = ESQueryBuilder(allow_nested_query=True)
    with pytest.raises(ValueError) as exc_info:
        nested_builder.build("term", aggs=["taxid(_id)"])
    assert "not available for sorting or aggregation" in str(exc_info.value)

    # a normal aggregation is unaffected
    assert "aggs" in builder.build("term", aggs=["taxid"]).to_dict()


class _FakeMetadata:
    """Stands in for BiothingsESMetadata, which reads the map from index _meta."""

    ALIASES = {
        None: {"gnomad_genome": "gnomad_genome.chrom", "dbnsfp": "dbnsfp.alt"},
        "hg38": {"gnomad_genome": "gnomad_genome.alt"},
    }

    def __init__(self, aliases=None):
        self.aliases = self.ALIASES if aliases is None else aliases

    def get_exists_aliases(self, biothing_type):
        return self.aliases.get(biothing_type, {})


class TestExistsAliasRewrite:
    """
    '_exists_:<object field>' is expanded by ES into a disjunction over every
    leaf below the object, which is orders of magnitude more expensive than an
    exists query on one subfield that every matching document has. The hub
    records which subfield qualifies in the index _meta; the builder looks it
    up and substitutes it.
    """

    @staticmethod
    def _query_string(q, aliases=None, **options):
        builder = ESQueryBuilder(metadata=_FakeMetadata(aliases))
        return builder.build(q, **options).to_dict()["query"]["query_string"]["query"]

    @pytest.mark.parametrize(
        "q, expected",
        [
            ("_exists_:gnomad_genome", "_exists_:gnomad_genome.chrom"),
            # several occurrences in one query string
            (
                "_exists_:gnomad_genome AND _exists_:dbnsfp",
                "_exists_:gnomad_genome.chrom AND _exists_:dbnsfp.alt",
            ),
            # ... including through negation and grouping
            (
                "chr1:100-200 AND NOT (_exists_:gnomad_genome OR _exists_:dbnsfp)",
                "chr1:100-200 AND NOT (_exists_:gnomad_genome.chrom OR _exists_:dbnsfp.alt)",
            ),
            # no recorded alias: left exactly as it was
            ("_exists_:cadd", "_exists_:cadd"),
            # matching is on the whole field name, so a nested field does not
            # inherit its parent's alias
            ("_exists_:gnomad_genome.af", "_exists_:gnomad_genome.af"),
            # and only directly after '_exists_:'
            ("gnomad_genome:1", "gnomad_genome:1"),
            ("_missing_:gnomad_genome", "_missing_:gnomad_genome"),
        ],
        ids=[
            "single",
            "multiple_occurrences",
            "negation_and_grouping",
            "no_alias_recorded",
            "nested_field_not_inherited",
            "not_an_exists_clause",
            "not_the_exists_keyword",
        ],
    )
    def test_rewrite(self, q, expected):
        assert self._query_string(q) == expected

    def test_map_is_per_biothing_type(self):
        # each index gets its own build, so each has its own map
        assert self._query_string("_exists_:gnomad_genome") == "_exists_:gnomad_genome.chrom"
        assert self._query_string("_exists_:gnomad_genome", biothing_type="hg38") == "_exists_:gnomad_genome.alt"
        assert self._query_string("_exists_:dbnsfp", biothing_type="hg38") == "_exists_:dbnsfp"

    def test_filter_and_post_filter_are_query_strings_too(self):
        builder = ESQueryBuilder(metadata=_FakeMetadata())
        query = builder.build("term", filter="_exists_:gnomad_genome", post_filter="_exists_:dbnsfp").to_dict()

        assert query["query"]["bool"]["filter"][0]["query_string"]["query"] == "_exists_:gnomad_genome.chrom"
        assert query["post_filter"]["query_string"]["query"] == "_exists_:dbnsfp.alt"

    @pytest.mark.parametrize(
        "builder",
        [
            ESQueryBuilder(),
            ESQueryBuilder(metadata=_FakeMetadata({})),
            ESQueryBuilder(metadata=_FakeMetadata({None: {}})),
        ],
        ids=["no_metadata_service", "no_map_for_any_type", "empty_map"],
    )
    def test_nothing_is_rewritten_without_a_map(self, builder):
        query = builder.build("_exists_:gnomad_genome").to_dict()
        assert query["query"]["query_string"]["query"] == "_exists_:gnomad_genome"

    @pytest.mark.parametrize(
        "q, expected_clause",
        [(None, "match_all"), ("", "match_none")],
        ids=["match_all", "match_none"],
    )
    def test_non_string_queries_are_untouched(self, q, expected_clause):
        # the rewrite must not disturb the __all__/empty-q paths
        assert expected_clause in ESQueryBuilder(metadata=_FakeMetadata()).build(q).to_dict()["query"]
