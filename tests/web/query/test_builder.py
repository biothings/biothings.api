import pprint

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
