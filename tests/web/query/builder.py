from pprint import pprint as print

from biothings.web.query.builder import ESQueryBuilder, MongoQueryBuilder, SQLQueryBuilder


def test_sql():
    builder = SQLQueryBuilder(
        {
            "album": "album",
            "track": "track",
        }
    )
    print(
        builder.build(
            "term",
            scopes=["fieldA", "fieldB"],
            biothing_type="track",
        )
    )
    print(
        builder.build(
            "term",
            scopes=["fieldA"],
            _source=["id", "fieldA"],
        )
    )
    print(builder.build("term", size=10, from_=10))
    print(builder.build("fieldA:termB"))


def test_mongo():
    builder = MongoQueryBuilder()
    print(builder.build("term"))
    print(builder.build("fieldA:term"))
    print(builder.build("term", scopes=["fieldA", "fieldB"]))
    print(builder.build("term", scopes=["fieldA"], _source=["_id", "fieldA"]))


def test_es():
    builder = ESQueryBuilder()

    print(builder.build().to_dict())  # match_all
    print(builder.build("").to_dict())  # match_none
    print(builder.build("term").to_dict())  # query_string
    print(builder.build("term", scopes=None).to_dict())  # query_string, same as above
    print(builder.build("term", scopes=[]).to_dict())  # query_string, same as above
    print(builder.build(["A"]).to_dict())  # mutlisearch, query_string
    print(builder.build(["A", "B"]).to_dict())  # multisearch, query_string

    print(builder.build("A", scopes=["scope1"]).to_dict())  # match
    print(builder.build("A", scopes="scope1").to_dict())  # match
    print(builder.build(["A"], scopes=["scope1"]).to_dict())  # multisearch, match
    print(builder.build(["A"], scopes="scope1").to_dict())  # multisearch, match
    print(builder.build([["A"]], scopes=[["scope1"]]).to_dict())  # multisearch, one query, match
    print(builder.build(["A", "B"], scopes=["scope1", "scope2"]).to_dict())  # multisearch, match
    print(
        builder.build([["A", "B"], ["C", "D"]], scopes=["scope1", ["S2", "S3"]]).to_dict()
    )  # multisearch, compound match
    query = builder.build("A", scopes=["scope1"], _source=["_id", "fieldA", "-fieldB", "-*.description"]).to_dict()
    print(query)
    assert "fieldB" in query["_source"]["excludes"]
    assert "*.description" in query["_source"]["excludes"]
    assert "_id" in query["_source"]["includes"]
    assert "fieldA" in query["_source"]["includes"]

def test_regex_order


if __name__ == "__main__":
    test_es()
