import pytest

from biothings.web import connections
from biothings.web.query import (
    ESQueryBackend,
    ESQueryBuilder,
    ESQueryPipeline,
    ESResultFormatter,
    MongoQueryBackend,
    MongoQueryBuilder,
    MongoQueryPipeline,
    MongoResultFormatter,
)


@pytest.mark.xfail(reason="Backend setup required for pipeline testing")
def test_mongodb_pipeline():
    client = connections.get_mongo_client("mongodb://localhost:27017")
    query_builder = MongoQueryBuilder()

    indices = {
        None: "demo_allspecies_20191111_n2o6r9ax",
        "old": "mygene_allspecies_20210510_yqynv8db",
        "new": "mygene_allspecies_20210517_04usbghm",
    }
    query_backend = MongoQueryBackend(client, indices)
    query_formatter = MongoResultFormatter()

    pipeline = MongoQueryPipeline(query_builder, query_backend, query_formatter)
    fields = ["_id", "name", "symbol"]
    print(pipeline.fetch("100004228", _source=fields))
    print(pipeline.search("slc27a2b", scopes=["symbol"], _source=fields))


@pytest.mark.xfail(reason="Backend setup required for pipeline testing")
def test_elasticsearch_pipeline():
    client = connections.get_es_client("http://localhost:9200", True)
    pipeline = ESQueryPipeline(ESQueryBuilder(), ESQueryBackend(client), ESResultFormatter())
    print(pipeline.fetch("ecf3767159a74988", rawquery=1))
    print(pipeline.fetch("ecf3767159a74988", _source=["_*"]))
    print(pipeline.fetch("nonexists"))
    print(pipeline.search("infection", scopes=["name"], _source=["_*", "name"]))
    print(pipeline.search("nonexists", scopes=["name"]))
