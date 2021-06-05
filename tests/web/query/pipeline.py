

from biothings.web.query import *
from biothings.web import connections


def test_mongo():
    client = connections.get_mongo_client("mongodb://su05:27017/genedoc")
    pipeline = MongoQueryPipeline(
        MongoQueryBuilder(),
        MongoQueryBackend(client, {
            None: "demo_allspecies_20191111_n2o6r9ax",
            "old": "mygene_allspecies_20210510_yqynv8db",
            "new": "mygene_allspecies_20210517_04usbghm"
        }),
        MongoResultFormatter()
    )
    fields = ["_id", "name", "symbol"]
    print(pipeline.fetch("100004228", _source=fields))
    print(pipeline.search("slc27a2b", scopes=["symbol"], _source=fields))

def test_es():
    client = connections.get_es_client("localhost:9200")
    pipeline = ESQueryPipeline(
        ESQueryBuilder(),
        ESQueryBackend(client),
        ESResultFormatter()
    )
    # print(pipeline.fetch("ecf3767159a74988", rawquery=1))
    # print(pipeline.fetch("ecf3767159a74988", _source=['_*']))
    # print(pipeline.fetch("nonexists"))
    # print(pipeline.search("infection", scopes=["name"], _source=['_*', 'name']))
    print(pipeline.search("nonexists", scopes=["name"]))


def test_sql():
    client = connections.get_sql_client("mysql+pymysql://root@localhost/album")
    pipeline = SQLQueryPipeline(
        SQLQueryBuilder({
            None: 'album',
            'album': 'album',
            'track': 'album JOIN track ON album.id = track.album_id',
        }, ('id', )),
        SQLQueryBackend(client),
        SQLResultFormatter()
    )
    print(pipeline.fetch('1'))
    print(pipeline.search('Two Men with the Blues', scopes=['title']))
    print(pipeline.search('1', biothing_type='track', scopes=['album.id']))


if __name__ == '__main__':
    test_mongo()
