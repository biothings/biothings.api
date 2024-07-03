import logging

from biothings.web import connections

logging.basicConfig(level="DEBUG")


def test_es_1():
    client = connections.es.get_client("http://localhost:9200")
    print(client.info())


def test_es_2():  # see if the client is reused
    client1 = connections.es.get_client("http://localhost:9200")
    client2 = connections.es.get_client("http://localhost:9200", timeout=20)
    client3 = connections.es.get_client("http://localhost:9200", timeout=20)
    print(id(client1))
    print(id(client2))
    print(id(client3))


def test_es_3():  # async
    import asyncio

    connections.es.get_async_client("http://localhost:9200")
    loop = asyncio.get_event_loop()
    loop.run_forever()


def test_es_4():
    import asyncio

    connections.es.get_async_client("http://localhost:9200")  # es7
    connections.es.get_async_client("http://localhost:9201")  # es6
    loop = asyncio.get_event_loop()
    loop.run_forever()


def test_mongo():
    client = connections.mongo.get_client("mongodb://su05:27017/genedoc")
    collection = client["mygene_allspecies_20210510_yqynv8db"]
    print(next(collection.find()))


def test_sql():
    client = connections.sql.get_client("mysql+pymysql://<USER>:<PASSWORD>@localhost/album")
    result = client.execute("SELECT * FROM track")
    print(result.all())


if __name__ == "__main__":
    test_es_4()
