import asyncio
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
    connections.es.get_async_client("http://localhost:9200")
    loop = asyncio.get_event_loop()
