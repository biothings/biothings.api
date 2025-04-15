import asyncio

from biothings.web import connections
from biothings.web.services.health import ESHealth


def test_es_async_1():
    client = connections.get_es_client("localhost:9200", True)
    health = ESHealth(client)

    async def main():
        response = await health.async_check()
        print(vars(response))

    asyncio.run(main())


def test_es_async_2():
    client = connections.get_es_client("localhost:9200", True)
    health = ESHealth(
        client,
        {
            "index": "bts_test",
            "id": "1017",
            "_source": ["taxid", "symbol"],
        },
    )

    async def main():
        response = await health.async_check()
        print(vars(response))

    asyncio.run(main())


def test_es_async_3():
    client = connections.get_es_client("localhost:9200", True)
    health = ESHealth(
        client,
        {
            "index": "nonexists",
            "id": "1017",
        },
    )

    async def main():
        response = await health.async_check()
        print(vars(response))

    asyncio.run(main())
