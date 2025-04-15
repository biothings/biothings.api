import asyncio

import pytest

from biothings.web import connections
from biothings.web.services.health import ESHealth


def test_es_async_1():
    client = connections.get_es_client("http://localhost:9200", True)
    health = ESHealth(client)

    async def main():
        expected_response = {"success": True, "status": "green"}
        response = await health.async_check()
        assert response == expected_response

    asyncio.run(main())


@pytest.mark.xfail(reason="elasticsearch index setup required for index `bts_test`")
def test_es_async_2():
    client = connections.get_es_client("http://localhost:9200", True)
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


@pytest.mark.xfail(reason="need exception handling for expected missing index `nonexists`")
def test_es_async_3():
    client = connections.get_es_client("http://localhost:9200", True)
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
