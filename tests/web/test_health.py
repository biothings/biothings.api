import asyncio

import elasticsearch
import pytest

from biothings.web import connections
from biothings.web.services.health import ESHealth


def test_localhost_health_check():
    client = connections.get_es_client("http://localhost:9200", True)
    health = ESHealth(client)

    async def main():
        response = await health.async_check()
        assert response["success"]

    asyncio.run(main())

def test_nonexistant_index_failure():
    client = connections.get_es_client("http://localhost:9200", True)
    health = ESHealth(
        client,
        {
            "index": "nonexists",
            "id": "1017",
        },
    )

    async def main():
        with pytest.raises(elasticsearch.NotFoundError):
            response = await health.async_check()

    asyncio.run(main())
