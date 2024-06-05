import aiohttp
import orjson
import pytest

from aioresponses import aioresponses
from biothings.web.analytics.channels import GA4Channel, GAChannel
from biothings.web.analytics.events import GAEvent


@pytest.mark.asyncio
async def test_send_GA():
    event = GAEvent(
        {
            "__request__": {
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": None,
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            },
            "category": "test",
            "action": "play",
            "label": "sample.mp4",
            "value": 60,
        }
    )
    channel = GAChannel("G-XXXXXX", 2)
    assert await channel.handles(event)

    with aioresponses() as responses:
        # Mock the URL to return a 200 OK response
        responses.post(channel.url, status=200)

        # If the function completes without raising an exception, the test will pass
        await channel.send(event)


@pytest.mark.asyncio
async def test_send_GA4():
    event = GAEvent(
        {
            "__request__": {
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": None,
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            },
            "category": "test",
            "action": "play",
            "label": "sample.mp4",
            "value": 60,
        }
    )
    channel = GA4Channel("GA4_MEASUREMENT_ID", "GA4_API_SECRET", 1)
    assert await channel.handles(event)

    with aioresponses() as responses:
        # Mock the URL to return a 200 OK response
        responses.post(channel.url, status=200)

        # If the function completes without raising an exception, the test will pass
        await channel.send(event)


@pytest.mark.asyncio
async def test_send_GA4_request_retries():
    channel = GA4Channel("G-XXXXXX", "SECRET")
    url = channel.url
    data = orjson.dumps({"test": "data"})

    async with aiohttp.ClientSession() as session:
        with aioresponses() as responses:
            # Mock the URL to return HTTP 500 on the first call and HTTP 200 on the second call
            responses.post(url, status=500)
            responses.post(url, status=200)

            await channel.send_request(session, url, data)

            assert responses._responses[0].status == 500
            assert responses._responses[1].status == 200


@pytest.mark.asyncio
async def test_send_GA4_request_max_retries():
    channel = GA4Channel("G-XXXXXX", "SECRET")
    url = channel.url
    data = orjson.dumps({"test": "data"})

    async with aiohttp.ClientSession() as session:
        with aioresponses() as responses:
            # Mock the URL to always return a 500 response
            responses.post(url, status=500)
            responses.post(url, status=500)

            with pytest.raises(Exception, match="GA4Channel: Maximum retries reached. Unable to complete request."):
                await channel.send_request(session, url, data)

            # Ensure the post method was called max_retries + 1 times
            assert len(responses._responses) == channel.max_retries + 1


if __name__ == "__main__":
    test_send_GA()
    test_send_GA4()
    test_send_GA4_request_retries()
    test_send_GA4_request_max_retries()
