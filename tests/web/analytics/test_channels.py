import asyncio
from unittest.mock import AsyncMock, call, patch

import aiohttp
import pytest

from biothings.utils import serializer
from biothings.web.analytics.channels import GA4Channel, SlackChannel
from biothings.web.analytics.events import GAEvent, Message


class MockClientResponse:
    def __init__(self, status):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False


@pytest.mark.asyncio
async def test_send_Slack():
    message = Message()
    url = "http://example.com"
    channel = SlackChannel([url])

    assert await channel.handles(message)

    with patch(
        "biothings.web.analytics.channels.aiohttp.ClientSession.post", return_value=MockClientResponse(200)
    ) as mock_post, patch("biothings.web.analytics.channels.certifi.where") as mock_certifi, patch(
        "biothings.web.analytics.channels.ssl.create_default_context"
    ) as mock_ssl_context:

        # Mocking the post request response and certifi.where
        mock_certifi.return_value = "/path/to/fake_cert.pem"  # Any dummy path
        mock_ssl_context.return_value = None  # Return None to bypass actual SSL context

        await channel.send(message)

    mock_post.assert_called_once_with(url, json=message.to_slack_payload(), ssl=None)


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

    expected_events = event.to_GA4_payload(channel.measurement_id, channel.uid_version)
    expected_data = serializer.to_json(
        {
            "client_id": "12345",
            "user_id": "67890",
            "events": expected_events,
        },
        return_bytes=True,
    )

    with patch(
        "biothings.web.analytics.channels.aiohttp.ClientSession.post", return_value=MockClientResponse(200)
    ) as mock_post, patch.object(event, "_cid", side_effect=[12345, 67890]):
        await channel.send(event)

    mock_post.assert_called_once_with(channel.url, data=expected_data)


@pytest.mark.asyncio
async def test_send_GA4_request_retries():
    channel = GA4Channel("G-XXXXXX", "SECRET")
    url = channel.url
    # data = orjson.dumps({"test": "data"})
    data = serializer.to_json({"test": "data"}, return_bytes=True)

    async with aiohttp.ClientSession() as session:
        with patch(
            "biothings.web.analytics.channels.aiohttp.ClientSession.post",
            side_effect=[MockClientResponse(500), MockClientResponse(200)],
        ) as mock_post, patch("biothings.web.analytics.channels.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await channel.send_request(session, url, data)

    assert mock_post.call_count == channel.max_retries + 1
    mock_post.assert_has_calls([call(url, data=data), call(url, data=data)])
    mock_sleep.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_GA4_request_max_retries():
    channel = GA4Channel("G-XXXXXX", "SECRET")
    url = channel.url
    data = serializer.to_json({"test": "data"}, return_bytes=True)

    async with aiohttp.ClientSession() as session:
        with patch(
            "biothings.web.analytics.channels.aiohttp.ClientSession.post",
            side_effect=[MockClientResponse(500) for _ in range(channel.max_retries + 1)],
        ) as mock_post, patch("biothings.web.analytics.channels.asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(Exception, match="GA4Channel: Maximum retries reached. Unable to complete request."):
                await channel.send_request(session, url, data)

    assert mock_post.call_count == channel.max_retries + 1
    mock_post.assert_has_calls([call(url, data=data) for _ in range(channel.max_retries + 1)])
