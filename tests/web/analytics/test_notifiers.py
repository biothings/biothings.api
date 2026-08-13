import asyncio
from types import SimpleNamespace

import pytest

from tornado.httputil import HTTPHeaders

from biothings.web.analytics.events import Event
from biothings.web.analytics.notifiers import AnalyticsMixin


class DummyNotifier:
    def __init__(self):
        self.events = []

    async def broadcast(self, event):
        self.events.append(event)


class DummyAnalyticsHandler(AnalyticsMixin):
    def __init__(self, headers):
        self.request = SimpleNamespace(
            headers=HTTPHeaders(headers),
            remote_ip="127.0.0.1",
            host="example.org",
            path="/v1/query",
        )
        self.event = Event()
        self._biothings = SimpleNamespace(notifier=DummyNotifier())

    @property
    def settings(self):
        return {}

    @property
    def biothings(self):
        return self._biothings

    def get_argument(self, name, default=None):
        return default


@pytest.mark.asyncio
async def test_analytics_mixin_records_referer_header():
    handler = DummyAnalyticsHandler(
        {
            "Referer": "https://data.niaid.nih.gov/",
        }
    )

    handler.on_finish()
    await asyncio.sleep(0)

    assert handler.biothings.notifier.events == [handler.event]
    assert handler.event["__request__"]["referer"] == "https://data.niaid.nih.gov/"
    assert (
        handler.event.to_GA4_payload("GA4_MEASUREMENT_ID")[0]["params"]["page_referrer"]
        == "https://data.niaid.nih.gov/"
    )


@pytest.mark.asyncio
async def test_analytics_mixin_referer_falls_back_to_referer_header():
    handler = DummyAnalyticsHandler({"Referer": "https://data.niaid.nih.gov/"})

    handler.on_finish()
    await asyncio.sleep(0)

    assert handler.biothings.notifier.events == [handler.event]
    assert handler.event["__request__"]["referer"] == "https://data.niaid.nih.gov/"
