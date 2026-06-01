from types import SimpleNamespace
from unittest.mock import patch

from tornado.httputil import HTTPHeaders

from biothings.web.analytics.events import Event
from biothings.web.analytics.notifiers import AnalyticsMixin


class DummyNotifier:
    async def broadcast(self, event):
        pass


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


def test_analytics_mixin_records_referer_header():
    handler = DummyAnalyticsHandler(
        {
            "Referer": "https://data.niaid.nih.gov/",
        }
    )

    with patch("biothings.web.analytics.notifiers.asyncio.get_event_loop", return_value=object()), patch(
        "biothings.web.analytics.notifiers.asyncio.run_coroutine_threadsafe"
    ) as schedule:
        handler.on_finish()

    schedule.call_args.args[0].close()
    assert handler.event["__request__"]["referer"] == "https://data.niaid.nih.gov/"
    assert (
        handler.event.to_GA4_payload("GA4_MEASUREMENT_ID")[0]["params"]["page_referrer"]
        == "https://data.niaid.nih.gov/"
    )


def test_analytics_mixin_referer_falls_back_to_referer_header():
    handler = DummyAnalyticsHandler({"Referer": "https://data.niaid.nih.gov/"})

    with patch("biothings.web.analytics.notifiers.asyncio.get_event_loop", return_value=object()), patch(
        "biothings.web.analytics.notifiers.asyncio.run_coroutine_threadsafe"
    ) as schedule:
        handler.on_finish()

    schedule.call_args.args[0].close()
    assert handler.event["__request__"]["referer"] == "https://data.niaid.nih.gov/"
