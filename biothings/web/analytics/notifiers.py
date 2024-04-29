import requests

from collections import defaultdict

from tornado.httpclient import AsyncHTTPClient
from tornado.web import RequestHandler

from biothings.web.analytics.channels import GA4Channel, GAChannel, SlackChannel


class Notifier:
    def __init__(self, settings):
        self.channels = []

        if hasattr(settings, "SLACK_WEBHOOKS"):
            self.channels.append(SlackChannel(getattr(settings, "SLACK_WEBHOOKS")))  # noqa B009
        if getattr(settings, "GA_ACCOUNT", None):
            self.channels.append(
                GAChannel(
                    getattr(settings, "GA_ACCOUNT"),  # noqa B009
                    getattr(settings, "GA_UID_GENERATOR_VERSION", 1),
                )
            )
        if getattr(settings, "GA4_MEASUREMENT_ID", None):
            self.channels.append(
                GA4Channel(
                    measurement_id=getattr(settings, "GA4_MEASUREMENT_ID"),  # noqa B009
                    api_secret=getattr(settings, "GA4_API_SECRET"),  # noqa B009
                    uid_version=getattr(settings, "GA4_UID_GENERATOR_VERSION", 2),
                )
            )

    def broadcast(self, event):
        for channel in self.channels:
            if channel.handles(event):
                yield from channel.send(event)


class AnalyticsMixin(RequestHandler):
    def on_finish(self):
        super().on_finish()

        if self.get_argument("no_tracking", False) or self.settings.get("debug", False):
            return

        request_info = defaultdict(type(None))
        request_info["user_ip"] = self.request.remote_ip
        request_info["user_agent"] = self.request.headers.get("User-Agent")
        request_info["host"] = self.request.host
        request_info["path"] = self.request.path
        request_info["referer"] = self.request.headers.get("Referer")
        self.event["__request__"] = request_info

        if hasattr(self, "biothings"):
            notifier = self.biothings.notifier
            for channel in notifier.broadcast(self.event):
                self.send_requests(channel)

    def send_requests(self, channel):
        url = channel["url"]
        method = channel["method"]
        data = channel.get("data")
        headers = channel.get("headers")

        response = requests.request(method, url, data=data, headers=headers)

# FastAPI

# ...
