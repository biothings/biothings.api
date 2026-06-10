import asyncio
from collections import defaultdict

from tornado.web import RequestHandler

from biothings.web.analytics.channels import GA4Channel, SlackChannel


class Notifier:
    def __init__(self, settings):
        self.channels = []

        if hasattr(settings, "SLACK_WEBHOOKS"):
            self.channels.append(SlackChannel(getattr(settings, "SLACK_WEBHOOKS")))
        if getattr(settings, "GA4_MEASUREMENT_ID", None):
            self.channels.append(
                GA4Channel(
                    measurement_id=getattr(settings, "GA4_MEASUREMENT_ID"),
                    api_secret=getattr(settings, "GA4_API_SECRET"),
                    uid_version=getattr(settings, "GA4_UID_GENERATOR_VERSION", 2),
                )
            )

    async def broadcast(self, event):
        for channel in self.channels:
            if await channel.handles(event):
                await channel.send(event)


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
            # on_finish runs on the event loop thread: schedule the broadcast
            # there directly, retrieving a potential exception once done
            task = asyncio.get_running_loop().create_task(notifier.broadcast(self.event))
            task.add_done_callback(lambda t: t.cancelled() or t.exception())
        else:  # need to initialize a notifier
            raise NotImplementedError()
