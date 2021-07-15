from collections import defaultdict

from biothings.web.analytics.channels import *
from tornado.httpclient import AsyncHTTPClient
from tornado.web import RequestHandler


class Notifier:

    def __init__(self, settings):
        self.channels = []

        if hasattr(settings, 'SLACK_WEBHOOKS'):
            self.channels.append(SlackChannel(
                getattr(settings, 'SLACK_WEBHOOKS')
            ))
        if getattr(settings, 'GA_ACCOUNT', None):
            self.channels.append(GAChannel(
                getattr(settings, 'GA_ACCOUNT'),
                getattr(settings, 'GA_UID_GENERATOR_VERSION', 1)
            ))

    def broadcast(self, event):
        for channel in self.channels:
            if channel.handles(event):
                yield from channel.send(event)

# Web Framework Support
# ----------------------------

# Tornado

# https://www.tornadoweb.org/en/stable/httputil.html
# #tornado.httputil.HTTPServerRequest.remote_ip
class AnalyticsMixin(RequestHandler):
    def on_finish(self):
        super().on_finish()

        if self.get_argument('no_tracking', False):
            return  # this feature is undocumented

        if self.settings.get('debug', False):
            return  # for testing and development

        # Make sure to start the server with xheaders=True so that
        # remote_ip considers X-Real-Ip and X-Forwarded-For headers

        request = defaultdict(type(None))
        request["user_ip"] = self.request.remote_ip
        request["user_agent"] = self.request.headers.get("User-Agent")
        request["host"] = self.request.host
        request["path"] = self.request.path
        request["referer"] = self.request.headers.get("Referer")
        self.event["__request__"] = request

        if hasattr(self, 'biothings'):
            client = AsyncHTTPClient()
            notifier = self.biothings.notifier
            for request in notifier.broadcast(self.event):
                client.fetch(request)

        else:  # need to initialize a notifier
            raise NotImplementedError()


# FastAPI

# ...
