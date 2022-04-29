import orjson
import certifi
from tornado.httpclient import HTTPRequest

from biothings.web.analytics.events import Event, Message


class Channel:

    def handles(self, event):
        raise NotImplementedError()

    def send(self, event):
        raise NotImplementedError()


class SlackChannel(Channel):

    def __init__(self, hook_urls):
        self.hooks = hook_urls

    def handles(self, event):
        return isinstance(event, Message)

    def send(self, message):
        for url in self.hooks:
            yield HTTPRequest(
                url=url,
                method='POST',
                headers={'content-type': 'application/json'},
                body=orjson.dumps(message.to_slack_payload()).decode(),
                ca_certs=certifi.where()  # for Windows compatibility
            )


# Measurement Protocol (Universal Analytics)
# https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide

class GAChannel(Channel):

    def __init__(self, tracking_id, uid_version=1):
        self.tracking_id = tracking_id
        self.uid_version = uid_version

    def handles(self, event):
        return isinstance(event, Event)

    def send(self, payload):
        events = payload.to_GA_payload(self.tracking_id, self.uid_version)
        # #batch-limitations section of the URL above
        # A maximum of 20 hits can be specified per request.
        for i in range(0, len(events), 20):
            yield HTTPRequest(
                'http://www.google-analytics.com/batch', method='POST',
                body='\n'.join(events[i: i + 20])
            )


class GA4Channel(Channel):

    def __init__(self, measurement_id, api_secret, uid_version=1):
        self.measurement_id = measurement_id
        self.api_secret = api_secret
        self.uid_version = uid_version

    def handles(self, event):
        return isinstance(event, Event)

    def send(self, payload):
        """

        Limitations:
        https://developers.google.com/analytics/devguides/collection/protocol/ga4/sending-events?client_type=gtag
        """
        events = payload.to_GA4_payload(self.measurement_id, self.uid_version)
        # #batch-limitations section of the URL above
        # A maximum of 25 hits can be specified per request.
        url = f'https://www.google-analytics.com/mp/collect?measurement_id={self.measurement_id}&api_secret={self.api_secret}'
        for i in range(0, len(events), 25):
            data = {
                'client_id': str(payload._cid(self.uid_version)),
                'user_id': str(payload._cid(1)),
                'events': events[i: i + 25]
            }
            yield HTTPRequest(
                url, method='POST',
                body=orjson.dumps(data)
            )
