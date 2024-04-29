import orjson

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
            request_data = {
                "url": url,
                "method": "POST",
                "headers": {"content-type": "application/json"},
                "data": orjson.dumps(message.to_slack_payload()).decode(),
                # TODO: include other certificate param
            }
            yield request_data


class GAChannel(Channel):
    def __init__(self, tracking_id, uid_version=1):
        self.tracking_id = tracking_id
        self.uid_version = uid_version

    def handles(self, event):
        return isinstance(event, Event)

    def send(self, payload):
        events = payload.to_GA_payload(self.tracking_id, self.uid_version)
        for i in range(0, len(events), 20):
            request_data = {
                "url": "http://www.google-analytics.com/batch",
                "method": "POST",
                "data": "\n".join(events[i : i + 20]),
            }
            yield request_data


class GA4Channel(Channel):
    def __init__(self, measurement_id, api_secret, uid_version=1):
        self.measurement_id = measurement_id
        self.api_secret = api_secret
        self.uid_version = uid_version

    def handles(self, event):
        return isinstance(event, Event)

    def send(self, payload):
        events = payload.to_GA4_payload(self.measurement_id, self.uid_version)
        url = f"https://www.google-analytics.com/mp/collect?measurement_id={self.measurement_id}&api_secret={self.api_secret}"
        for i in range(0, len(events), 25):
            data = {
                "client_id": str(payload._cid(self.uid_version)),
                "user_id": str(payload._cid(1)),
                "events": events[i : i + 25],
            }
            request_data = {
                "url": url,
                "method": "POST",
                "data": orjson.dumps(data),
            }
            yield request_data
