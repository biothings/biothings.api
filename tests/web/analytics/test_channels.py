from biothings.web.analytics.channels import *
from biothings.web.analytics.events import GAEvent
from tornado.httpclient import HTTPClient


def test_1():
    event = GAEvent({
        "__request__": {
            "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
            "referer": None,
            "user_ip": "127.0.0.1",
            "host": "example.org",
            "path": "/"
        },
        "category": "test",
        "action": "play",
        "label": "sample.mp4",
        "value": 60
    })
    channel = GAChannel("UA-107372303-1", 2)
    assert channel.handles(event)

    client = HTTPClient()
    for request in channel.send(event):
        print(client.fetch(request))


def test_2():
    event = GAEvent({
        "__request__": {
            "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
            "referer": None,
            "user_ip": "127.0.0.1",
            "host": "example.org",
            "path": "/"
        },
        "category": "test",
        "action": "play",
        "label": "sample.mp4",
        "value": 60
    })
    channel = GA4Channel("GA4_MEASUREMENT_ID", "GA4_API_SECRET", 1)
    assert channel.handles(event)

    client = HTTPClient()
    for request in channel.send(event):
        print(client.fetch(request))


if __name__ == '__main__':
    test_1()
    test_2()
