from pprint import pprint as print

from biothings.web.analytics.events import Event, GAEvent

# validator
# https://ga-dev-tools.web.app/hit-builder/


def test_pageview_1():
    event = Event(
        dict(
            __request__={
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": "https://example.com/",
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            }
        )
    )
    print(event.to_GA_payload("UA-000000-2"))
    print(event.to_GA_payload("UA-000000-2", 2))


def test_pageview_2():
    event = Event(
        dict(
            __request__={
                "user_agent": None,
                "referer": None,
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/404.html",
            }
        )
    )
    print(event.to_GA_payload("UA-000000-2"))
    print(event.to_GA_payload("UA-000000-2", 2))


def test_event_1():
    event = GAEvent(
        {
            "__request__": {
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": "https://example.com/",
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            },
            "category": "video",
            "action": "play",
            "label": "sample.mp4",
            "value": 60,
        }
    )
    print(event.to_GA_payload("UA-000000-2"))
    print(event.to_GA_payload("UA-000000-2", 2))


def test_event_2():
    event = GAEvent(
        {
            "__request__": {
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": "https://example.com/",
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            }
        }
    )
    print(event.to_GA_payload("UA-000000-2"))
    print(event.to_GA_payload("UA-000000-2", 2))


def test_pageview_ga4_1():
    event = Event(
        dict(
            __request__={
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": "https://example.com/",
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            }
        )
    )
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID"))
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID", 2))


def test_pageview_ga4_2():
    event = Event(
        dict(
            __request__={
                "user_agent": None,
                "referer": None,
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/404.html",
            }
        )
    )
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID"))
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID", 2))


def test_event_ga4_1():
    event = GAEvent(
        {
            "__request__": {
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": "https://example.com/",
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            },
            "category": "video",
            "action": "play",
            "label": "sample.mp4",
            "value": 60,
        }
    )
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID"))
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID", 2))


def test_event_ga4_2():
    event = GAEvent(
        {
            "__request__": {
                "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
                "referer": "https://example.com/",
                "user_ip": "127.0.0.1",
                "host": "example.org",
                "path": "/",
            }
        }
    )
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID"))
    print(event.to_GA4_payload("GA4_MEASUREMENT_ID", 2))


if __name__ == "__main__":
    test_event_2()
