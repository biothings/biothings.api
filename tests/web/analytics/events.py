from biothings.web.analytics.events import *
from pprint import pprint as print

# validator
# https://ga-dev-tools.web.app/hit-builder/

def test_1():
    event = Event(dict(__request__={
        "user_agent": "Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1",
        "referer": "https://example.com/",
        "user_ip": "127.0.0.1",
        "host": "example.org",
        "path": "/"
    }))
    print(event.to_GA_payload("UA-000000-2"))
    print(event.to_GA_payload("UA-000000-2", 2))


if __name__ == '__main__':
    test_1()
