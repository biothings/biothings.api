from biothings.web.query.engine import *
from biothings.web import connections
from pprint import pprint as print

def test_adjust_index():

    client = connections.get_es_client("localhost:9200", True)
    queryBackend = ESQueryBackend(client)
    assert '_all' == queryBackend.adjust_index()

def test_adjust_index_overrided():

    class MyESQueryBackend(ESQueryBackend):
        def adjust_index(self, **options):
            return 'any_index'

    client = connections.get_es_client("localhost:9200", True)
    queryBackend = MyESQueryBackend(client)
    assert 'any_index' == queryBackend.adjust_index()

if __name__ == '__main__':
    test_adjust_index()
    test_adjust_index_overrided()
