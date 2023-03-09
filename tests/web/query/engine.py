from biothings.web.query.engine import *
from biothings.web import connections
from pprint import pprint as print

def test_adjust_index_overrided():

    class MyESQueryBackend(ESQueryBackend):
        def adjust_index(self, query, **options):
            return 'any_index'

    client = connections.get_es_client("localhost:9200", True)
    backend = MyESQueryBackend(client)
    query = None
    index = backend.adjust_index(query)
    assert index == 'any_index'

if __name__ == '__main__':
    test_adjust_index_overrided()
