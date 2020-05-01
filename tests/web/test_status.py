"""
    Test Status Endpoint

    GET  /status
    HEAD /status

"""

from biothings.tests.web import BiothingsTestCase
from setup import setup_es  # pylint: disable=unused-import


class TestStatus(BiothingsTestCase):

    def test_01_get(self):
        """
        {
            "code": 200,
            "status": "yellow",
            "payload": {
                "id": "1017",
                "index": "bts_test",
                "doc_type": "_all"
            },
            "response": {
                "_index": "bts_test",
                "_type": "gene",
                "_id": "1017",
                "_version": 1,
                "found": true,
                "_source": { ... }
            }
        }
        """
        res = self.request('/status').json()
        assert res['code'] == 200
        assert res['response']['found']

    def test_02_head(self):

        self.request('/status', method='HEAD')
