"""
    Test Status Endpoint

    GET  /status
    HEAD /status

"""

from biothings.tests.web import BiothingsWebAppTest
from setup import setup_es  # pylint: disable=unused-import


class TestStatus(BiothingsWebAppTest):

    def test_01_get(self):
        """
        GET /status
        {
            "success": true,
            "status": "yellow"
        }
        GET /status?dev
        {
            ...
            "status": "yellow",
            "payload": {
                "id": "1017",
                "index": "bts_test",
                "doc_type": "_all"
            },
            "document": {
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
        assert res['success']
        res = self.request('/status?dev').json()
        assert res['document']['found']

    def test_02_head(self):

        self.request('/status', method='HEAD')
