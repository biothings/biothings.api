"""
    Test Status Endpoint

    GET  /status
    HEAD /status

"""

from biothings.tests.web import BiothingsTestCase
from setup import setup_es  # pylint: disable=unused-import


class TestStatus(BiothingsTestCase):

    def test_01_get(self):

        res = self.request('/status').text
        assert res == 'OK'

    def test_02_head(self):

        self.request('/status', method='HEAD')
