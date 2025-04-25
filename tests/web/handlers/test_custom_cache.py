"""
Test Custom Cache Age

GET /case/1
GET /case/2
GET /case/3

"""

import sys
from biothings.tests.web import BiothingsWebAppTest
from biothings.web.settings.configs import ConfigModule


class TestBase(BiothingsWebAppTest):

    TEST_DATA_DIR_NAME = "data"

    @property
    def config(self):
        if not hasattr(self, "_config"):
            self._config = ConfigModule(sys.modules["config"])
        return self._config

    def test_01_get(self):
        """
        GET /case/1
        {
            "success": true,
            "status": "yellow"
        }
        """
        res = self.request("/case/1")
        assert res.headers["Cache-Control"] == "max-age=100, public"

    def test_02_get(self):
        """
        GET /case/2
        {
            "success": true,
            "status": "yellow"
        }

        """
        res = self.request("/case/2")
        assert res.headers["Cache-Control"] == "max-age=999, public"

    def test_03_get(self):
        """
        GET /case/3
        {
            "success": true,
            "status": "yellow"
        }

        """
        res = self.request("/case/3")
        assert res.headers["Cache-Control"] == "max-age=604800, public"
