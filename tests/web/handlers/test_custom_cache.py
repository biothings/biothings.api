"""
Test Custom Cache Age

GET /case/1
GET /case/2
GET /case/3

"""

from biothings.tests.web import BiothingsWebAppTest


class TestBase(BiothingsWebAppTest):
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
