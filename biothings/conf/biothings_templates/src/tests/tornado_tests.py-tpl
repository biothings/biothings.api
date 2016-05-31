import sys
import os

src_path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application
from tests.tests import {% nosetest_settings_class %}
from biothings.tests.test_helper import TornadoRequestHelper
from www.api.handlers import return_applist
import unittest

class {% nosetest_settings_class %}TornadoClient(AsyncHTTPTestCase, {% nosetest_settings_class %}):
    __test__ = True

    def__init__(self, methodName='runTest', **kwargs):
        super(AsyncHTTPTestCase, self).__init__(methodName, **kwargs)
        self.h = TornadoRequestHelper(self)

    def get_app(self):
        return Application(return_applist())

if __name__ == "__main__":
    unittest.TextTestRunner().run({% nosetest_settings_class %}TornadoClient.suite())
