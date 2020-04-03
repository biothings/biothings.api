'''
    Biothings Test Helper

    Envs:
    TEST_TIMEOUT    Individual request timeout in seconds.
    TEST_HOST       Tornado API server URL to test on. For example:

                    - When not specified, starts a local server
                    - Test a remote API server: http://www.mygene.info/v3
                    - Test a local API server: http://localhost:8000/api

    ES_HOST         Elasticsearch host address. Read in BiothingSettings.
                    Required when testing locally. Default to localhost:9200.

'''
import os
from functools import partial

import requests
from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

import config
from biothings.web.settings import BiothingESWebSettings

TEST_INDEX = 'bts_test'
TEST_DOC_TYPE = 'gene'


class BiothingsTestCase(AsyncHTTPTestCase):
    '''
        Starts a tornado server to run tests on.
    '''

    @classmethod
    def setup_class(cls):
        cls.settings = BiothingESWebSettings(config)
        cls.prefix = cls.settings.API_VERSION

    # override
    def get_new_ioloop(self):
        return IOLoop.current()

    # override
    def get_app(self):

        app_list = self.settings.generate_app_handlers()

        settings = {"static_path": self.settings.STATIC_PATH}  # TODO maybe settings should generate app settings
        if getattr(self.settings, 'COOKIE_SECRET', None):
            settings["cookie_secret"] = self.settings.COOKIE_SECRET

        return Application(app_list, **settings)

    # override
    def request(self, path='/', method="GET", expect_status=200, **kwargs):
        """
        Make a requets with python requests library syntax.
        In addition, it compares response status code.
        """
        partial_func = partial(requests.request, method, self.get_url(path), **kwargs)

        res = self.io_loop.run_sync(
            lambda: self.io_loop.run_in_executor(None, partial_func),
            timeout=os.getenv("TEST_TIMEOUT"))

        status_code = res.status_code
        assert status_code == expect_status
        return res

    # override
    def get_url(self, path):
        '''
        Return the URL that can be passed to an HTTP client.

        When environment API_HOST is set to /v3: # TODO update

        http://example.com/     ->      http://example.com/
        /query?q=cdk2           ->      http://<test_server>/v3/query?q=cdk2
        metadata                ->      http://<test_server>/v3/metadata

        When environment API_HOST is set to http://localhost:8000/api:

        http://example.com/     ->      http://example.com/
        /query?q=cdk2           ->      http://localhost:8000/api/query?q=cdk2
        metadata                ->      http://localhost:8000/api/metadata
        '''

        if path.lower().startswith(("http://", "https://")):
            return path

        if not path.startswith('/'):
            return self.get_url('/' + path)

        return super().get_url(path)

    def query(self, method='GET', endpoint='query', expect_hits=True, **kwargs):
        '''
        Make a Biothings API query request.
        Query parameters are passed in as keyword arguments.
        '''  # TODO add in the /v1/
        endpoint = self.prefix + '/' + endpoint
        if method == 'GET':
            dic = self.request(endpoint, params=kwargs).json()
            if expect_hits:
                assert dic.get('hits', []), "No Hits"
            else:
                assert dic.get('hits', None) == [], f"Get {dic.get('hits')} instead."
            return dic

        if method == 'POST':
            lst = self.request(endpoint, method=method, data=kwargs).json()
            hits = False
            for item in lst:
                if "_id" in item:
                    hits = True
                    break
            if expect_hits:
                assert hits
            else:
                assert not hits
            return lst

        raise ValueError(f'Query method {method} is not supported.')
