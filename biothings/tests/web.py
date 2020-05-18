"""
    Biothings Test Case Helper

    Envs:
    TEST_TIMEOUT    Individual request timeout in seconds.
    TEST_HOST       Tornado API server URL to test on. For example:

                    - When not specified, starts a local server
                    - Test a remote API server: http://www.mygene.info/v3
                    - Test a local API server: http://localhost:8000/api

    ES_HOST         Elasticsearch host address. Default to localhost:9200.

"""
import os
from functools import partial

import pytest
import requests
from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase

from biothings.web.settings import BiothingESWebSettings


class BiothingsTestCase(AsyncHTTPTestCase):
    """
        Starts a tornado server to run tests locally.
        Need a config.py under the current working dir.
        If a host is specified, test against that host.
    """
    conf = 'config'
    host = os.getenv("TEST_HOST", '')  # test locally when empty.
    path = ''  # api path with prefix, populated from web settings.

    @classmethod
    def setup_class(cls):
        cls.settings = BiothingESWebSettings(cls.conf)
        prefix = cls.settings.API_PREFIX
        version = cls.settings.API_VERSION
        cls.path = f'/{prefix}/{version}/'
        cls.path = cls.path.replace('//', '/')
        cls.host = cls.host.rstrip('/')

    # override
    def get_new_ioloop(self):
        return IOLoop.current()

    # override
    def get_app(self):
        return self.settings.get_app()

    # override
    def request(self, path, method="GET", expect=200, *args, **kwargs):
        ''' Use requests.py instead of the built-in client
            Override to make the requests non-blocking
            param: path: network path to make request to
            param: method: ('GET') http request method
            param: expect: (200) check status code
        '''
        url = self.get_url(path)

        if self.host:  # remote test
            res = requests.request(method, url, **kwargs)

        else:  # local test
            func = partial(requests.request, method, url, **kwargs)
            res = self.io_loop.run_sync(
                lambda: self.io_loop.run_in_executor(None, func, *args),
                timeout=os.getenv("TEST_TIMEOUT"))

        assert res.status_code == expect, res.text
        return res

    # override
    def get_url(self, path):
        """
        Return the URL that can be passed to an HTTP client.

        http://example.com/     ->      http://example.com/
        /status                 ->      http://<test_server>/status
        query?q=cdk2            ->      http://<test_server>/<api_path>/query?q=cdk2
        """
        if path.lower().startswith(("http://", "https://")):
            return path
        if not path.startswith('/'):  # biothings api call
            path = self.path + path
        if self.host:                 # remote server
            return self.host + path
        return super().get_url(path)  # local server

    def query(self, method='GET', endpoint='query', hits=True, data=None, json=None, **kwargs):
        """ Make a query and assert positive hits by default.
            Assert zero hit when hits is set to False. """

        if method == 'GET':
            res = self.request(
                endpoint,
                params=kwargs,
                data=data,
                json=json).json()

            assert bool(res.get('hits', [])) == hits
            return res

        if method == 'POST':
            res = self.request(
                endpoint,
                method=method,
                params=kwargs,
                data=data,
                json=json).json()

            for item in res:  # list
                if "_id" not in item:
                    _hits = False
                    break
            else:
                _hits = bool(res)
            assert _hits is hits
            return res

        raise ValueError(f'Query method {method} is not supported.')

    @staticmethod
    def msgpack_ok(packed_bytes):
        ''' Load msgpack into a dict '''
        try:
            import msgpack
        except ImportError:
            pytest.skip('Msgpack is not installed.')
        try:
            dic = msgpack.unpackb(packed_bytes)
        except BaseException:  # pylint: disable=bare-except
            assert False, 'Not a valid Msgpack binary.'
        return dic
