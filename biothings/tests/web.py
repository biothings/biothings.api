"""
    Biothings Test Helper

    Envs:

    TEST_SCHEME
    TEST_PREFIX
    TEST_HOST
    TEST_CONF

"""
import inspect
import os
from functools import partial

import pytest
import requests
from biothings.web import BiothingsAPI
from biothings.web.settings import configs
from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase


class BiothingsDataTest():

    # relative path parsing configuration
    scheme = 'http'
    prefix = 'v1'
    host = ''

    def request(self, path, method='GET', expect=200, **kwargs):
        """
        Use requests library to make an HTTP request.
        Ensure path is translated to an absolute path.
        Conveniently check if status code is as expected.
        """
        url = self.get_url(path)
        res = requests.request(method, url, **kwargs)
        assert res.status_code == expect, res.text

        return res

    def get_url(self, path):
        """
        Try best effort to get a full url to make a request.
        Return an absolute url when class var 'host' is defined.
        If not, return a path relative to the host root.
        """
        scheme = os.getenv("TEST_SCHEME", self.scheme)
        prefix = os.getenv("TEST_PREFIX", self.prefix).strip('/')
        host = os.getenv("TEST_HOST", self.host).strip('/')

        # already an absolute path
        if path.lower().startswith(("http://", "https://")):
            return path

        # path standardization
        if not path.startswith('/'):
            if prefix:  # append prefix
                path = '/'.join((prefix, path))
            path = '/' + path

        # host standardization
        if host:
            path = f"{scheme}://{host}{path}"

        return path

    def query(self, method='GET', endpoint='query', hits=True, data=None, json=None, **kwargs):
        """
        Make a query and assert positive hits by default.
        Assert zero hit when hits is set to False.
        """

        if method == 'GET':
            res = self.request(
                endpoint, params=kwargs,
                data=data, json=json).json()

            assert bool(res.get('hits')) == hits
            return res

        if method == 'POST':
            res = self.request(
                endpoint, method=method,
                params=kwargs, data=data,
                json=json).json()

            for item in res:  # list
                if "_id" not in item:
                    _hits = False
                    break
            else:
                _hits = bool(res)
            assert _hits is hits
            return res

        raise ValueError('Invalid Request Method.')

    @staticmethod
    def msgpack_ok(packed_bytes):
        """ Load msgpack into a dict """
        try:
            import msgpack
        except ImportError:
            pytest.skip('Msgpack is not installed.')
        try:
            dic = msgpack.unpackb(packed_bytes)
        except BaseException:  # pylint: disable=bare-except
            assert False, 'Not a valid Msgpack binary.'
        return dic


class BiothingsWebAppTest(BiothingsDataTest, AsyncHTTPTestCase):
    """
        Starts the tornado application to run tests locally.
        Need a config.py under the current working dir.
    """

    # override
    def get_new_ioloop(self):
        return IOLoop.current()

    # override
    def get_app(self):
        conf = os.getenv("TEST_CONF", 'config.py')
        base = os.path.dirname(inspect.getfile(type(self)))
        file = os.path.join(base, conf)
        config = configs.load(file)
        prefix = config.API_PREFIX
        version = config.API_VERSION
        self.prefix = f'{prefix}/{version}'
        return BiothingsAPI.get_app(config)

    # override
    def request(self, path, method="GET", expect=200, **kwargs):

        url = self.get_url(path)

        func = partial(requests.request, method, url, **kwargs)
        res = self.io_loop.run_sync(
            lambda: self.io_loop.run_in_executor(None, func))

        assert res.status_code == expect, res.text
        return res

    # override
    def get_url(self, path):

        path = BiothingsDataTest.get_url(self, path)
        return AsyncHTTPTestCase.get_url(self, path)


# Compatibility
BiothingsTestCase = BiothingsWebAppTest
