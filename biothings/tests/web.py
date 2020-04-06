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
import sys
from difflib import Differ
from functools import partial

import requests
from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from biothings.web.settings import BiothingESWebSettings


class BiothingsTestCase(AsyncHTTPTestCase):
    """
        Starts a tornado server to run tests locally.
        Need a config.py under the current working dir.
        If a host is specified, test against that host.
    """

    host = os.getenv("TEST_HOST", '')  # test locally when empty.
    path = ''  # api path with prefix, populated from web settings.

    @classmethod
    def setup_class(cls):
        cls.settings = BiothingESWebSettings('config')
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
        handlers = self.settings.generate_app_handlers()
        settings = self.settings.generate_app_settings()
        return Application(handlers, **settings)

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

            assert bool(res.get('hits', [])) is hits
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


def equal(type_a, value_a, type_b, value_b):
    ''' Equality assertion with helpful diff messages '''

    def split(string):
        chars_per_line = 68
        return [string[i:i+chars_per_line]+'\n'
                for i in range(0, len(string), chars_per_line)]

    if not value_a == value_b:
        if isinstance(value_a, set) and isinstance(value_b, set):
            print('Objects in', type_a, 'only:')
            print(value_a - value_b)
            print('Objects in', type_b, 'only:')
            print(value_b - value_a)
        else:
            lines1 = split(str(value_a))
            lines2 = split(str(value_b))
            differ = Differ()
            result = list(differ.compare(lines1, lines2))
            start_index = None  # inclusive
            end_index = None  # exclusive
            for index, result_line in enumerate(result):
                if result_line.startswith('  '):  # common lines
                    if start_index:
                        end_index = index
                        break
                    continue
                if not start_index:
                    start_index = index
                else:
                    end_index = index

            if end_index - start_index > 8:  # show 2 mismatch lines max
                end_index = start_index + 8
            else:
                end_index += 3  # show context unless diff is too long
            start_index -= 3  # show context
            if start_index < 0:
                start_index = 0
            if end_index > len(result):
                end_index = len(result)
            result[end_index-1] = result[end_index-1][:-1]  # remove trailing newline
            sys.stdout.writelines(result[start_index:end_index])
        raise AssertionError(type_a + ' != ' + type_b)
