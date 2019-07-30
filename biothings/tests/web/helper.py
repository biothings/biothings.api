'''
    Biothings Test Helper
'''
import json
import re
import sys
import unittest
from difflib import Differ
from functools import partial, wraps
from urllib.parse import urlparse

import requests
from nose import SkipTest
from nose.tools import eq_
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from biothings.web.settings import BiothingESWebSettings


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


class TornadoTestServerMixin(AsyncHTTPTestCase):
    '''
        Starts a tornado server to run tests on
        Must appear first in subclass's inheritance list
        May override with customzied 'settings'
    '''

    host = ''

    def __new__(cls, *args, **kwargs):
        if not getattr(cls, 'settings', None):
            cls.settings = BiothingESWebSettings(config='config')
        return super(TornadoTestServerMixin, cls).__new__(cls)

    # override
    def get_app(self):
        app_list = self.settings.generate_app_list()
        static_path = self.settings.STATIC_PATH
        if getattr(self.settings, 'COOKIE_SECRET', None):
            return Application(app_list, static_path=static_path,
                               cookie_secret=self.settings.COOKIE_SECRET)
        return Application(app_list, static_path=static_path)

    # override
    def request(self, *args, **kwargs):
        ''' Use requests.py instead of the built-in client
            Override to make the requests non-blocking
            param: path: network path to make request to
            param: method: ('GET') http request method
            param: expect_status: (200) check status code
        '''

        partial_func = partial(super(TornadoTestServerMixin, self).request, **kwargs)

        async def call_blocking():
            return await self.io_loop.run_in_executor(None, partial_func, *args)

        return self.io_loop.run_sync(call_blocking)


class BiothingsTestCase(unittest.TestCase):
    '''
        Contains common functions to help facilitate testing.
        Assumes that .host .api are set in the subclass
    '''

    # override in subclass
    host = None  # no trailing slash '/'
    api = None  # do not include host, starts with '/'

    #############################################################
    # Helper functions                                          #
    #############################################################

    # HTTP Requests

    def request(self, path, method="GET", expect_status=200, **kwargs):
        '''
           Prefix path with api endpoint unless it specifies an absolute
           or relative URL. Make a request according to the method specified.
           Validate response status code and return the response object. '''

        assert self.api is not None  # allow empty string
        if not path:
            url = self.get_url('/')
        elif path.lower().startswith(("http://", "https://")):
            url = path
        elif path.startswith('/'):
            url = self.get_url(path)
        else:
            url = self.get_url(self.api + '/' + path)
        res = requests.request(method, url, **kwargs)
        eq_(res.status_code, expect_status)
        return res

    def get_url(self, path):
        ''' Takes a relative path starting with '/'
            Append it to host and return the full url
            Need override for local tornado tests '''

        assert path.startswith('/')
        return self.host + path

    def query(self, method='GET', endpoint='query', expect_hits=True, **kwargs):
        ''' Make a query and assert positive hits by default.
            Assert zero hit when expect_hits is set to False. '''

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

    @staticmethod
    def parse_url(url, option):
        ''' Parse the url string to see if an option is specified,
            if so return the option value, if not return empty string '''
        options = urlparse(url).query.split('&')
        for opt in options:
            if opt.split('=')[0] == option:
                return opt.split('=')[1]
        return ''

    # Data Formats

    @staticmethod
    def truncate(string, limit):
        ''' Truncate a long string with a trailing ellipsis '''
        if len(string) <= limit:
            return string
        return string[:limit] + '...'

    @classmethod
    def msgpack_ok(cls, packed_bytes, checkerror=True):
        ''' Load msgpack into a dict '''
        try:
            import msgpack
        except ImportError:
            raise SkipTest('Msgpack is not installed.')
        try:
            dic = msgpack.unpackb(packed_bytes)
        except:  # pylint: disable=bare-except
            assert False, 'Not a valid Msgpack binary.'
        if checkerror:
            assert not (isinstance(dic, dict)
                        and 'error' in dic), cls.truncate(str(dic), 100)
        return dic

    @staticmethod
    def convert_msgpack(obj):
        '''
            obj is a msgpack decoded dict (strings are still byte objects).
            Traverse through obj and decode all bytes into python strings.  Return the result.
        '''
        def convert_str(k):
            if isinstance(k, bytes):
                return k.decode('utf-8')
            return k

        def traverse(obj):
            if isinstance(obj, list):
                return [traverse(i) for i in obj]
            if isinstance(obj, dict):
                return {convert_str(tk): traverse(tv) for (tk, tv) in obj.items()}
            return convert_str(obj)

        return traverse(obj)

    def check_jsonld(self, jsld, jsonld_context):
        '''
            Traverse through jsld and assert that JSON-LD contexts are inserted and then removed
            should leave jsld a context-less JSON object...(no guarantee, see below)
        '''
        # Currently only tests that contexts in context file are in the object, and
        # removes them.  Maybe should add an else to the if not k: clause, and test that no
        # @context are in objects where they shouldn't be, to be really complete
        def traverse(jsld, k):
            # valid jsonld context?
            if isinstance(jsld, list):
                return [traverse(i, k) for i in jsld]
            if isinstance(jsld, dict):
                if not k:
                    # Root
                    self.assertIn(
                        '@context', jsld, "JSON-LD context not found in root.  Expected: {}"
                        .format(jsonld_context['root']))
                    self.assertDictEqual(
                        jsonld_context['root']['@context'], jsld['@context'])
                    del jsld['@context']
                    return {tk: traverse(tv, tk) for (tk, tv) in jsld.items()}
                if k in jsonld_context['root']['@context'] and k not in jsonld_context:
                    # No context, but defined in root context
                    return {tk: traverse(tv, k + '/' + tk) for (tk, tv) in jsld.items()}
                if k in jsonld_context:
                    # Context inserted, test it, and remove it
                    self.assertIn(
                        '@context', jsld, "JSON-LD context not found in {}.  Expected: {}"
                        .format(k, jsonld_context[k]))
                    self.assertDictEqual(
                        jsonld_context[k]['@context'], jsld['@context'])
                    del jsld['@context']
                    return {tk: traverse(tv, k + '/' + tk) for (tk, tv) in jsld.items()}
            return jsld
        if 'root' not in jsonld_context:
            return jsld  # can't check anything
        jsonld_context = jsonld_context  # maybe not needed...
        traverse(jsld, '')

    def extract_results_from_callback(self, base_url):
        '''
            Given a base query (with callback), the corresponding escaped GET url,
            test the response and return the JSON object inside
        '''
        res_text = self.request(base_url).text()
        callback = self.parse_url(base_url, 'callback')
        res_text = re.sub('\n', '', res_text)
        pattern = r'^(?P<callback>' + callback + r'\()(?P<res>\{.*\})\)$'
        match = re.search(pattern, res_text)
        assert match, 'JSONP object malformed'
        result = match.groupdict()
        # get the json object out of the callback so we can test it
        return json.loads(result['res'])

    @staticmethod
    def check_fields(res, res_all, fields, additionals=None):
        ''' Asserts that the fields in a response corresponds to the "fields" parameter in request
                :param res: the response to a test request
                :param res_all: the response to a "field = all" reference request
                :param list fields: fields specified in the test request e.g. ['cadd.gene']
                :param list additionals: additionally supported fields besides
                    typical biothing fields  eg. _license for cadd in myvariant
        '''
        def flatten_dict(json_obj, prefix, result):
            ''' Flattens the keys of a multi-level json object using dot notation
            returns a list of all dotted keys in the object, leaving a . in front :) '''
            if isinstance(json_obj, list):
                for obj in json_obj:
                    flatten_dict(obj, prefix, result)
            elif isinstance(json_obj, dict):
                for key in json_obj.keys():
                    result[prefix + '.' + key] = 0
                    flatten_dict(json_obj[key], prefix + '.' + key, result)

        def expand_fields(requested_fields, all_fields):
            ''' Returns the list of all possible keys in a valid response object,
            based on the users fields input, e.g.: cadd.gene means that cadd and cadd.gene
            should be in the output (if available), similarly, if clinvar is not in the total
            object, then it should not be in the output (even if the user specifies it).
            '''
            # find all possible fields from the request
            possible_fields = []
            if requested_fields[0] == 'all':
                return all_fields
            for field in requested_fields:
                possible_fields += [s for s in all_fields if s
                                    == field or s.startswith(field + '.')]
            # Go through and add parent nodes, which must be in the object....
            pfs = set(possible_fields)
            for fld in possible_fields:
                key = ''
                for path in fld.split('.'):
                    key = key + '.' + path if key else path
                    pfs.add(key)
            return list(pfs)

        # extract fields from "fields=all" response
        fields_all_flattened = {}
        flatten_dict(res_all, '', fields_all_flattened)
        # produce comparable keyword list
        fields_all_expanded = expand_fields(
            fields, [x.lstrip('.') for x in fields_all_flattened])
        # add root fields (_id, _version, query, _score)
        if not additionals:
            additionals = []
        fields_all_additional = ['_id', '_version',
                                 'query', '_score'] + additionals
        # combine comparable keyword lists into a set
        fields_all = set(fields_all_expanded + fields_all_additional)

        # extract fields from response to test
        fields_res_flattened = {}
        flatten_dict(res, '', fields_res_flattened)
        # produce comparable keyword set
        fields_res = {x.lstrip('.') for x in fields_res_flattened}

        # asserts no unspecified fields are returned
        assert fields_res.issubset(fields_all), \
            "The returned keys of object {} have extra keys than expected, the offending keys \
            are: {}".format(res['_id'], fields_res.difference(fields_all))


#############################################
# Decorators for easy parameterized testing
#############################################


PATTR = '%values'


def parameterized(cls):
    ''' Class decorator to add all parameterized test cases to class '''
    def feed(func, new_test_name, val):
        ''' produce a new test function '''
        @wraps(func)
        def wrapper(self):
            return func(self, val)
        wrapper.__name__ = new_test_name
        if func.__doc__:
            wrapper.__doc__ = func.__doc__
        return wrapper

    for name, method in list(cls.__dict__.items()):
        if hasattr(method, PATTR):
            for i, val in enumerate(getattr(method, PATTR)[0]):
                new_test_name = "{}_{}".format(name, i + 1)
                setattr(cls, new_test_name, feed(method, new_test_name, val))
            delattr(cls, name)
    return cls


def parameters(*values):
    ''' Method decorator to add parameter lists to template test methods '''
    def wrapper(func):
        setattr(func, PATTR, values)
        return func
    return wrapper
