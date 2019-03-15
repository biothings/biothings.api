'''
    Biothings Test Helper
'''
import json
import re
import sys
import unittest
from functools import wraps

import msgpack
import requests
import six
from tornado.testing import AsyncHTTPTestCase

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

PY3 = bool(sys.version_info.major >= 3)


class TornadoTestServerMixin(AsyncHTTPTestCase):
    '''
        Starts a tornado server to run tests on
        Must appear first in inheritance list
    '''
    __test__ = False  # not to be used directly

    def __init__(self, methodName):
        super(TornadoTestServerMixin, self).__init__(methodName)
        # remove host part (http://localhost:8000)
        # as test client require URLs starting with "/..."
        self.api = self.api.replace(self.host, '')
        self.host = ''

    def get_app(self):
        """ Should be overridden by subclasses to return a
            `tornado.web.Application` or other `.HTTPServer` callback. """
        raise NotImplementedError()

    def request(self, url, method="GET", body=None, headers=None):
        ''' Overrides http request client with Tornado's
            Function signature simulates httplib2.Http.request()'''
        res = self.fetch(url, method=method, body=body, headers=headers)
        res.status_code = res.code
        return res, res.body


class BiothingsTestCase(unittest.TestCase):
    '''
        Contains common functions to help facilitate testing.
        Assumes that .host .api are set in the subclass
    '''
    __test__ = False  # not to be used directly

    # override in subclass
    host = None
    api = None

    _d = staticmethod(json.loads)    # shorthand for json decode
    _e = staticmethod(json.dumps)    # shorthand for json encode

    #############################################################
    # Helper functions                                          #
    #############################################################

    # HTTP Requests

    @staticmethod
    def request(url, method="GET", body=None, headers=None):
        ''' Function signature simulates httplib2.Http.request() '''
        if method == 'GET':
            res = requests.get(url, headers=headers)
        elif method == 'POST':
            res = requests.post(url, data=body, headers=headers)
        elif method == 'HEAD':
            res = requests.head(url, headers=headers)
        return res, res.content

    def head_ok(self, url):
        ''' Send a HEAD request and asserts response status code indicates
        the entity-header fields corresponding to the requested resource
        are sent in the response without any message-body (status 200) '''
        res, _ = self.request(url, 'HEAD')
        assert res.status_code == 200, "status {} != 200 for HEAD to url: {}".format(
            res.status_code, url)

    def get_status_match(self, url, status_code):
        ''' Make a get request and asserts the response status code matches
        that of expectation, returns response content in bytes '''
        res, con = self.request(url)
        assert res.status_code == status_code, "status {} != {} for GET to url: {}".format(
            res.status_code, status_code, url)
        return con

    def get_ok(self, url):
        ''' Asserts server says the requested resource is sent, returns response body in bytes '''
        return self.get_status_match(url, 200)

    def get_404(self, url):
        ''' Asserts server says the requested resource is not found '''
        self.get_status_match(url, 404)

    def get_405(self, url):
        ''' Asserts server says GET is not allowed for the specified resource '''
        self.get_status_match(url, 405)

    def post_status_match(self, url, params, status_code, add_headers=None):
        ''' Make a post request and asserts the response status code matches
            that of expectation, return the response content in bytes  '''
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        headers.update(add_headers)
        res, con = self.request(url, 'POST', urlencode(
            self.encode_dict(params)), headers=headers)
        assert res.status_code == status_code, "status {} != {} for url: {}\nparams: {}".format(
            res.status_code, status_code, url, params)
        return con

    def post_ok(self, url, params):
        ''' Asserts server says the POST request has succeeded and it responded
            with an entity describing or containing the result of the action '''
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        res, con = self.request(url, 'POST', urlencode(
            self.encode_dict(params)), headers=headers)
        assert res.status_code == 200, "status {} != 200 for url: {}\nparams: {}".format(
            res.status, url, params)
        return con

    @staticmethod
    def parse_url(url, option):
        ''' Parse the url string to see if an option is specified,
            if so return the option value, if not return empty string '''
        options = urlparse(url).query.split('&')
        for opt in options:
            if opt.split('=')[0] == option:
                return opt.split('=')[1]
        return ''

    # Data and Formats

    @staticmethod
    def encode_dict(dic):
        ''' urllib.urlencode (python 2.x) cannot take unicode string.
            encode as utf-8 first to get it around.
        '''
        if PY3:
            # no need to do anything
            return dic

        def smart_encode(string):
            return string.encode('utf-8') if isinstance(string, six.text_type) else string

        return {key: smart_encode(val) for key, val in dic.items()}

    @staticmethod
    def truncate(string, limit):
        ''' Truncate a long string with a trailing ellipsis '''
        if len(string) <= limit:
            return string
        return string[:limit] + '...'

    @classmethod
    def json_ok(cls, byte_str, checkerror=True):
        ''' Load utf-8 encoded json into a dict '''
        dic = cls._d(byte_str.decode('utf-8'))
        if checkerror:
            assert not (isinstance(dic, dict)
                        and 'error' in dic), cls.truncate(str(dic), 100)
        return dic

    @classmethod
    def msgpack_ok(cls, packed_bytes, checkerror=True):
        ''' Load msgpack into a dict '''
        dic = msgpack.unpackb(packed_bytes)
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
        res_text = self.get_ok(base_url).decode('utf-8')
        callback = self.parse_url(base_url, 'callback')
        res_text = re.sub('\n', '', res_text)
        pattern = r'^(?P<callback>' + callback + r'\()(?P<res>\{.*\})\)$'
        match = re.search(pattern, res_text)
        assert match, 'JSONP object malformed'
        result = match.groupdict()
        # get the json object out of the callback so we can test it
        return self._d(result['res'])

    # Biothings

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
                possible_fields += [s for s in all_fields if s ==
                                    field or s.startswith(field + '.')]
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

    def query_has_hits(self, param_q, query_endpoint='query'):
        ''' Query and asserts positive hits, return the search result '''
        dic = self.json_ok(self.get_ok(
            self.api + '/' + query_endpoint + '?q=' + param_q))
        assert dic.get('total', 0) > 0 and dic.get('hits', [])
        return dic

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
