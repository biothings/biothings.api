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
    ''' Starts a tornado server to run tests against
        Accepts server settings by overriding get_app in sub-classes
    '''
    __test__ = False

    def __init__(self, methodName):
        super(TornadoTestServerMixin, self).__init__(methodName)
        # remove host part (http://localhost:8000)
        # as test client require URLs starting with "/..."
        self.api = self.api.replace(self.host, '')
        self.host = ''

    def get_app(self):
        """Should be overridden by subclasses to return a
        `tornado.web.Application` or other `.HTTPServer` callback.
        """
        raise NotImplementedError()

    def request(self, url, method="GET", body=None, headers=None):
        ''' Overrides http request client with Tornado's
            Function signature simulates httplib2.Http.request()'''
        res = self.fetch(url, method=method, body=body, headers=headers)
        res.status_code = res.code
        return res, res.body


class BiothingsTestHelper(unittest.TestCase):
    ''' Contains common functions to help facilitate testing.
        Assumes that .host .api are set in the subclass '''

    __test__ = False

    _d = staticmethod(json.loads)    # shorthand for json decode
    _e = staticmethod(json.dumps)    # shorthand for json encode

    # TODO: Additional_fields, query_endpoint

    #############################################################
    # Helper functions                                          #
    #############################################################

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

    @staticmethod
    def encode_dict(dic):
        '''urllib.urlencode (python 2.x) cannot take unicode string.
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
        ''' truncate a long string with a trailing ellipsis '''
        if len(string) <= limit:
            return string
        return string[:limit] + '...'

    @classmethod
    def json_ok(cls, byte_str, checkerror=True):
        ''' load utf-8 encoded json into a dict '''
        dic = cls._d(byte_str.decode('utf-8'))
        if checkerror:
            assert not (isinstance(dic, dict)
                        and 'error' in dic), cls.truncate(str(dic), 100)
        return dic

    @classmethod
    def msgpack_ok(cls, packed_bytes, checkerror=True):
        ''' load msgpack into a dict '''
        dic = msgpack.unpackb(packed_bytes)
        if checkerror:
            assert not (isinstance(dic, dict)
                        and 'error' in dic), cls.truncate(str(dic), 100)
        return dic

    def head_ok(self, url):
        ''' status code indicating the entity-header fields corresponding to
        the requested resource are sent in the response without any message-body '''
        res, _ = self.request(url, 'HEAD')
        assert res.status_code == 200, "status {} != 200 for HEAD to url: {}".format(
            res.status_code, url)

    def get_status_code(self, url, status_code):
        ''' make a get request and return the content if the status code matches the expectation'''
        res, con = self.request(url)
        assert res.status_code == status_code, "status {} != {} for GET to url: {}".format(
            res.status_code, status_code, url)
        return con

    def get_ok(self, url):
        ''' status code indicating the requested resource is sent in the response '''
        return self.get_status_code(url, 200)

    def get_404(self, url):
        ''' status code indicating the requested resource is not found '''
        return self.get_status_code(url, 404)

    def get_405(self, url):
        ''' status code indicating the GET is not allowed for the specified resource '''
        return self.get_status_code(url, 405)

    def post_status_code(self, url, params, status_code):
        ''' make a post request and return the content if the status code matches the expectation'''
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        res, con = self.request(url, 'POST', urlencode(
            self.encode_dict(params)), headers=headers)
        assert res.status_code == status_code, "status {} != {} for url: {}\nparams: {}".format(
            res.status_code, status_code, url, params)
        return con

    def post_ok(self, url, params):
        ''' status code indicating the request has succeeded and the server
        responded with an entity describing or containing the result of the action'''
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        res, con = self.request(url, 'POST', urlencode(
            self.encode_dict(params)), headers=headers)
        assert res.status_code == 200, "status {} != 200 for url: {}\nparams: {}".format(
            res.status, url, params)
        return con

    def query_has_hits(self, param_q, query_endpoint='query'):
        ''' make the specified query and examine if return is empty '''
        dic = self.json_ok(self.get_ok(
            self.api + '/' + query_endpoint + '?q=' + param_q))
        assert dic.get('total', 0) > 0 and dic.get('hits', [])
        return dic

    @staticmethod
    def parse_url(url, option):
        ''' parse the url string to see if option is specified,
        if so return the option param, if not return empty string'''
        options = urlparse(url).query.split('&')
        for opt in options:
            if opt.split('=')[0] == option:
                return opt.split('=')[1]
        return ''

    @staticmethod
    def convert_msgpack(obj):
        ''' obj is a msgpack decoded dict (strings are still byte objects).
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

    @staticmethod
    def check_fields(res_req_flds, res_all_flds, req_flds, add_flds=None):
        ''' Tests the fields parameter.  Currently this takes these parameters:
                res_req_flds is the dict with original request response (with fields)
                res_all_flds is the dict with total request response (with fields = all)
                req_flds is the list of requested fields e.g. ['cadd.gene']
                add_flds is the list of additional fields not already known as
                    a typical biothing object eg. _license for cadd in myvariant

            expand_requested_fields: gives the list of all possible keys in an object,
            based on the users fields input, e.g.: cadd.gene means that cadd and cadd.gene
            should be in the output (if available), similarly, if clinvar is not in the total
            object, then it should not be in the output (even if the user specifies it).

            flatten_dict: this flattens a json objects keys using dot notation, returns a
            list of all dotted keys in the object, written poorly, so it leaves a . in front :)

            The test basically works like this: it flattens the total request (all possible fields),
            and then uses it in expand_requested_fields (as all_fields).  This returns the list of
            fields which should be in the o object (with fields f).  Next we flatten o, and make
            sure that flattened o is a subset of the list of fields which should be in the object +
            the root fields (_id, _version, query, _score).  Can do a more strict test with set
            equality, but this is fine for now....
        '''

        def expand_requested_fields(requested_fields, all_fields):
            # find all possible fields from the request,
            possible_fields = []
            if requested_fields[0] == 'all':
                return all_fields
            for field in requested_fields:
                possible_fields += [s for s in all_fields if s ==
                                    field or s.startswith(field + '.')]
            # Go through and add parent nodes, which must be in the object....
            pfs = set(possible_fields)
            for fld in possible_fields:
                tk = ''
                for path in fld.split('.'):
                    tk = tk + '.' + path if tk else path
                    pfs.add(tk)
            return list(pfs)

        def flatten_dict(d, p, r):
            if isinstance(d, list):
                for i in d:
                    flatten_dict(i, p, r)
            elif isinstance(d, dict):
                # Add these keys
                for k in d.keys():
                    r[p + '.' + k] = 0
                    flatten_dict(d[k], p + '.' + k, r)

        if not add_flds:
            add_flds = []
        possible_fields = {}
        flatten_dict(res_all_flds, '', possible_fields)
        true_fields = expand_requested_fields(
            req_flds, [x.lstrip('.') for x in possible_fields])
        actual_flattened_keys = {}
        flatten_dict(res_req_flds, '', actual_flattened_keys)
        actual_flattened_keys = [x.lstrip('.')
                                 for x in actual_flattened_keys]
        additional_fields = ['_id', '_version', 'query', '_score'] + add_flds
        # Make sure that all of the actual keys are among the set of requested fields
        assert set(actual_flattened_keys).issubset(set(true_fields + additional_fields)), \
            "The returned keys of object {} have extra keys than expected, the offending keys \
            are: {}".format(res_req_flds['_id'], set(actual_flattened_keys).difference(
                set(true_fields + additional_fields)))

    def extract_results_from_callback(self, base_url):
        '''
            given a base query (with callback), the corresponding escaped GET url,
            test the response and return the JSON object inside
        '''
        c = self.get_ok(base_url).decode('utf-8')
        f = self.parse_url(base_url, 'callback')
        c = re.sub('\n', '', c)
        p = r'^(?P<callback>' + f + '\()(?P<res>\{.*\})\)$'
        m = re.search(p, c)
        assert m, 'JSONP object malformed'
        r = m.groupdict()
        # get the json object out of the callback so we can test it
        return self._d(r['res'])

    def check_jsonld(self, d, jsonld_context):
        '''
            Traverse through d and assert that JSON-LD contexts are inserted and then remove them.
        should leave d a context-less JSON object...(no guarantee of this though, see below)
        '''
        # TODO:  Currently only tests that contexts in context file are in the object, and
        # removes them.  Maybe should add an else to the if not k: clause, and test that no
        # @context are in objects where they shouldn't be, to be really complete
        def traverse(d, k):
            # valid jsonld context?
            if isinstance(d, list):
                return [traverse(i, k) for i in d]
            elif isinstance(d, dict):
                if not k:
                    # Root
                    self.assertIn(
                        '@context', d, "JSON-LD context not found in root.  Expected: {}"
                        .format(jsonld_context['root']))
                    self.assertDictEqual(
                        jsonld_context['root']['@context'], d['@context'])
                    del(d['@context'])
                    return dict([(tk, traverse(tv, tk)) for (tk, tv) in d.items()])
                elif k in jsonld_context['root']['@context'] and k not in jsonld_context:
                    # No context, but defined in root context
                    return dict([(tk, traverse(tv, k + '/' + tk)) for (tk, tv) in d.items()])
                elif k in jsonld_context:
                    # Context inserted, test it, and remove it
                    self.assertIn(
                        '@context', d, "JSON-LD context not found in {}.  Expected: {}"
                        .format(k, jsonld_context[k]))
                    self.assertDictEqual(
                        jsonld_context[k]['@context'], d['@context'])
                    del(d['@context'])
                    return dict([(tk, traverse(tv, k + '/' + tk)) for (tk, tv) in d.items()])
            else:
                return d

        if 'root' not in jsonld_context:
            return d  # can't check anything

        jsonld_context = jsonld_context  # maybe not needed...
        traverse(d, '')

#############################################
# Decorators for easy parameterized testing
#############################################


PATTR = '%values'


def feed(f, new_test_name, v):
    ''' decorator to create a new test function '''
    @wraps(f)
    def wrapper(self):
        return f(self, v)
    wrapper.__name__ = new_test_name
    if f.__doc__:
        try:
            wrapper.__doc__ = f.__doc__
        except:
            pass
    return wrapper


def add_test(cls, new_test_name, f, v):
    ''' add test case with new_test_name to class cls '''
    setattr(cls, new_test_name, feed(f, new_test_name, v))


def parameters(*values):
    ''' decorator to add parameter lists to tests in unittest.TestCase '''
    def wrapper(f):
        setattr(f, PATTR, values)
        return f
    return wrapper


def parameterized(cls):
    ''' class decorator to go through and actually add all parameterized test cases to class '''
    for name, f in list(cls.__dict__.items()):
        if hasattr(f, PATTR):
            for i, v in enumerate(getattr(f, PATTR)[0]):
                new_test_name = "{}_{}".format(name, i + 1)
                add_test(cls, new_test_name, f, v)
            delattr(cls, name)
    return cls
