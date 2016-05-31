import sys
import json
import re
import msgpack

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

if sys.version_info.major >= 3:
    PY3 = True
else:
    PY3 = False


_d = json.loads    # shorthand for json decode
_e = json.dumps    # shorthand for json encode

class BiothingTestHelperMixin(object):
    ''' Contains common functions to help facilitate testing.  Assumes that this class will be
    subclassed by a class that inherits from both this mixin and from unittest.TestCase.
    Also assumes that .host .api .h are set in the subclass. '''

    #def __init__(self):
    #    self.host = os.getenv(ns.nosetest_envar)
    #    if not self.host:
    #        self.host = ns.nosetest_default_url
    #    self.host = self.host.rstrip('/')
    #    self.api = self.host + '/' + ns.api_version
    #    self.h = httplib2.Http()

    # TODO: Additional_fields, query_endpoint

    #############################################################
    # Helper functions                                          #
    #############################################################
    def encode_dict(self, d):
        '''urllib.urlencode (python 2.x) cannot take unicode string.
           encode as utf-8 first to get it around.
        '''
        if PY3:
            # no need to do anything
            return d
        else:
            def smart_encode(s):
                return s.encode('utf-8') if isinstance(s, unicode) else s   # noqa

            return dict([(key, smart_encode(val)) for key, val in d.items()])

    def truncate(self, s, limit):
        '''truncate a string.'''
        if len(s) <= limit:
            return s
        else:
            return s[:limit] + '...'

    def json_ok(self, s, checkerror=True):
        d = _d(s.decode('utf-8'))
        if checkerror:
            assert not (isinstance(d, dict) and 'error' in d), self.truncate(str(d), 100)
        return d

    def msgpack_ok(self, b, checkerror=True):
        d = msgpack.unpackb(b)
        if checkerror:
            assert not (isinstance(d, dict) and 'error' in d), self.truncate(str(d), 100)
        return d

    def get_ok(self, url):
        res, con = self.h.request((url))
        assert res.status == 200, "%r != 200" % (res.status)
        return con

    def get_404(self, url):
        res, con = self.h.request((url))
        assert res.status == 404, "%r != 404" % (res.status)

    def get_405(self, url):
        res, con = self.h.request((url))
        assert res.status == 405, "%r != 405" % (res.status)

    def head_ok(self, url):
        res, con = self.h.request((url), 'HEAD')
        assert res.status == 200, "%r != 200" % (res.status)

    def post_ok(self, url, params):
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        res, con = self.h.request(url, 'POST', urlencode(self.encode_dict(params)), headers=headers)
        assert res.status == 200, "%r != 200" % (res.status)
        return con

    def query_has_hits(self, q, query_endpoint='query'):
        d = self.json_ok(self.get_ok(self.api + '/' + query_endpoint + '?q=' + q))
        assert d.get('total', 0) > 0 and len(d.get('hits', [])) > 0
        return d

    def check_boolean_url_option(self, url, option):
        # for boolean params
        if self.parse_url(url, option) and self.parse_url(url, option).lower() in [1, 'true']:
            return True
        return False

    def parse_url(self, url, option):
        # parse the url string to see if option is specified, if so return it, if not return ''
        options = urlparse(url).query.split('&')
        for o in options:
            if o.split('=')[0] == option:
                return o.split('=')[1]
        return ''

    def convert_msgpack(self, d):
        ''' d is a msgpack decoded dict (strings are still byte objects).

            Traverse through d and decode all bytes into python strings.  Return the result.
        '''
        # convert d to a proper json object
        def convert_str(k):
            if isinstance(k, bytes):
                return k.decode('utf-8')
            else:
                return k

        def traverse(d):
            # decode the key
            if isinstance(d, list):
                return [traverse(i) for i in d]
            elif isinstance(d, dict):
                return dict([(convert_str(tk), traverse(tv)) for (tk,tv) in d.items()])
            else:
                return convert_str(d)

        return traverse(d)

    def check_msgpack(self, url):
        ''' Test msgpack with GET request.  Given a url, get the non msgpack results,
            and the msgpack results (after conversion), assert they are equal.
        '''
        separator = '?' if urlparse(url).query == '' else '&'
        # Get true query results
        res_t = self.json_ok(self.get_ok(url))
        # took is different between these requests, so messes up the equality assertion, just remove it
        res_t.pop('took', None)
        # Get same query results with messagepack
        res_msgpack = self.convert_msgpack(self.msgpack_ok(self.get_ok(url + separator + 'msgpack=true')))
        res_msgpack.pop('took', None)
        assert res_t == res_msgpack, 'Results with msgpack differ from original for: "{}"'.format(url)

    def check_fields(self, o, t, f, a=[]):
        ''' Tests the fields parameter.  Currently this takes these parameters:
                o is the dict with original request (with fields)
                t is the dict with total request (with fields = all)
                f is the list of requested fields e.g. ['cadd.gene']
                a is the list of additional fields that could be in a biothing object eg. _license
                    for cadd in myvariant        

            expand_requested_fields: gives the list of all possible keys in an object,
            based on the users fields input, e.g.: cadd.gene means that cadd and cadd.gene
            should be in the output (if available), similarly, if clinvar is not in the total
            object, then it should not be in the output (even if the user specifies it).

            flatten_dict: this flattens a json objects keys using dot notation, returns a 
            list of all dotted keys in the object, written poorly, so it leaves a . in front :)

            The test basically works like this: it flattens the total request (all possible fields),
            and then uses it in expand_requested_fields (as all_fields).  This returns the list of
            fields which should be in the o object (with fields f).  Next we flatten o, and make sure that
            flattened o is a subset of the list of fields which should be in the object + the root fields 
            (_id, _version, query, _score).  Can do a more strict test with set equality, 
            but this is fine for now....
        '''

        def expand_requested_fields(requested_fields, all_fields):
            # find all possible fields from the request,
            possible_fields = []
            if requested_fields[0] == 'all':
                return all_fields
            for field in requested_fields:
                possible_fields += [s for s in all_fields if s == field or s.startswith(field + '.')]
            # Go through and add parent nodes, which must be in the object....
            pfs = set(possible_fields)
            for f in possible_fields:
                tk = ''
                for path in f.split('.'):
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

        possible_fields = {}
        flatten_dict(t, '', possible_fields)
        true_fields = expand_requested_fields(f, [x.lstrip('.') for x in possible_fields.keys()])
        actual_flattened_keys = {}
        flatten_dict(o, '', actual_flattened_keys)
        actual_flattened_keys = [x.lstrip('.') for x in actual_flattened_keys.keys()]
        additional_fields = ['_id', '_version', 'query', '_score'] + a
        # Make sure that all of the actual keys are among the set of requested fields 
        assert set(actual_flattened_keys).issubset(set(true_fields + additional_fields)), "The returned keys of object {} have extra keys than expected, the offending keys are: {}".format(o['_id'], set(actual_flattened_keys).difference(set(true_fields + additional_fields)))

    def extract_results_from_callback(self, base_url):
        '''
            given a base query (with callback), the corresponding escaped GET url, test the response and 
            return the JSON object inside
        '''
        c = self.get_ok(base_url).decode('utf-8')
        f = self.parse_url(base_url, 'callback')
        c = re.sub('\n', '', c)
        p = r'^(?P<callback>' + f + '\()(?P<res>\{.*\})\)$'
        m = re.search(p, c)
        assert m, 'JSONP object malformed'
        r = m.groupdict()
        return _d(r['res']) # get the json object out of the callback so we can test it

    def check_jsonld(self, d, k, jsonld_context):
        '''
            Traverse through d and assert that JSON-LD contexts are inserted and then remove them.
        should leave d a context-less JSON object...(no guarantee of this though, see below)
        '''
        #TODO:  Currently only tests that contexts in context file are in the object, and 
        #removes them.  Maybe should add an else to the if not k: clause, and test that no 
        #@context are in objects where they shouldn't be, to be really complete

        # valid jsonld context?
        if 'root' not in jsonld_context:
            return d # can't check anything
        if isinstance(d, list):
            return [self.check_jsonld(i, k, jsonld_context) for i in d]
        elif isinstance(d, dict):
            if not k:
                # Root
                assert '@context' in d, "JSON-LD context not found in {}.  Expected: {}".format('root', jsonld_context['root'])
                self.assertDictEqual(jsonld_context['root']['@context'], d['@context'])
                del(d['@context'])
                return dict([(tk, self.check_jsonld(tv, tk, jsonld_context)) for (tk, tv) in d.items()])
            elif k in jsonld_context['root']['@context'] and k not in jsonld_context:
                # No context, but defined in root context
                return dict([(tk, self.check_jsonld(tv, k + '/' + tk, jsonld_context)) for (tk, tv) in d.items()])
            elif k in jsonld_context:
                # Context inserted, test it, and remove it
                assert '@context' in d, "JSON-LD context not found in {}.  Expected: {}".format(k, jsonld_context[k])
                self.assertDictEqual(jsonld_context[k]['@context'], d['@context'])
                del(d['@context'])
                return dict([(tk, self.check_jsonld(tv, k + '/' + tk, jsonld_context)) for (tk, tv) in d.items()])
        else:
            return d

