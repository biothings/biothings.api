# -*- coding: utf-8 -*-
'''
Nose tests
run as "nosetests tests"
    or "nosetests tests:test_main"
'''
import httplib2
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
try:
    from urllib.parse import quote_plus
except ImportError:
    from urllib import quote_plus
import json
import sys
import os
import re
from nose.tools import ok_, eq_
try:
    import msgpack
except ImportError:
    sys.stderr.write("Warning: msgpack is not available.")
from biothings.tests.settings import NosetestSettings
from unittest import TestCase

if sys.version_info.major >= 3:
    PY3 = True
else:
    PY3 = False

ns = NosetestSettings()
_d = json.loads    # shorthand for json decode
_e = json.dumps    # shorthand for json encode
_q = quote_plus     # shorthand for url encoding

try:
    jsonld_context = json.load(open(ns.jsonld_context_path, 'r'))
except:
    sys.stderr.write("Couldn't load JSON-LD context.")
    jsonld_context = {}


class TornadoRequestHelper(object):
    def __init__(self,biothing_test_helper_instance):
        self.testinst = biothing_test_helper_instance
        # remove host part (http://localhost:8000) as test client require URLs
        # starting with "/..."
        self.testinst.api = self.testinst.api.replace(self.testinst.host,'')
        self.testinst.host = ''

    def request(self,url,method="GET",body=None,headers=None):#, body=None, headers=None, redirections=5,
        '''This simulates httplib2.Http.request() calls'''
        res = self.testinst.fetch(url,method=method,body=body,headers=headers)#,body=body,headers=headers)
        res.status = res.code
        return res,res.body


class BiothingTestHelper:
    def __init__(self):
        self.host = os.getenv(ns.nosetest_envar)
        if not self.host:
            self.host = ns.nosetest_default_url
        self.host = self.host.rstrip('/')
        self.api = self.host + '/' + ns.api_version
        self.h = httplib2.Http()

    #############################################################
    # Hepler functions                                          #
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
            ok_(not (isinstance(d, dict) and 'error' in d), self.truncate(str(d), 100))
        return d

    def msgpack_ok(self, b, checkerror=True):
        d = msgpack.unpackb(b)
        if checkerror:
            ok_(not (isinstance(d, dict) and 'error' in d), self.truncate(str(d), 100))
        return d

    def get_ok(self, url):
        res, con = self.h.request((url))
        eq_(res.status, 200)
        return con

    def get_404(self, url):
        res, con = self.h.request((url))
        eq_(res.status, 404)

    def get_405(self, url):
        res, con = self.h.request((url))
        eq_(res.status, 405)

    def head_ok(self, url):
        res, con = self.h.request((url), 'HEAD')
        eq_(res.status, 200)

    def post_ok(self, url, params):
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        res, con = self.h.request(url, 'POST', urlencode(self.encode_dict(params)), headers=headers)
        eq_(res.status, 200)
        return con
    
    def has_hits(self, q):
        d = self.json_ok(self.get_ok(self.api + '/' + ns.query_endpoint + '?q=' + q))
        ok_(d.get('total', 0) > 0 and len(d.get('hits', [])) > 0)
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

    def test_msgpack(self, url):
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

    def check_fields(self, o, t, f):
        ''' Tests the fields parameter.  Currently this takes these parameters:
                o is the dict with original request (with fields)
                t is the dict with total request (with fields = all)
                f is the list of requested fields e.g. ['cadd.gene']
        
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
        additional_fields = ['_id', '_version', 'query', '_score'] + ns.additional_fields_for_check_fields_subset
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

    def check_jsonld(self, d, k):
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
            return [self.check_jsonld(i, k) for i in d]
        elif isinstance(d, dict):
            if not k:
                # Root
                assert '@context' in d, "JSON-LD context not found in {}.  Expected: {}".format('root', jsonld_context['root'])
                eq_(jsonld_context['root']['@context'], d['@context'])
                del(d['@context'])
                return dict([(tk, self.check_jsonld(tv, tk)) for (tk, tv) in d.items()])
            elif k in jsonld_context['root']['@context'] and k not in jsonld_context:
                # No context, but defined in root context
                return dict([(tk, self.check_jsonld(tv, k + '/' + tk)) for (tk, tv) in d.items()])
            elif k in jsonld_context:
                # Context inserted, test it, and remove it
                assert '@context' in d, "JSON-LD context not found in {}.  Expected: {}".format(k, jsonld_context[k])
                eq_(jsonld_context[k]['@context'], d['@context'])
                del(d['@context'])
                return dict([(tk, self.check_jsonld(tv, k + '/' + tk)) for (tk, tv) in d.items()])
        else:
            return d

class BiothingTests(TestCase):
    __test__ = False # don't run nosetests on this class directly

    @classmethod
    def setup_class(cls):
        cls.h = BiothingTestHelper()
    
    @classmethod
    def teardown_class(cls):
        cls.h = None    

    #############################################################
    # Test functions                                            #
    #############################################################
        
    def test_main(self):
        self.h.get_ok(self.h.host)

    def test_annotation_object(self):
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.annotation_endpoint + '/' + ns.annotation_attribute_query))
        for attr in ns.annotation_attribute_list:
            assert res.get(attr, None) is not None, 'Missing field "{}" in {} "{}"'.format(attr, 
                                                            ns.annotation_endpoint, ns.annotation_attribute_query)
    
    def test_annotation_GET(self):
        ''' Function to test GETs to the annotation endpoint. 

            Currently supports automatic testing of:
                fields
                filter
                jsonld
                callback
        '''
        # Check some ids to make sure the resulting _id matches

        for (test_number, bid) in enumerate(ns.annotation_GET):
            base_url = self.h.api + '/' + ns.annotation_endpoint + '/' + bid
            get_url = self.h.api + '/' + ns.annotation_endpoint + '/' + _q(bid.split('?')[0]) + '?' + '?'.join(bid.split('?')[1:])
            # if it specifies a callback function, make sure it works
            if self.h.parse_url(base_url, 'callback'):
                res = self.h.extract_results_from_callback(get_url)
            else:
                res = self.h.json_ok(self.h.get_ok(get_url))
            # Check that the returned ID matches 
            eq_(res['_id'], bid.split('?')[0])
            # Is this a jsonld query?
            if self.h.check_boolean_url_option(base_url, 'jsonld') and 'root' in jsonld_context:
                self.h.check_jsonld(res, '') 
            if 'fields' in bid or 'filter' in bid:
                # This is a filter query, test it appropriately.  First get a list of fields the user specified
                if 'fields' in bid:
                    true_fields = [x.strip() for x in self.h.parse_url(base_url, 'fields').split(',')]
                elif 'filter' in bid:
                    true_fields = [x.strip() for x in self.h.parse_url(base_url, 'filter').split(',')]
                # Next get the object with no fields specified
                total_url = self.h.api + '/' + ns.annotation_endpoint + '/' + _q(bid.split('?')[0])
                res_total = self.h.json_ok(self.h.get_ok(total_url))
                # Check the fields
                self.h.check_fields(res, res_total, true_fields)
            # insert gibberish on first id, also test msgpack
            if test_number == 0:
                self.h.get_404(self.h.api + '/' + ns.annotation_endpoint + '/' + _q(bid[:-1] + '\xef\xbf\xbd\xef\xbf\xbd' + bid[-1]))
                self.h.test_msgpack(self.h.api + '/' + ns.annotation_endpoint + '/' + _q(bid.split('?')[0]))
            
            # override to add more tests
            self._extra_annotation_GET(bid, res)

        # test unicode string handling
        self.h.get_404(self.h.api + '/' + ns.annotation_endpoint + '/' + ns.unicode_test_string)

        # test empties 
        self.h.get_404(self.h.api + '/' + ns.annotation_endpoint)
        self.h.get_404(self.h.api + '/' + ns.annotation_endpoint + '/')

    
    def test_annotation_POST(self):
        ''' Function to test POSTs to the annotation endpoint.
            
            Currently supports automatic testing of:
                ids
                fields
                filters
                jsonld
        '''
        base_url = self.h.api + '/' + ns.annotation_endpoint
        for (test_number, ddict) in enumerate(ns.annotation_POST):
            res = self.h.json_ok(self.h.post_ok(base_url, ddict))
            returned_ids = [h['_id'] if '_id' in h else h['query'] for h in res]
            assert set(returned_ids) == set([x.strip() for x in ddict['ids'].split(',')]), "Set of returned ids doesn't match set of requested ids for annotation POST"
            # Check that the number of returned objects matches the number of inputs
            # Probably not needed given the previous test
            eq_(len(res), len(ddict['ids'].split(',')))
            for hit in res:
                # If it's a jsonld query, check that
                if 'jsonld' in ddict and ddict['jsonld'].lower() in ['true', 1]:
                    self.h.check_jsonld(hit, '')
                # If its a filtered query, check the return objects fields
                if 'filter' in ddict or 'fields' in ddict: 
                    true_fields = []
                    if 'fields' in ddict:
                        true_fields = [f.strip() for f in ddict.get('fields').split(',')]
                    elif 'filter' in ddict:
                        true_fields = [f.strip() for f in ddict.get('filter').split(',')]
                    total_url = base_url + '/' + _q(hit['_id'])
                    res_total = self.h.json_ok(self.h.get_ok(total_url))
                    self.h.check_fields(hit, res_total, true_fields)

                self._extra_annotation_POST(ddict, hit)

        # Check unicode handling on first test
        res_empty = self.h.json_ok(self.h.post_ok(base_url, {'ids': ns.unicode_test_string}))
        assert (len(res_empty) == 1) and (res_empty[0]['notfound']), "POST to annotation endpoint failed with unicode test string"
        # Check unicode test string as the second id in the list
        res_second_empty = self.h.json_ok(self.h.post_ok(base_url, {'ids': ns.annotation_POST[0]['ids'].split(',')[0] + ',' + ns.unicode_test_string}))
        assert (len(res_second_empty) == 2) and (res_second_empty[1]['notfound']), "POST to annotation endpoint failed with unicode test string"

    def test_query_GET(self):
        ''' Function to test GETs to the query endpoint.

            Automatically tested parameters:
            
            fields
            filters
            size
            fetch_all/scroll_id
            facets
            callback

            Separately tested:

            from
        ''' 
        # Test some simple GETs to the query endpoint, first check some queries to make sure they return some hits
        for (test_number, q) in enumerate(ns.query_GET):
            base_url = self.h.api + '/' + ns.query_endpoint + '?q=' + q
            # parse callback
            if self.h.parse_url(base_url, 'callback'):
                res = self.h.extract_results_from_callback(base_url)
            elif self.h.check_boolean_url_option(base_url, 'fetch_all'):
                # Is this a fetch all query?
                # TODO:  make this less crappy.
                sres = self.h.json_ok(self.h.get_ok(base_url))
                assert '_scroll_id' in sres, "_scroll_id not found for fetch_all query: {}".format(q)
                scroll_hits = int(sres['total']) if int(sres['total']) <= 1000 else 1000
                res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?scroll_id=' + sres['_scroll_id']))
                assert 'hits' in res, "No hits found for query: {}\nScroll ID: {}".format(q, sres['_scroll_id'])
                assert len(res['hits']) == scroll_hits, "Expected a scroll size of {}, got a scroll size of {}".format(scroll_hits, len(res['hits']))
            else:
                # does this query have hits?
                res = self.h.has_hits((q))
            # Test the size/size cap
            total_hits = int(res['total'])
            ret_size = len(res.get('hits', []))
            req_size = int(self.h.parse_url(base_url, 'size')) if self.h.parse_url(base_url, 'size') else 10
            if self.h.check_boolean_url_option(base_url, 'fetch_all'):
                true_size = scroll_hits
            elif total_hits < req_size:
                true_size = total_hits
            else:
                true_size = req_size if req_size <= 1000 else 1000
            assert ret_size == true_size, 'Expected {} hits for query "{}", got {} hits instead'.format(true_size, q, ret_size)
            # Test facets, maybe we should make it a subset test, i.e., set(returned facets) must be a subset of set(requested facets)
            if 'facets' in q:
                facets = [x.strip() for x in self.h.parse_url(base_url, 'facets').split(',')]
                assert 'facets' in res, "Facets were expected in the response object, but none were found."
                for facet in facets:
                    assert facet in res['facets'], 'Expected facet "{}" in response object, but it was not found'.format(facet)
            # Exhaustively test the first 10?
            for hit in res['hits'][:10]:
                # Make sure correct jsonld is in res
                if self.h.check_boolean_url_option(base_url, 'jsonld') and 'root' in jsonld_context:
                    hit = self.h.check_jsonld(hit, '')
                if self.h.parse_url(base_url, 'fields') or self.h.parse_url(base_url, 'filter'):
                    # This is a filter query, test it appropriately
                    if 'fields' in q:
                        true_fields = [x.strip() for x in self.h.parse_url(base_url, 'fields').split(',')]
                    elif 'filter' in bid:
                        true_fields = [x.strip() for x in self.h.parse_url(base_url, 'filter').split(',')]
                    total_url = self.h.api + '/' + ns.annotation_endpoint + '/' + _q(hit['_id'])
                    res_total = {}
                    res_total = self.h.json_ok(self.h.get_ok(total_url))
                    if res_total:
                        self.h.check_fields(hit, res_total, true_fields)
            # insert gibberish on first id, test msgpack
            if test_number == 0:
                res_f = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + q[:-1] + '\xef\xbf\xbd\xef\xbf\xbd' + q[-1]))
                assert res_f['hits'] == [], 'Query with non ASCII characters injected failed'
                self.h.test_msgpack(base_url)
            # extra tests
            self._extra_query_GET(q, res)
 
        # test unicode insertion
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + ns.unicode_test_string))
        assert res['hits'] == [], "GET to query endpoint failed with unicode test string"

        # test empty/error
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint), checkerror=False)
        assert 'error' in res
    
    def test_query_post(self):
        #query via post
        for (test_number, ddict) in enumerate(ns.query_POST):
            pass
        '''    
        json_ok(post_ok(api + '/query', {'q': 'rs58991260'}))

        res = json_ok(post_ok(api + '/query', {'q': 'rs58991260',
                                               'scopes': 'dbsnp.rsid'}))
        eq_(len(res), 1)
        eq_(res[0]['_id'], 'chr1:g.218631822G>A')

        res = json_ok(post_ok(api + '/query', {'q': 'rs58991260,rs2500',
                                               'scopes': 'dbsnp.rsid'}))
        eq_(len(res), 2)
        eq_(res[0]['_id'], 'chr1:g.218631822G>A')
        eq_(res[1]['_id'], 'chr11:g.66397320A>G')

        res = json_ok(post_ok(api + '/query', {'q': 'rs58991260',
                                               'scopes': 'dbsnp.rsid',
                                               'fields': 'dbsnp.chrom,dbsnp.alleles'}))
        assert len(res) == 1, (res, len(res))

        res = self.h.json_ok(self.h.post_ok(self.h.api + '/query', {}), checkerror=False)
        assert 'error' in res, res
        '''

    def test_metadata(self):
        self.h.get_ok(self.h.host + '/metadata')
        self.h.get_ok(self.h.api + '/metadata')

    def test_get_fields(self):
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/metadata/fields'))
        # Check to see if there are enough keys
        ok_(len(res) > ns.minimum_acceptable_fields)

        for field in ns.test_fields_get_fields_endpoint:
            assert field in res, '"{}" expected in response from /metadata/fields, but not found'.format(field)

    def test_status_endpoint(self):
        self.h.get_ok(self.h.host + '/status')
        # (testing failing status would require actually loading tornado app from there 
        #  and deal with config params...)

    ###########################################################################
    # Convenience functions for adding new nosetests/ don't really need these...
    ###########################################################################
    def _extra_annotation_GET(self, bid, res):
        # override to add extra annotation GET tests here
        pass

    def _extra_annotation_POST(self, ddict, res):
        # override to add extra annotation POST tests here
        pass

    def _extra_query_GET(self, q, res):
        # override to add extra query GET tests here
        pass

    def _extra_query_POST(self, ddict, res):
        # override to add extra query POST tests here
        pass
