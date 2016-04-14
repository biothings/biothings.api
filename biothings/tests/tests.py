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

try:
    jsonld_context = json.load(open(ns.jsonld_context_path, 'r'))
except:
    sys.stderr.write("Couldn't load JSON-LD context.")
    jsonld_context = {}


class BiothingTestHelper:
    def __init__(self):
        self.host = os.getenv(ns.nosetest_envar)
        if not self.host:
            self.host = ns.nosetest_default_url
        self.host = self.host.rstrip('/')
        self.api = self.host + '/' + ns.api_version
        self.h = httplib2.Http()
        self.all_flattened_fields = self.json_ok(self.get_ok(self.api + '/metadata/fields'))

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

    def msgpack_ok(b, checkerror=True):
        d = msgpack.unpackb(b)
        if checkerror:
            ok_(not (isinstance(d, dict) and 'error' in d), self.truncate(str(d), 100))
        return d

    def get_ok(self, url):
        res, con = self.h.request(url)
        eq_(res.status, 200)
        return con

    def get_404(self, url):
        res, con = self.h.request(url)
        eq_(res.status, 404)

    def get_405(self, url):
        res, con = self.h.request(url)
        eq_(res.status, 405)

    def head_ok(self, url):
        res, con = self.h.request(url, 'HEAD')
        eq_(res.status, 200)

    def post_ok(self, url, params):
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        res, con = self.h.request(url, 'POST', urlencode(encode_dict(params)), headers=headers)
        eq_(res.status, 200)
        return con
    
    def has_hits(self, q):
        d = self.json_ok(self.get_ok(self.api + '/' + ns.query_endpoint + '?q=' + q))
        ok_(d.get('total', 0) > 0 and len(d.get('hits', [])) > 0)
        return d

    def check_boolean_url_option(self, url, option):
            if [1 for f in urlparse(url).query.split('&') 
                    if ((f.split('=')[0] == option) and 
                        (f.split('=')[1].lower() in [1, 'true']))]:
                return True
            return False

    def check_fields(self, d, f):
        def expand_requested_fields(fields):
            # find all possible fields from the request
            possible_fields = []
            if fields[0] == 'all':
                return self.all_flattened_fields.keys()
            for field in fields:
                possible_fields += [s for s in self.all_flattened_fields.keys() 
                                    if s.startswith(field)]
            return possible_fields
                
        def flatten_dict(d, p, r):
            if isinstance(d, list):
                for i in d:
                    flatten_dict(i, p)
            elif isinstance(d, dict):
                # Add these keys
                for k in d.keys():
                    if p:
                        r[p + '.' + k] = 0
                    else:
                        r[k] = 0
                    flatten_dict(d[k], p + '.' + k)
        
        possible_fields = expand_requested_fields(f)
        actual_flattened_keys = {}
        flatten_dict(d , '', actual_flattened_keys)
        # Make sure that all of the actual keys are among the set of requested fields 
        assert set(actual_flattened_keys.keys()).issubset(set(possible_fields + ['_id', '_version', 'query']))
        # Also make sure that the difference between the actual keys and the possible keys is
        # nothing, i.e. a field wasn't returned that wasn't requested
        assert eq_(len(set(actual_flattened_keys.keys()).difference(set(possible_fields + ['_id', '_version', 'query']))), 0) 

    def check_jsonld(self, d, k):
        # recursively test for jsonld context
        if isinstance(d, list):
            for i in d:
                self.check_jsonld(i, k)
        elif isinstance(d, dict):
            if not k and 'root' in jsonld_context:
                assert '@context' in d
                eq_(jsonld_context['root']['@context'], d['@context'])
                del(d['@context'])
                for (tk, tv) in d.items():
                    self.check_jsonld(tv, tk)
            elif k in jsonld_context:
                assert '@context' in d
                eq_(jsonld_context[k]['@context'], d['@context'])
                del(d['@context'])
                for (tk, tv) in d.items():
                    self.check_jsonld(tv, k + '/' + tk)

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
        # Check some ids to make sure the resulting _id matches
        for bid in ns.annotation_GET:
            base_url = self.h.api + '/' + ns.annotation_endpoint + '/' + bid
            res = self.h.json_ok(self.h.get_ok(base_url))
            eq_(res['_id'], bid.split('?')[0])
            # Is this a jsonld query?
            if self.h.check_boolean_url_option(base_url, 'jsonld') and 'root' in jsonld_context:
                self.h.check_jsonld(res, '') 
            if 'fields' in bid or 'filter' in bid:
                # This is a filter query, test it appropriately
                if 'fields' in bid:
                    true_fields = [g.strip() for g in [f.split('=')[1] for f in urlparse(base_url).query.split('&') if f.split('=')[0] == 'fields'][0].split(',')]
                elif 'filter' in bid:
                    true_fields = [g.strip() for g in [f.split('=')[1] for f in urlparse(base_url).query.split('&') if f.split('=')[0] == 'filter'][0].split(',')]
                self.h.check_fields(res, true_fields)

        # testing non-ascii character
        self.h.get_404(self.h.api + '/' + ns.annotation_endpoint + '/' + ns.test_na_annotation[:-1] + '\xef\xbf\xbd\xef\xbf\xbd' + ns.test_na_annotation[-1])
        # test empties 
        self.h.get_404(self.h.api + '/' + ns.annotation_endpoint)
        self.h.get_404(self.h.api + '/' + ns.annotation_endpoint + '/')

        # override to add more tests
        self._extra_annotation_GET()
    
    def test_annotation_POST(self):
        # Test some simple POSTs to the annotation endpoint.
        for ddict in ns.annotation_POST:
            res = self.h.json_ok(self.h.post_ok(self.h.api + '/' + ns.annotation_endpoint, ddict))
            returned_ids = [h['_id'] for h in res]
            # Check that the number of returned objects matches the number of inputs
            # Probably not needed given the next test         
            eq_(len(res), len(ddict['ids'].split(',')))
            # Check that all of the supplied ids are in the returned ids list
            for bid in [g.strip() for g in ddict['ids'].split(',')]:
                assert bid in returned_ids
            # If its a filtered query, check the return objects fields
            if 'filter' in ddict or 'fields' in ddict: 
                for o in res:
                    true_fields = []
                    if 'fields' in ddict:
                        true_fields = [f.strip() for f in ddict.get('fields').split(',')]
                    elif 'filter' in ddict:
                        true_fields = [f.strip() for f in ddict.get('filter').split(',')]
                    # check root level
                    eq_(set(o), set(['_id', '_score', 'query'] + [x.split('.')[0] for x in true_fields]))
                    for f in true_fields:
                        self.h.check_nested_fields(res.items(), f)
            # If it's a jsonld query, check that
            if 'jsonld' in ddict and ddict['jsonld'].lower() in [true, 1]:
                for o in res:
                    self.check_jsonld(o, '')

        self._extra_annotation_POST()

    '''
    def test_query_GET(self):
        # Test some simple GETs to the query endpoint, first check some queries to make sure they return some hits
        for q in ns.query_GET:
            base_url = self.h.api + '/' + ns.query_endpoint + '/?q=' + q
            res = self.h.has_hits(q)
            total = res['total']
            ret_size = len(res.get('hits', []))
            req_size = [g.strip() for g in [f.split('=')[1] for f in urlparse(base_url).query.split('&') if
            if 'fields' in q or 'filter' in q:
                # This is a filter query, test it appropriately
                if 'fields' in q:
                    true_fields = [g.strip() for g in [f.split('=')[1] for f in urlparse(base_url).query.split('&') if f.split('=')[0] == 'fields'][0].split(',')]
                elif 'filter' in bid:
                    true_fields = [g.strip() for g in [f.split('=')[1] for f in urlparse(base_url).query.split('&') if f.split('=')[0] == 'filter'][0].split(',')]
                # Check root level
                eq_(set(res), set(['_id', '_version'] + [x.split('.')[0] for x in true_fields]))
                # Check nested fields
                for f in true_fields:
                    self.h.check_nested_fields(res.items(), f)
            # Is this a jsonld query?
            if [1 for f in urlparse(base_url).query.split('&') if ((f.split('=')[0] == 'jsonld') 
                                and (f.split('=')[1].lower() in [1, 'true']))] and 'root' in jsonld_context:
                self.h.check_jsonld(res, '')
            
            
        

        # Test a query with some callback
        con = self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + ns.callback_query + '&callback=mycallback')
        ok_(con.startswith('mycallback('.encode('utf-8')))

        # testing non-ascii character
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + ns.test_na_query[:-1] + '\xef\xbf\xbd\xef\xbf\xbd' + ns.test_na_query[-1]))
        eq_(res['hits'], [])

        # test empty/error
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint), checkerror=False)
        assert 'error' in res

        self._extra_query_GET()
    
    def test_query_post(self):
        #query via post
        for ( id_list, ddict ) in ns.query_POST:

            
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

    def test_query_size(self):
        # TODO: port other tests (refactor to biothing.api ?)
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + ns.test_query_size))
        eq_(len(res['hits']), 10) # default
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + ns.test_query_size + '&size=1000'))
        eq_(len(res['hits']), 1000)
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + ns.test_query_size + '&size=1001'))
        eq_(len(res['hits']), 1000)
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/' + ns.query_endpoint + '?q=' + ns.test_query_size + '&size=2000'))
        eq_(len(res['hits']), 1000)

    def test_metadata(self):
        self.h.get_ok(self.h.host + '/metadata')
        self.h.get_ok(self.h.api + '/metadata')
    '''
    def test_query_facets(self):
        res = json_ok(get_ok(api + '/query?q=cadd.gene.gene_id:ENSG00000113368&facets=cadd.polyphen.cat&size=0'))
        assert 'facets' in res and 'cadd.polyphen.cat' in res['facets']

    def test_unicode(self):
        s = '基因'

        get_404(api + '/variant/' + s)

        res = json_ok(post_ok(api + '/variant', {'ids': s}))
        eq_(res[0]['notfound'], True)
        eq_(len(res), 1)
        res = json_ok(post_ok(api + '/variant', {'ids': 'rs2500, ' + s}))
        eq_(res[1]['notfound'], True)
        eq_(len(res), 2)

        res = json_ok(get_ok(api + '/query?q=' + s))
        eq_(res['hits'], [])

        res = json_ok(post_ok(api + '/query', {"q": s, "scopes": 'dbsnp'}))
        eq_(res[0]['notfound'], True)
        eq_(len(res), 1)

        res = json_ok(post_ok(api + '/query', {"q": 'rs2500+' + s}))
        eq_(res[1]['notfound'], True)
        eq_(len(res), 2)


    def test_get_fields(self):
        res = self.h.json_ok(self.h.get_ok(self.h.api + '/metadata/fields'))
        # Check to see if there are enough keys
        ok_(len(res) > 480)

        # Check some specific keys
        assert 'cadd' in res
        assert 'dbnsfp' in res
        assert 'dbsnp' in res
        assert 'wellderly' in res
        assert 'clinvar' in res


    def test_fetch_all(self):
        res = json_ok(get_ok(api + '/query?q=_exists_:wellderly%20AND%20cadd.polyphen.cat:possibly_damaging&fields=wellderly,cadd.polyphen&fetch_all=TRUE'))
        assert '_scroll_id' in res

        # get one set of results
        res2 = json_ok(get_ok(api + '/query?scroll_id=' + res['_scroll_id']))
        assert 'hits' in res2
        ok_(len(res2['hits']) == 1000)

    def test_msgpack(self):
        res = json_ok(get_ok(api + '/variant/chr11:g.66397320A>G'))
        res2 = msgpack_ok(get_ok(api + '/variant/chr11:g.66397320A>G?msgpack=true'))
        ok_(res, res2)

        res = json_ok(get_ok(api + '/query?q=rs2500'))
        res2 = msgpack_ok(get_ok(api + '/query?q=rs2500&msgpack=true'))
        ok_(res, res2)

        res = json_ok(get_ok(api + '/metadata'))
        res2 = msgpack_ok(get_ok(api + '/metadata?msgpack=true'))
        ok_(res, res2)

    def test_jsonld(self):
        res = json_ok(get_ok(api + '/variant/chr11:g.66397320A>G?jsonld=true'))
        assert '@context' in res

        # Check some subfields
        assert 'snpeff' in res and '@context' in res['snpeff']

        assert 'ann' in res['snpeff'] and '@context' in res['snpeff']['ann'][0]

        # Check a post with jsonld
        res = json_ok(post_ok(api + '/variant', {'ids': 'chr16:g.28883241A>G, chr11:g.66397320A>G', 'jsonld': 'true'}))
        for r in res:
            assert '@context' in r

        # Check a query get with jsonld
        res = json_ok(get_ok(api + '/query?q=_exists_:clinvar&fields=clinvar&size=1&jsonld=true'))

        assert '@context' in res['hits'][0]

        # subfields
        assert 'clinvar' in res['hits'][0] and '@context' in res['hits'][0]['clinvar']
        assert 'gene' in res['hits'][0]['clinvar'] and '@context' in res['hits'][0]['clinvar']['gene']

        # Check query post with jsonld
        res = json_ok(post_ok(api + '/query', {'q': 'rs58991260,rs2500',
                                               'scopes': 'dbsnp.rsid',
                                               'jsonld': 'true'}))

        assert len(res) == 2
        assert '@context' in res[0] and '@context' in res[1]
        assert 'snpeff' in res[1] and '@context' in res[1]['snpeff']
        assert 'ann' in res[1]['snpeff'] and '@context' in res[1]['snpeff']['ann'][0]
    '''
    def test_status_endpoint(self):
        self.h.get_ok(self.h.host + '/status')
        # (testing failing status would require actually loading tornado app from there 
        #  and deal with config params...)

    ###########################################################################
    # Convenience functions for adding new nosetests/ don't really need these...
    ###########################################################################
    def _extra_annotation_GET(self):
        # override to add extra annotation GET tests here
        pass

    def _extra_annotation_POST(self):
        # override to add extra annotation POST tests here
        pass
