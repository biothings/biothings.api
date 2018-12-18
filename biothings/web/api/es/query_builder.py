import logging
import json
import os
from biothings.utils.common import is_seq
from biothings.utils.web.userquery import get_userquery, get_userfilter
try:
    from re import fullmatch as match
except ImportError:
    from re import match

class ESQueries(object):
    ''' A very simple class to object-ize Elasticsearch Query DSL.
    This should be replaced by the official `Elasticsearch equivalent <https://pypi.python.org/pypi/elasticsearch-dsl>`_.
    Also contains a simple JSON validator after generating all queries (dump to string and re-read) '''
    def __init__(self, es_kwargs={}):
        self.es_kwargs = es_kwargs

    def _validate_json(self, d):
        ''' dumps the dict to string and reads back in as json - should validate the json structure? '''
        try:
            return json.loads(json.dumps(d))
        except:
            raise ValueError('Could not validate query "{}"'.format(d))

    def _es_query_template(self, query_type=None, query_kwargs=None, raw_query={}):
        _ret = raw_query if raw_query else {"query":{query_type: query_kwargs}}
        _ret.update(self.es_kwargs)
        return self._validate_json(_ret)

    def multi_match(self, query_kwargs):
        ''' Given ``query_kwargs``, validate and return a **multi_match** query. '''
        return self._es_query_template(query_type="multi_match", query_kwargs=query_kwargs)

    def match(self, query_kwargs):
        ''' Given ``query_kwargs``, validate and return a **match** query. '''
        return self._es_query_template(query_type="match", query_kwargs=query_kwargs)

    def match_all(self, query_kwargs):
        ''' Given ``query_kwargs``, validate and return a **match_all** query. '''
        return self._es_query_template(query_type="match_all", query_kwargs=query_kwargs)

    def query_string(self, query_kwargs):
        ''' Given ``query_kwargs``, validate and return a **query_string** query. '''
        return self._es_query_template(query_type="query_string", query_kwargs=query_kwargs)

    def bool(self, query_kwargs):
        ''' Given ``query_kwargs``, validate and return a **bool** query. '''
        return self._es_query_template(query_type="bool", query_kwargs=query_kwargs)

    def raw_query(self, raw_query):
        ''' Given ``query_kwargs``, validate and return a *raw* query (queries that don't fit the same query_template). '''
        return self._es_query_template(raw_query=raw_query)

class ESQueryBuilder(object):
    ''' Class to return the correct query given the request endpoint, method, and URL params.

    :param index: The Elasticsearch index to run the query on 
    :param doc_type: The Elasticsearch document type of the query
    :param options: Options from the URL string relevant to query building 
    :param es_options: Options for Elasticsearch query stage 
    :param scroll_options: Options for scroll requests
    :param regex_list: A list of (regex, scope) tuples for annotation lookup
    :param userquery_dir: The directory containing user queries for this app
    :param default_scopes: A list representing the default Elasticsearch query scope(s) for this query'''
    def __init__(self, index, doc_type, options, es_options, scroll_options={}, 
                       userquery_dir='', regex_list=[], default_scopes=['_id']):
        self.index = index
        self.doc_type = doc_type
        self.options = options
        self.es_options = es_options
        self.scroll_options = scroll_options
        self.regex_list = regex_list
        self.userquery_dir = userquery_dir
        self.default_scopes = default_scopes
        self.queries = ESQueries(es_options)

    def _return_query_kwargs(self, query_kwargs):
        _kwargs = {"index": self.index, "doc_type": self.doc_type}
        _kwargs.update(query_kwargs)
        return _kwargs 

    def _get_term_scope(self, term):
        _scopes = None
        for (regex, scope) in self.regex_list:
            if match(regex, term):
                _scopes = scope
                break
        return _scopes
    
    def _build_single_query(self, term, scopes=None):
        scopes = scopes or self.default_scopes
        if len(scopes) == 1:
            return self.queries.match({scopes[0]:{"query": "{}".format(term), "operator": "and"}})
        else:
            return self.queries.multi_match({"query":"{}".format(term), "fields":scopes, "operator":"and"})

    def _build_multiple_query(self, terms, scopes=None):
        _q = []
        _infer_scope = True if not scopes else False
        for term in terms:
            if _infer_scope:
                scopes = self._get_term_scope(term)
            _q.extend(['{}', json.dumps(self._build_single_query(term, scopes=scopes))])
        return self._return_query_kwargs({'body': '\n'.join(_q)})

    def _default_query(self, q):
        ''' Override me '''
        return ESQueries().query_string({"query": q})

    def _is_match_all(self, q):
        return (q == '__all__')

    def _extra_query_types(self, q):
        ''' Override me '''
        return {}

    def _match_all(self, q):
        ''' Override me '''
        return ESQueries().match_all({})

    def _is_user_query(self, text_file='query.txt'):
        try:
            query_dir = os.path.join(os.path.abspath(self.userquery_dir), self.options.userquery)
            return (os.path.exists(query_dir) and (os.path.isdir(query_dir)) and 
                    (os.path.exists(os.path.join(query_dir, text_file))))
        except Exception:
            return False
    
    def _user_query(self, q):
        _args = {'q': q}
        if self.options.userquery_kwargs:
            _args.update(self.options.userquery_kwargs)
        _ret = json.loads(get_userquery(os.path.abspath(self.userquery_dir), 
                            self.options.userquery).format(**_args))
        return ESQueries().raw_query(_ret)

    def _user_query_filter(self):
        return json.loads(get_userfilter(os.path.abspath(self.userquery_dir), self.options.userquery))

    def _get_query_filters(self):
        _filter = []
        if self._is_user_query(text_file='filter.txt'):
            _filter.append(self._user_query_filter())
        return _filter

    def _get_missing_filters(self):
        return []

    def get_query_filters(self):
        ''' Override me to add more query filters '''
        return self._get_query_filters()

    def get_missing_filters(self):
        ''' Override me to add more must_not filters '''
        return self._get_missing_filters()

    def add_extra_filters(self, q):
        ''' Override me to add more filters '''
        return q

    def add_query_filters(self, _query):
        ''' Given a query, add any other filters '''
        filters = self.get_query_filters()
        missing = self.get_missing_filters()
        if not filters and not missing:
            return _query

        #add filters as filtered query
        #this will apply to facet counts
        _query = {
            'bool': {
                'must': _query.get('query', _query)
            }
        }
        if filters:
            _query['bool']['filter'] = filters
        if missing:
            _query['bool']['must_not'] = missing

        return _query
    
    def _scroll(self, scroll_id):
        return {'body': {'scroll_id': scroll_id}, 'scroll': self.scroll_options.get('scroll', '1m')}

    def _annotation_GET_query(self, bid):
        _scopes = self._get_term_scope(bid)
        if _scopes:
            return self._return_query_kwargs({'body': self._build_single_query(bid, scopes=_scopes)})
        else:
            # go to es.get
            _get_kwargs = {'id': bid}
            _get_kwargs.update(self.es_options)
            return self._return_query_kwargs(_get_kwargs)
    
    def _annotation_POST_query(self, bids):
        return self._build_multiple_query(terms=bids)

    def _query_GET_query(self, q):
        if self._is_user_query():
            _query = self._user_query(q)
        elif self._is_match_all(q):
            _query = self._match_all(q)
        else:
            _query = self._extra_query_types(q)

        if not _query:
            _query = self._default_query(q)

        _query['query'] = self.add_query_filters(_query)
        _query = self.add_extra_filters(_query)

        _query = self.queries.raw_query(_query)

        _ret = self._return_query_kwargs({'body': _query})

        if self.options.fetch_all:
            _ret['body'].pop('sort', None)  # don't allow sorting for fetch all, defeats the purpose
            _ret['body'].pop('size', None)
            _ret.update(self.scroll_options)
        return _ret

    def _query_POST_query(self, qs, scopes):
        return self._build_multiple_query(terms=qs, scopes=scopes)

    def _metadata_query(self):
        return self._return_query_kwargs({})

    def annotation_GET_query(self, bid):
        ''' Return an annotation lookup GET query for this ``bid``. 

        :param bid: Biothing ID, used to lookup the annotation'''
        return self._annotation_GET_query(bid)
    
    def annotation_POST_query(self, bids):
        ''' Return an annotation lookup POST query for these ``bids``. 

        :param bids: Biothing IDs, used to lookup the annotations'''
        return self._annotation_POST_query(bids)

    def query_GET_query(self, q):
        ''' Return a query endpoint GET query for this query string ``q``. 

        :param q: query string specifying the query'''
        return self._query_GET_query(q)

    def query_POST_query(self, qs, scopes):
        ''' Return query endpoint POST queries for these query strings ``qs``. 

        :param qs: Query strings to query
        :param scopes: Scope of query strings ``qs``'''
        return self._query_POST_query(qs, scopes)

    def metadata_query(self):
        ''' Return a metadata query ''' 
        return self._metadata_query()

    def scroll(self, scroll_id):
        ''' Return the next batch of results from a *scroll* query. 

        :param scroll_id: ID of the batch to yield hits from '''
        return self._scroll(scroll_id)
