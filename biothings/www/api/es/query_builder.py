import logging
import json
from biothings.utils.common import is_seq
from biothings.utils.www.userquery import get_userquery, get_userfilter
try:
    from re import fullmatch as match
except ImportError:
    from re import match

class ESQueries(object):
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
        return self._es_query_template(query_type="multi_match", query_kwargs=query_kwargs)

    def match(self, query_kwargs):
        return self._es_query_template(query_type="match", query_kwargs=query_kwargs)

    def match_all(self, query_kwargs):
        return self._es_query_template(query_type="match_all", query_kwargs=query_kwargs)

    def query_string(self, query_kwargs):
        return self._es_query_template(query_type="query_string", query_kwargs=query_kwargs)

    def bool(self, query_kwargs):
        return self._es_query_template(query_type="bool", query_kwargs=query_kwargs)

    def raw_query(self, raw_query):
        return self._es_query_template(raw_query=raw_query)

class ESQueryBuilder(object):
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
        return self.queries.query_string({"query": q})

    def _is_match_all(self, q):
        return (q == '__all__')

    def _extra_query_types(self, q):
        ''' Override me '''
        return {}

    def _match_all(self, q):
        ''' Override me '''
        return self.queries.match_all({})

    def _is_user_query(self, text_file='query.txt'):
        try:
            query_dir = os.path.abspath(self.userquery_dir)
            return (hasattr(self.options, 'userquery') and (os.path.exists(query_dir)) and
                (os.path.isdir(query_dir)) and (os.path.exists(os.path.join(query_dir, text_file))))
        except Exception:
            return False
    
    def _user_query(self, q):
        _args = {'q': q}
        _args.update(getattr(self.options, 'userquery_kwargs', {}))
        _ret = json.loads(get_userquery(os.path.abspath(self.userquery_dir), 
                            self.options.userquery).format(**_args))
        return self.queries.raw_query(_ret)

    def _user_query_filter(self):
        return json.loads(get_userfilter(os.path.abspath(self.userquery_dir), self.options.userquery))

    def _get_query_filters(self):
        _filter = []
        if self._is_user_query(text_file='filter.txt'):
            _filter.append(self._user_query_filter())
        return _filter

    def get_query_filters(self):
        ''' Override me '''
        return self._get_query_filters()

    def add_query_filters(self, _query):
        filters = self.get_query_filters()
        if not filters:
            return _query

        #add filters as filtered query
        #this will apply to facet counts
        _query = {
            'filtered': {
                'query': _query,
                'filter': filters
            }
        }

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

        _query = self.add_query_filters(_query)

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
        return self._annotation_GET_query(bid)
    
    def annotation_POST_query(self, bids):
        return self._annotation_POST_query(bids)

    def query_GET_query(self, q):
        return self._query_GET_query(q)

    def query_POST_query(self, qs, scopes):
        return self._query_POST_query(qs, scopes)

    def metadata_query(self):
        return self._metadata_query()

    def scroll(self, scroll_id):
        return self._scroll(scroll_id)
