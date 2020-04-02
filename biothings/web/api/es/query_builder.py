"""
    Elasticsearch Query DSL Construction
"""
from biothings.web.api.es.query import AsyncMultiSearch, AsyncSearch

try:
    from re import fullmatch as match
except ImportError:
    from re import match


def get_term_scope(term, regex_list):
    """
    Process ANNOTATION_ID_REGEX_LIST
    """
    _scopes = None
    _term = term
    for (regex, scope) in regex_list:
        r = match(regex, term)
        if r:
            if r.groupdict() and 'search_term' in r.groupdict(
            ) and r.groupdict()['search_term']:
                _scopes = scope
                _term = r.groupdict()['search_term']
                break
            else:
                _scopes = scope
                break

    return _term, _scopes

class ESQueryBuilder(object):
    """
    Build an Elasticsearch query with elasticsearch-dsl
    """

    def __init__(self, web_settings):

        # for string queries
        self.user_query = web_settings.userquery
        self.allow_random_query = web_settings.ALLOW_RANDOM_QUERY
        self.allow_nested_query = web_settings.ALLOW_NESTED_AGGS

        # for term queries
        self.regex_list = web_settings.ANNOTATION_ID_REGEX_LIST
        self.default_scopes = web_settings.DEFAULT_SCOPES

    def build(self, options):
        '''
        Build the corresponding query.
        May override to add more. Handle uncaught exceptions.
        '''
        if 'bid' in options:
            return self.build_terms_query([options.bid])[0]
        if 'ids' in options:
            return self.build_terms_query(options.ids, options.scopes)
        if 'q' in options:
            if isinstance(options.q, list):
                return self.build_terms_query(options.q, options.scopes)
            if isinstance(options.q, str):
                return self.build_string_query(
                    options.q, options.userquery, options.aggs, options.facet_size)

    def build_default_query(self, q):
        '''
        Override this to customize default string query.
        By default it implements a query string query.
        '''

        ## for extra query types:
        #
        # if q == 'case_1':
        #    return case_1(q)
        # elif q == 'case_2':
        #    return case_2(q)
        #
        # return default_case(q)

        return AsyncSearch().query("query_string", query=q)

    def build_string_query(self, q, userquery, aggs, facet_size):
        '''
        Return a query endpoint GET query for this query string ``q``.

        :param q: query string specifying the query
        '''

        search = AsyncSearch()

        if self.user_query.has_query(userquery):
            userquery_ = self.user_query.get_query(userquery, q=q)
            search = search.query(userquery_)

        elif q == '__all__':
            search = search.query()

        elif q == '__any__' and self.allow_random_query:
            search = search.query('function_score', random_score={})

        else:  # customization here
            search = self.build_default_query(q)

        if self.user_query.has_filter(userquery):
            userfilter = self.user_query.get_filter(userquery)
            search = search.filter(userfilter)

        for agg in aggs or []:
            term, bucket = agg, search.aggs
            while term:
                if self.allow_nested_query and \
                        '(' in term and term.endswith(')'):
                    _term, term = term[:-1].split('(', 1)
                else:
                    _term, term = term, ''
                bucket = bucket.bucket(
                    _term, 'terms', field=_term, size=facet_size)

        return search

    def build_terms_query(self, qs, scopes=None):
        '''
        Return query endpoint POST queries for these query strings ``qs``.

        :param qs: Query strings to query
        :param scopes: Scope of query strings ``qs``
        '''

        assert isinstance(qs, list)

        scopes = scopes or self.default_scopes
        msearch = AsyncMultiSearch()

        for q in qs:
            q, scopes_ = get_term_scope(q, self.regex_list)
            scopes = scopes_ or scopes
            search = AsyncSearch().query('multi_match', query=q, fields=scopes, operator="and")
            msearch = msearch.add(search)

        return msearch
