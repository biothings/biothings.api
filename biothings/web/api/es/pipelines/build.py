"""
    Elasticsearch Query DSL Construction
"""
import re

from biothings.utils.web.es_dsl import AsyncMultiSearch, AsyncSearch
from elasticsearch_dsl import Q


class ESQueryBuilder(object):
    """
    Build an Elasticsearch query with elasticsearch-dsl
    """

    def __init__(self, web_settings):

        # for string queries
        self.user_query = web_settings.userquery
        self.allow_random_query = web_settings.ALLOW_RANDOM_QUERY

        # for aggregations
        self.allow_nested_query = web_settings.ALLOW_NESTED_AGGS

    def build(self, q, options):
        '''
        Build the corresponding query.
        '''
        if isinstance(q, list):
            msearch = AsyncMultiSearch()
            for _q in q:
                search = self.build(str(_q), options)
                msearch = msearch.add(search)
            return msearch

        if isinstance(q, str):
            if isinstance(options.scopes, list):
                search = self.build_terms_query(q, options)
            else:
                search = self.build_string_query(q, options)
            search = self._apply_extras(search, options)
            return search

        raise TypeError(type(q))

    def default_string_query(self, q, options):
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

    def default_terms_query(self, q, scopes, options):
        '''
        Override this to customize default terms query.
        By default it implements a multi_match query.
        '''
        q = Q('multi_match', query=q, fields=scopes, operator="and")
        return AsyncSearch().query(q)

    def build_string_query(self, q, options):

        search = AsyncSearch()
        userquery = options.userquery or ''

        if self.user_query.has_query(userquery):
            userquery_ = self.user_query.get_query(userquery, q=q)
            search = search.query(userquery_)

        elif q == '__all__':
            search = search.query()

        elif q == '__any__' and self.allow_random_query:
            search = search.query('function_score', random_score={})

        else:  # customization here
            search = self.default_string_query(q, options)

        if self.user_query.has_filter(userquery):
            userfilter = self.user_query.get_filter(userquery)
            search = search.filter(userfilter)

        return search

    def build_terms_query(self, q, options):

        scopes = options.scopes or []
        for regex, scope in options.regexs or []:
            match = re.match(regex, q)
            if match:
                q = match.groupdict().get('search_term') or q
                scopes = scope if isinstance(scope, list) else [scope]
                break

        return self.default_terms_query(q, scopes, options)

    def _apply_extras(self, search, options):

        facet_size = options.facet_size or 10

        for agg in options.aggs or []:
            term, bucket = agg, search.aggs
            while term:
                if self.allow_nested_query and \
                        '(' in term and term.endswith(')'):
                    _term, term = term[:-1].split('(', 1)
                else:
                    _term, term = term, ''
                bucket = bucket.bucket(
                    _term, 'terms', field=_term, size=facet_size)

        if isinstance(options.sort, list):
            # accept '-' prefixed field names
            search = search.sort(*options.sort)

        if isinstance(options._source, list):
            if 'all' not in options._source:
                search = search.source(options._source)

        for key, value in options.items():
            if key in ('from', 'size', 'explain', 'version'):
                search = search.extra(**{key: value})

        return search
