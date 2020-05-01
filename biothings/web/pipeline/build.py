"""
    Elasticsearch Query DSL Construction
"""
import re

from elasticsearch_dsl import Q

from biothings.utils.web.es_dsl import AsyncMultiSearch, AsyncSearch
from biothings.web.handlers.exceptions import BadRequest


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
        try:  # TODO clarify
            return self._build(q, options)
        except TypeError as exc:
            raise BadRequest(
                reason='TypeError',
                value=str(exc))
        except ValueError as exc:
            raise BadRequest(
                reason='ValueError',
                details=str(exc))

    def _build(self, q, options):

        if options.scopes is not None:
            build_query = self.build_match_query
        else:  # no scopes, only q
            build_query = self.build_string_query

        if isinstance(q, list):
            search = AsyncMultiSearch()
            for _q in q:
                _search = build_query(_q, options)
                _search = self._apply_extras(_search, options)
                search = search.add(_search)
        else:  # str, int ...
            search = build_query(str(q), options)
            search = self._apply_extras(search, options)

        return search

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

        return AsyncSearch().query("query_string", query=str(q))

    def default_match_query(self, q, scopes, options):
        '''
        Override this to customize default match query.
        By default it implements a multi_match query.
        '''
        if isinstance(q, (str, int, float)):
            query = Q('multi_match', query=str(q),
                      operator="and", fields=scopes)

        elif isinstance(q, list):
            if not isinstance(scopes, list):
                raise TypeError(scopes)
            if len(q) != len(scopes):
                raise ValueError(q)

            query = Q()  # combine conditions
            for _q, _scopes in zip(q, scopes):
                query = query & Q(
                    'multi_match', query=_q,
                    operator="and", fields=_scopes)

        else:  # invalid
            raise TypeError(q)

        return AsyncSearch().query(query)

    def build_string_query(self, q, options):
        """ q + options -> query object

            options:
                userquery
        """
        assert isinstance(q, str)
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

    def build_match_query(self, q, options):
        """ q + options -> query object

            options:
                scopes - default scopes
                regexs - q -> scopes override
        """
        scopes = options.scopes or []
        if isinstance(q, str):
            for regex, scope in options.regexs or []:
                match = re.fullmatch(regex, q)
                if match:
                    q = match.groupdict().get('search_term') or q
                    scopes = scope if isinstance(scope, list) else [scope]
                    break
        return self.default_match_query(q, scopes, options)

    def _apply_extras(self, search, options):

        # add aggregations
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

        # add es params
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
