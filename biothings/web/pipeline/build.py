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
        """
        Build a query according to q and options.
        This is the public method called by API handlers.

        Options:

            q: string query or queries
            scopes: fields to query q(s)

                _source: fields to return
                size: maximum number of hits to return
                from: starting index of result list to return
                sort: customized sort keys for result list
                explain: include es scoring information
                userquery: customized function to interpret q
                regexs: substitution groups to infer scopes

            aggs: customized aggregation string
            facet_size: maximum number of agg results

            * additional es keywords are passed through
              for example: 'explain', 'version' ...

        """
        try:
            # process single q vs list of q(s).
            # dispatch 'val' vs 'key:val' to corresponding functions.

            if options.scopes is not None:
                build_query = self._build_match_query
            else:  # no scopes, only q
                build_query = self._build_string_query

            if isinstance(q, list):
                search = AsyncMultiSearch()
                for _q in q:
                    _search = build_query(_q, options)
                    _search = self._apply_extras(_search, options)
                    search = search.add(_search)
            else:  # str, int ...
                search = build_query(str(q), options)
                # pass through es query options. (from, size ...)
                search = self._apply_extras(search, options)

        except TypeError as exc:
            raise BadRequest(reason='TypeError', value=str(exc))
        except ValueError as exc:
            raise BadRequest(reason='ValueError', details=str(exc))
        else:
            return search

    def default_string_query(self, q, options):
        """
        Override this to customize default string query.
        By default it implements a query string query.
        """
        search = AsyncSearch()

        if q == '__all__':
            search = search.query()

        elif q == '__any__' and self.allow_random_query:
            search = search.query('function_score', random_score={})

        else:  # elasticsearch default
            search = search.query("query_string", query=str(q))

        return search

    def default_match_query(self, q, scopes, options):
        """
        Override this to customize default match query.
        By default it implements a multi_match query.
        """
        if isinstance(q, (str, int, float)):
            query = Q('multi_match', query=str(q),
                      operator="and", fields=scopes,
                      lenient=True)

        elif isinstance(q, list):
            if not isinstance(scopes, list):
                raise TypeError(scopes)
            if len(q) != len(scopes):
                raise ValueError(q)

            query = Q()  # combine conditions
            for _q, _scopes in zip(q, scopes):
                query = query & Q(
                    'multi_match', query=_q,
                    operator="and", fields=_scopes,
                    lenient=True)

        else:  # invalid
            raise TypeError(q)

        return AsyncSearch().query(query)

    def _build_string_query(self, q, options):
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

        else:  # customization here
            search = self.default_string_query(q, options)

        if self.user_query.has_filter(userquery):
            userfilter = self.user_query.get_filter(userquery)
            search = search.filter(userfilter)

        return search

    def _build_match_query(self, q, options):
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
        """
        Process non-query options and customize their behaviors.
        Customized aggregation syntax string is translated here.
        """

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
