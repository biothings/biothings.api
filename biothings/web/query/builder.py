"""
    Biothings Query Builder

    Turn the biothings query language to that of the database.
    The interface contains a query term (q) and query options.

    Depending on the underlying database choice, the data type
    of the query term and query options vary. At a minimum,
    a query builder should support:

    q: str, a query term, it is the only required input.
    options: dotdict, optional query options.

        scopes: list[str], the fields to look for the query term.
                the meaning of scopes being an empty list or a
                None object/not provided is controlled by specific
                class implementations or not defined.

        _source: list[str], fields to return in the result.
        size: int, maximum number of hits to return.
        from_: int, starting index of result to return.
        sort: str, customized sort keys for result list

        aggs: str, customized aggregation string.
        facet_size: int, maximum number of agg results.

"""
import json
import logging
import os
import re
from copy import deepcopy
from biothings.utils.common import dotdict
from elasticsearch_dsl import MultiSearch, Q, Search
from elasticsearch_dsl.exceptions import IllegalOperation
from collections import UserString, namedtuple

class RawQueryInterrupt(Exception):
    def __init__(self, data):
        super().__init__()
        self.data = data


Query = namedtuple('Query', ('term', 'scopes'))
Group = namedtuple('Group', ('term', 'scopes'))

class QueryStringParser:

    def __init__(
            self, default_scopes=("_id", ),
            patterns=((r"(?P<scope>\w+):(?P<term>[^:]+)", ()),),
            gpnames=('term', 'scope')):
        assert isinstance(default_scopes, (tuple, list))
        assert all(isinstance(field, str) for field in default_scopes)
        self.default = default_scopes  # ["_id", "entrezgene", "ensembl.gene"]
        self.patterns = []  # [(re.compile(r'^\d+$'), ['entrezgene', 'retired'])]
        self.gpname = Group(*gpnames)  # symbolic group name for term substitution
        for pattern, fields in patterns:
            fields = [fields] if isinstance(fields, str) else fields
            assert all(isinstance(field, str) for field in fields)
            pattern = re.compile(pattern) if isinstance(pattern, str) else pattern
            assert isinstance(pattern, re.Pattern)  # TODO python cross-version compatibility
            self.patterns.append((pattern, fields))

    def parse(self, q):
        assert isinstance(q, str)
        for regex, fields in self.patterns:
            match = re.fullmatch(regex, q)
            if match:
                named_groups = match.groupdict()
                q = named_groups.get(self.gpname.term) or q
                _fields = named_groups.get(self.gpname.scopes)
                fields = [_fields] if _fields else fields or self.default
                return Query(q, fields)
        return Query(q, self.default)

class ESScrollID(UserString):
    def __init__(self, seq: object):
        super().__init__(seq)
        # scroll id cannot be empty
        assert self.data

class ESQueryBuilder():
    """
    Build an Elasticsearch query with elasticsearch-dsl
    """

    def __init__(
        self, user_query=None,  # like a prepared statement in SQL
        scopes_regexs=(),  # inference used when encountering empty scopes
        scopes_default=('_id',),  # fallback used when scope inference fails
        allow_random_query=True,  # used for data exploration, can be expensive
        allow_nested_query=False  # nested aggregation support, can be expensive
    ):

        self.user_query = user_query or ESUserQuery('userquery')
        self.string_query = QueryStringParser(scopes_default, scopes_regexs)

        self.allow_random_query = allow_random_query
        self.allow_nested_query = allow_nested_query  # for aggregations

    def build(self, q, **options):
        """
        Build a query according to q and options.
        This is the public method called by API handlers.

        Regarding multisearch:
            TODO

        Regarding scopes:
            scopes: [str] nonempty, match query.
            scopes: [] empty, infer with regex, fallback to default.
            scopes: NoneType, no scope, so query string query.

        * if 'scopes' is not provided, it is considered a NoneType
        * the differentiation between [] and None is unique to this class.

        Additionally support these options:
            explain: include es scoring information
            userquery: customized function to interpret q

        * additional keywords are passed through as es keywords
            for example: 'explain', 'version' ...

        """
        options = dotdict(options)

        if options.scroll_id:
            # bypass all query building stages
            return ESScrollID(options.scroll_id)

        try:
            # process single q vs list of q(s).
            # dispatch 'val' vs 'key:val' to corresponding functions.

            if options.scopes is not None:
                build_query = self._build_match_query
            else:  # no scopes, only q
                build_query = self._build_string_query

            if options.fetch_all:
                options.pop('sort', None)
                options.pop('size', None)

            if isinstance(q, list):
                if not q:  # es cannot execute empty multisearch
                    raise ValueError("No search terms.")
                search = MultiSearch()
                for _q in q:
                    _search = build_query(_q, options)
                    _search = self.apply_extras(_search, options)
                    search = search.add(_search)
            else:  # str, int ...
                search = build_query(q, options)
                # pass through es query options. (from, size ...)
                search = self.apply_extras(search, options)

        # except (TypeError, ValueError) as exc:
        #     raise BadRequest(reason=type(exc).__name__, details=str(exc))
        except IllegalOperation as exc:
            raise TypeError(str(exc))  # ex. sorting by -_score
            # raise BadRequest(reason=str(exc))

        if options.get('rawquery'):
            raise RawQueryInterrupt(search.to_dict())

        return search

    def _build_string_query(self, q, options):
        """ q + options -> query object

            options:
                userquery
        """
        search = Search()
        userquery = options.userquery or ''

        if q == "":  # same empty q behavior as that of ES.
            search = search.query("match_none")

        if q == '__all__' or q is None:
            search = search.query()

        elif q == '__any__' and self.allow_random_query:
            search = search.query('function_score', random_score={})

        elif self.user_query.has_query(userquery):
            userquery_ = self.user_query.get_query(userquery, q=q)
            search = search.query(userquery_)

        else:  # customization here
            search = self.default_string_query(str(q), options)

        if self.user_query.has_filter(userquery):
            userfilter = self.user_query.get_filter(userquery)
            search = search.filter(userfilter)

        return search

    def _build_match_query(self, q, options):
        """ q + options -> query object

            options:
                scopes - fields to query term q
        """
        scopes = options.scopes
        if not scopes and not isinstance(q, list):  # infer scopes
            q, scopes = self.string_query.parse(str(q))
        return self.default_match_query(q, scopes, options)

    def default_string_query(self, q, options):
        """
        Override this to customize default string query.
        By default it implements a query string query.
        """
        assert isinstance(q, str)
        return Search().query("query_string", query=q)

    def default_match_query(self, q, scopes, options):
        """
        Override this to customize default match query.
        By default it implements a multi_match query.
        """

        if isinstance(q, (str, int, float, bool)):
            query = Q('multi_match', query=q,
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

        return Search().query(query)

    def apply_extras(self, search, options):
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

class MongoQueryBuilder():

    def __init__(self, default_scopes=('_id',)):
        self.parser = QueryStringParser(default_scopes)

    def build(self, q, **options):
        fields = options.get("scopes", ())
        if not fields and q:
            q, fields = self.parser.parse(q)

        assert isinstance(fields, (list, tuple))
        assert q is None and not fields or q and isinstance(q, str)
        assert all((isinstance(field, str) for field in fields))

        filter = {
            field: 1  # project fields to return
            for field in options.get('_source', ())
        } or None

        query = {
            "$or": [
                {field: q}
                for field in fields
            ]
        } if fields else {}

        if options.get('rawquery'):
            raise RawQueryInterrupt((query, filter))

        return (query, filter)

class SQLQueryBuilder():

    # PROOF OF CONCEPT
    # INPUT NOT SANITIZED
    # INTERNAL USE ONLY

    def __init__(
            self, tables,
            default_scopes=('id',),
            default_limit=10
    ):
        assert default_scopes
        assert isinstance(default_limit, int)
        assert tables and isinstance(tables, dict)

        self.tables = tables
        self.default_limit = default_limit
        self.parser = QueryStringParser(default_scopes)

        if None not in self.tables:  # set default table
            self.tables[None] = next(iter(self.tables.values()))

    def build(self, q, **options):

        statements = [
            "SELECT {}".format(', '.join(options.get("_source", ())) or "*"),
            "FROM {}".format(self.tables[options.get("biothing_type")]),
        ]

        scopes = options.get("scopes")
        if not scopes:
            q, scopes = self.parser.parse(q)

        if scopes and q:
            assert isinstance(q, str)
            selections = ['{} = "{}"'.format(field, q) for field in scopes]
            statements.append('WHERE')
            statements.append(' OR '.join(selections))

        # limit result window
        statements.append('LIMIT {}'.format(
            options.get('size', self.default_limit)))

        if 'from_' in options:
            statements.append('OFFSET {}'.format(options['from_']))

        if options.get('rawquery'):
            raise RawQueryInterrupt(statements)

        return ' '.join(statements)


class ESUserQuery():

    def __init__(self, path):

        self._queries = {}
        self._filters = {}
        try:
            for (dirpath, dirnames, filenames) in os.walk(path):
                if dirnames:
                    self.logger.info("User query folders: %s.", dirnames)
                    continue
                for filename in filenames:
                    with open(os.path.join(dirpath, filename)) as text_file:
                        if 'query' in filename:
                            ## alternative implementation
                            # self._queries[os.path.basename(dirpath)] = text_file.read()
                            ##
                            self._queries[os.path.basename(dirpath)] = json.load(text_file)
                        elif 'filter' in filename:
                            self._filters[os.path.basename(dirpath)] = json.load(text_file)
        except Exception:
            self.logger.exception('Error loading user queries.')

    def has_query(self, named_query):

        return named_query in self._queries

    def has_filter(self, named_query):

        return named_query in self._filters

    def get_query(self, named_query, **kwargs):

        def in_place_sub(dic, kwargs):
            for key in dic:
                if isinstance(dic[key], dict):
                    in_place_sub(dic[key], kwargs)
                elif isinstance(dic[key], list):
                    for item in dic[key]:
                        in_place_sub(item, kwargs)
                elif isinstance(dic[key], str):
                    dic[key] = dic[key].format(**kwargs).format(**kwargs)  # {{q}}

        dic = deepcopy(self._queries.get(named_query))
        in_place_sub(dic, kwargs)
        key, val = next(iter(dic.items()))
        return Q(key, **val)

        ## alternative implementation
        # string = self._queries.get(named_query)
        # string1 = re.sub(r"\}", "}}", string)
        # string2 = re.sub(r"\{", "{{", string1)
        # string3 = re.sub(r'\{\{\{\{(?P<var>.*?)\}\}\}\}', r'{\g<var>}', string2)
        # return string3
        ##

    def get_filter(self, named_query):

        dic = self._filters.get(named_query)
        key, val = next(iter(dic.items()))
        return Q(key, **val)

    @property
    def logger(self):
        return logging.getLogger(__name__)
