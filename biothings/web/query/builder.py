"""
    Biothings Query Builder

    Turn the biothings query language to that of the database.
    The interface contains a query term (q) and query options.

    Depending on the underlying database choice, the data type
    of the query term and query options vary. At a minimum,
    a query builder should support:

    q: str, a query term, 
        when not provided, always perform a match all query.
        when provided as an empty string, always match none.

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
from collections import UserString, namedtuple
from copy import deepcopy
from random import randrange

from biothings.utils.common import dotdict
from elasticsearch_dsl import MultiSearch, Q, Search
from elasticsearch_dsl.exceptions import IllegalOperation


class RawQueryInterrupt(Exception):
    def __init__(self, data):
        super().__init__()
        self.data = data


Query = namedtuple('Query', ('term', 'scopes'))
Group = namedtuple('Group', ('term', 'scopes'))

class QStringParser:

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
            if hasattr(re, 'Pattern'):  # TODO remove for python>3.7
                assert isinstance(pattern, re.Pattern)
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

#
#             ES Query Builder Architecture
#-------------------------------------------------------
#                         build
#                 (support multisearch)
#--------------------------↓↓↓--------------------------
#                        _build_one
#               (dispatch basing on scopes)
#------------↓↓↓------------------------↓↓↓-------------
#    _build_string_query    |  _build_match_query
#  (__all__, userquery,..)  | (compound match query)
#------------↓↓↓------------------------↓↓↓-------------
#    default_string_query   |   default_match_query
#  (map to ES query string) | (map to ES match query)
#-------------------------------------------------------


class ESQueryBuilder():
    """
    Build an Elasticsearch query with elasticsearch-dsl.
    """
    # Different from other query pipelines, elasticsearch
    # supports querystring query, which means we can directly
    # dispatch queires without fields to querystring query,
    # and those with fields specified to typical match queries.

    def __init__(
        self, user_query=None,  # like a prepared statement in SQL
        scopes_regexs=(),  # inference used when encountering empty scopes
        scopes_default=('_id',),  # fallback used when scope inference fails
        allow_random_query=True,  # used for data exploration, can be expensive
        allow_nested_query=False,  # nested aggregation can be expensive
        metadata=None  # access to data like total number of documents
    ):
        # for autoscope feature, to infer scope from q when enabled
        self.parser = QStringParser(scopes_default, scopes_regexs)

        # all settings below affect only query string queries
        self.user_query = user_query or ESUserQuery('userquery')
        self.allow_random_query = allow_random_query
        self.allow_nested_query = allow_nested_query  # for aggregations

        # currently metadata is only used for __any__ query
        self.metadata = metadata

    def build(self, q=None, **options):
        """
        Build a query according to q and options.
        This is the public method called by API handlers.

        Regarding multisearch:
            TODO

        Regarding scopes:
            scopes: [str] nonempty, match query.
            scopes: NoneType, or [], no scope, so query string query.

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

        if options.fetch_all:
            # clean up conflicting parameters
            options.pop('sort', None)
            options.pop('size', None)

        try:
            # process single q vs list of q(s).
            # dispatch 'val' vs 'key:val' to corresponding functions.

            if isinstance(q, list):
                search = MultiSearch()
                for _q in q:
                    _search = self._build_one(_q, options)
                    search = search.add(_search)
            else:  # str, int ...
                search = self._build_one(q, options)

        except IllegalOperation as exc:
            raise ValueError(str(exc))  # ex. sorting by -_score

        if options.get('rawquery'):
            raise RawQueryInterrupt(search.to_dict())

        return search

    def _build_one(self, q, options):
        # a single query, possibly included in a multi-search
        # later but it itself is a single query unit.

        if options.scopes:
            search = self._build_match_query(q, options.scopes, options)
        elif not isinstance(q, (list, tuple)) and options.autoscope:
            q, scopes = self.parser.parse(str(q))
            search = self._build_match_query(q, scopes, options)
        else:  # no scope provided and cannot derive from q
            search = self._build_string_query(q, options)

        # pass through es query options. (from, size ...)
        search = self.apply_extras(search, options)
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

        elif q == '__all__' or q is None:
            search = search.query()

        elif q == '__any__':
            if self.allow_random_query:
                search = search.query('function_score', random_score={})
            else:  # pseudo random by overriding 'from' value
                try:
                    metadata = self.metadata[options.biothing_type]
                    total = metadata['stats']['total']
                    from_ = randrange(total - options.get('size', 0))
                    options['from'] = from_ if from_ >= 0 else 0
                except Exception:
                    raise ValueError("random query not available.")

        elif self.user_query.has_query(userquery):
            userquery_ = self.user_query.get_query(userquery, q=q)
            search = search.query(userquery_)

        else:  # customization here
            search = self.default_string_query(str(q), options)

        if self.user_query.has_filter(userquery):
            userfilter = self.user_query.get_filter(userquery)
            search = search.filter(userfilter)

        return search

    def _build_match_query(self, q, scopes, options):
        """ q + scopes + options -> query object

            case 1: 
                # single match query
                q = "1017"
                scopes = ["_id"] or "_id"

            case 2:
                # compound match query
                q = ["1017", "CDK2"]
                scopes = [["_id", "entrezgene"], "symbol"]
        """

        if not isinstance(q, (list, tuple)):
            q, scopes = [q], [scopes]

        # considering the complexity of data types,
        # for example, q can take the type of int, bool, and float,
        # maybe it's better to let elasticsearch or its python package
        # handle the type checking. the checks below represent a
        # typical case but is inconclusive.

        # if not all((
        #         isinstance(q, (list, tuple)),
        #         all(isinstance(_q, str) for _q in q))):
        #     raise TypeError("Expect q: Union[list[str], str].", q)

        # if not all((
        #         isinstance(scopes, (list, tuple)),
        #         all(isinstance(_s, (list, tuple, str)) for _s in scopes))):
        #     raise TypeError("Expect scopes: list[Union[list[str], str]].", scopes)

        if not len(q) == len(scopes):
            raise ValueError("Expect len(q) == len(scopes).")

        # additional uncommon type errors
        # will be raised in elasticsearch

        search = Search()
        for _q, _scopes in zip(q, scopes):
            if not (_q and _scopes):
                raise ValueError("No search terms or scopes.")
            _search = self.default_match_query(_q, _scopes, options)
            search = search.query(_search.query)
        return search

    def default_string_query(self, q, options):
        """
        Override this to customize default string query.
        By default it implements a query string query.
        """
        assert isinstance(q, str) and q
        assert not options.scopes
        return Search().query(
            "query_string", query=q, default_operator="AND"
        )

    def default_match_query(self, q, scopes, options):
        """
        Override this to customize default match query.
        By default it implements a multi_match query.
        """
        assert isinstance(q, (str, int, float, bool))
        assert isinstance(scopes, (list, tuple, str)) and scopes
        return Search().query(
            'multi_match', query=q, fields=scopes,
            operator="and", lenient=True
        )

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
        for key in ('from', 'size', 'explain', 'version'):
            if key in options:
                search = search.extra(**{key: options[key]})

        return search

class MongoQueryBuilder():

    def __init__(self, default_scopes=('_id',)):
        self.parser = QStringParser(default_scopes)

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
        self.parser = QStringParser(default_scopes)

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
