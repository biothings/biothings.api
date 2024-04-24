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
        post_filter: str, when provided, the search hits are filtered after the aggregations are calculated.
        facet_size: int, maximum number of agg results.

"""

from collections import UserString, namedtuple
from copy import deepcopy
from random import randrange
import logging
import os
import re
from typing import Iterable, List, Tuple, Union

from elasticsearch_dsl import MultiSearch, Q, Search
from elasticsearch_dsl.exceptions import IllegalOperation
import orjson

from biothings.utils.common import dotdict
from biothings.web.services.metadata import BiothingsMetadata
from biothings.web.settings.default import ANNOTATION_DEFAULT_REGEX_PATTERN


logger = logging.getLogger(__name__)


class RawQueryInterrupt(Exception):
    def __init__(self, data):
        super().__init__()
        self.data = data


Query = namedtuple("Query", ("term", "scopes"))
Group = namedtuple("Group", ("term", "scopes"))


class QStringParser:
    def __init__(
        self,
        default_scopes: Tuple[str] = None,
        patterns: Iterable[Tuple[Union[str, re.Pattern], Union[str, Iterable]]] = None,
        default_pattern: Tuple[Union[str, re.Pattern], Union[str, Iterable]] = ANNOTATION_DEFAULT_REGEX_PATTERN,
        gpnames: Tuple[str] = None,
        metadata: BiothingsMetadata = None,
    ):
        if default_scopes is None:
            default_scopes = ("_id",)

        if gpnames is None:
            gpnames = ("term", "scope")
        self.gpname = Group(*gpnames)  # symbolic group name for term substitution

        assert isinstance(default_scopes, (tuple, list))
        assert all(isinstance(field, str) for field in default_scopes)
        self.default_scopes = default_scopes

        self.metadata = metadata
        self.default_pattern = self._verify_default_regex_pattern(default_pattern=default_pattern)
        self.patterns = self._build_regex_pattern_collection(patterns=patterns)

    def _build_endpoint_metadata_fields(self, metadata: BiothingsMetadata) -> dict:
        """
        Extracts the field mappings stored in our "metadata" instance

        BiothingsESMetadata is constructed in
        biothings.web.services.namespace._configure_elasticsearch

        We want to access the mappings stored in elasticsearch provided via
        the biothing_mappings class property

        -----------------------------------------------------------------
            def get_mappings(self, biothing_type):
                return self.biothing_mappings[biothing_type]
        -----------------------------------------------------------------

        We need to extract the biothing_type from the metadata in order to the
        access this metadata
        """
        metadata_fields = {}
        if metadata is not None:
            general_metadata = metadata.biothing_metadata[None]
            metadata_fields = metadata.get_mappings(general_metadata["biothing_type"])
        return metadata_fields

    def _verify_default_regex_pattern(
        self, default_pattern: Tuple[Union[str, re.Pattern], Union[str, Iterable]]
    ) -> Tuple[re.Pattern, Iterable]:
        """
        Take the default pattern and ensure that if the user does intend to override
        the default value provided by ANNOTATION_DEFAULT_REGEX_PATTERN that it still matches
        the overall structure we expect

        Also provides a warning if the user does change the value in case that provides
        unwanted behavior

        We do allow for setting the regex pattern to None in case the instance does want to
        eliminate regex pattern matching in the query building
        """
        if default_pattern != ANNOTATION_DEFAULT_REGEX_PATTERN:
            logger.warning(
                (
                    "Default regex pattern changed to [%s]."
                    "Set by <ANNOTATION_DEFAULT_REGEX_PATTERN> in the configuration",
                    ANNOTATION_DEFAULT_REGEX_PATTERN,
                )
            )

        # Initialize to the default pattern and then reset it as well if any exceptions occur
        # while loading the overrided pattern
        default_regex_pattern = ANNOTATION_DEFAULT_REGEX_PATTERN[0]
        default_regex_fields = ANNOTATION_DEFAULT_REGEX_PATTERN[1]

        if default_pattern is not None:
            try:
                default_regex_pattern = re.compile(default_pattern[0])
                default_regex_fields = [str(field_entry) for field_entry in default_pattern[1]]
            except Exception as gen_exc:
                logger.exception(gen_exc)
                logger.error(
                    (
                        "Invalid new regex pattern [%s]. Resetting to the default pattern [%s]",
                        default_pattern,
                        ANNOTATION_DEFAULT_REGEX_PATTERN,
                    )
                )
                default_regex_pattern = ANNOTATION_DEFAULT_REGEX_PATTERN[0]
                default_regex_fields = ANNOTATION_DEFAULT_REGEX_PATTERN[1]
            finally:
                default_pattern = (default_regex_pattern, default_regex_fields)
        return default_pattern

    def _build_regex_pattern_collection(
        self,
        patterns: Iterable[Tuple[Union[str, re.Pattern], Union[str, Iterable]]],
    ) -> List[Tuple[re.Pattern, List[str]]]:
        """
        Builds the regex pattern list based off the provided patterns. With the
        ANNOTATION_ID_REGEX_LIST configuration parameter, the user can provide
        regex patterns matching the following structure:
        (Union[str, re.Pattern], Union[str, Iterable])

        We also load the default annotation regex pattern from the settings and ensure it's
        applied as the very last pattern in a the potential list of regex patterns provided
        by the instance configuration. We don't want to publically expose the default regex
        pattern in the configuration as accidently modifying that could lead to unexpected /
        unwanted behavior. Therefore we add it at runtime if it isn't discovered

        Flow:
        1) Branch on if a regex pattern list was provided. If none provided then set to the default
        and return
        2) If an iterable of regex patterns is provided then we force the structure into what we
        expect: List[re.Pattern, Iterable]
        3) We then iterate over the structure looking for the default regex pattern.
            - If we find the default regex pattern match, we ignore updating our list
            - If we don't find the default regex pattern match, we update our list with the pattern
        4) At the end we add the default regex pattern because we've exhausted our search of the
        current pattern list and trimmed any instances we found. This should ensure we've set the
        default as the last instance in the regex pattern list

        """
        if self.default_pattern:
            default_regex_pattern = self.default_pattern[0]
        else:
            default_regex_pattern = None

        structured_patterns = []
        if isinstance(patterns, Iterable):
            for regex_pattern, regex_fields in patterns:
                regex_pattern = re.compile(regex_pattern)
                if isinstance(regex_fields, str):
                    regex_fields = [regex_fields]

                # Check if the pattern matchs the default
                # If it does match, we ignore adding it until outside the loop
                # If it doesn't match we add it in the next instruction
                if (
                    default_regex_pattern
                    and regex_pattern.pattern == default_regex_pattern.pattern
                    and len(regex_fields) == 0
                ):
                    continue
                structured_patterns.append((regex_pattern, regex_fields))

        if self.default_pattern:
            structured_patterns.append(self.default_pattern)
        return structured_patterns

    def parse(self, query: str, metadata: BiothingsMetadata):
        """
        Parsing method for the QStringParser object

        Inputs
        query: string query to search the elasticsearch instance

        It greedily searchs the supplied regex patterns supplied
        via <self.patterns>  to the first match in the list
        """
        logger.debug("Attempting to parse query string %s", query)
        query_metadata = self._build_endpoint_metadata_fields(metadata)

        fallback_scope_fields = self.default_scopes
        query_object = Query(query, fallback_scope_fields)

        for regex, pattern_fields in self.patterns:
            match = re.fullmatch(regex, query)
            if match:
                logger.debug("Discovered regex-query match: regex [%s] | match [%s]", regex, match)

                named_groups = match.groupdict()
                match_term = named_groups.get(self.gpname.term, None)
                matched_fields = named_groups.get(self.gpname.scopes, None)

                term_query = match_term or query
                scope_fields = matched_fields or pattern_fields or fallback_scope_fields

                if not isinstance(scope_fields, (list, tuple)):
                    scope_fields = [scope_fields]
                query_object = Query(term_query, scope_fields)
                print(locals())
                break

        logger.info("Generated query object: [%s]", query_object)
        return query_object


class ESScrollID(UserString):
    def __init__(self, seq: object):
        super().__init__(seq)
        # scroll id cannot be empty
        assert self.data


class ESUserQuery:
    def __init__(self, path):
        self._queries = {}
        self._filters = {}
        try:
            for dirpath, dirnames, filenames in os.walk(path):
                if dirnames:
                    self.logger.info("User query folders: %s.", dirnames)
                    continue
                for filename in filenames:
                    with open(os.path.join(dirpath, filename)) as text_file:
                        if "query" in filename:
                            ## alternative implementation  # noqa: E266
                            # self._queries[os.path.basename(dirpath)] = text_file.read()
                            ##
                            self._queries[os.path.basename(dirpath)] = orjson.loads(text_file.read())
                        elif "filter" in filename:
                            self._filters[os.path.basename(dirpath)] = orjson.loads(text_file.read())
        except Exception:
            self.logger.exception("Error loading user queries.")

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

        ## alternative implementation  # noqa: E266
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


class ESQueryBuilder:
    """
    Build an Elasticsearch query with elasticsearch-dsl.

                ES Query Builder Architecture
    -------------------------------------------------------
                            build
                    (support multisearch)
    --------------------------↓↓↓--------------------------
                           _build_one
     (dispatch basing on scopes, then apply_extras(..))
    ------------↓↓↓------------------------↓↓↓-------------
       _build_string_query    |  _build_match_query
     (__all__, userquery,..)  | (compound match query)
    ------------↓↓↓------------------------↓↓↓-------------
       default_string_query   |   default_match_query
     (map to ES query string) | (map to ES match query)
    -------------------------------------------------------
    """

    # Different from other query pipelines, elasticsearch
    # supports querystring query, which means we can directly
    # dispatch queries without fields to querystring query,
    # and those with fields specified to typical match queries.

    def __init__(
        self,
        user_query: Union[str, ESUserQuery] = None,  # like a prepared statement in SQL
        scopes_regexs: Iterable[Tuple[Union[str, re.Pattern], Union[str, Iterable]]] = None,
        scopes_default: Tuple[str] = ("_id",),  # fallback used when scope inference fails
        pattern_default: Tuple[Union[str, re.Pattern], Union[str, Iterable]] = ANNOTATION_DEFAULT_REGEX_PATTERN,
        allow_random_query: bool = True,  # used for data exploration, can be expensive
        allow_nested_query: bool = False,  # nested aggregation can be expensive
        metadata: BiothingsMetadata = None,  # access to data like total number of documents
    ):
        # all settings below affect only query string queries
        if user_query is None:
            user_query = ESUserQuery("userquery")
        self.user_query = user_query

        self.allow_random_query = allow_random_query
        self.allow_nested_query = allow_nested_query  # for aggregations

        # currently metadata is only used for __any__ query
        self.metadata = metadata

        self.parser = QStringParser(
            default_scopes=scopes_default,
            patterns=scopes_regexs,
            default_pattern=pattern_default,
            gpnames=("term", "scope"),
            metadata=self.metadata,
        )

    def build(self, q=None, **options):
        """
        Build a query according to q and options.
        This is the public method called by API handlers.

        Regarding scopes:
            scopes: [str] nonempty, match query.
            scopes: NoneType, or [], no scope, so query string query.

        Additionally support these options:
            explain: include es scoring information
            userquery: customized function to interpret q

        * additional keywords are passed through as es keywords
            for example: 'explain', 'version' ...

        * multi-search is supported when q is a list. all queries
            are built individually and then sent in one request.

        """
        options = dotdict(options)

        if options.scroll_id:
            # bypass all query building stages
            return ESScrollID(options.scroll_id)

        if options.fetch_all:
            # clean up conflicting parameters
            options.pop("sort", None)
            options.pop("size", None)

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

        except IllegalOperation as illegal_operation_error:
            logger.exception(illegal_operation_error)
            raise ValueError from illegal_operation_error

        if options.get("rawquery"):
            raise RawQueryInterrupt(search.to_dict())

        return search

    def _build_one(self, q, options):
        # a single query, possibly included in a multi-search
        # later but it itself is a single query unit.

        if options.scopes:
            search = self._build_match_query(q, options.scopes, options)
        elif not isinstance(q, (list, tuple)) and options.autoscope:
            q, scopes = self.parser.parse(str(q), self.metadata)
            search = self._build_match_query(q, scopes, options)
        else:  # no scope provided and cannot derive from q
            search = self._build_string_query(q, options)

        # pass through es query options. (from, size ...)
        search = self.apply_extras(search, options)
        return search

    def _build_string_query(self, q, options):
        """q + options -> query object

        options:
            userquery
        """
        search = Search()
        userquery = options.userquery or ""

        if q == "":  # same empty q behavior as that of ES.
            search = search.query("match_none")

        elif q == "__all__" or q is None:
            search = search.query()
            if options.aggs and not options.size:
                options.size = 0

        elif q == "__any__":
            if self.allow_random_query:
                search = search.query("function_score", random_score={})
            else:  # pseudo random by overriding 'from' value
                search = search.query()
                try:  # limit 'from' parameter to a valid result window
                    metadata = self.metadata.biothings_metadata[options.biothing_type]
                    total = metadata["stats"]["total"]
                    fmax = total - options.get("size", 0)
                    from_ = randrange(fmax if fmax < 10000 else 10000)
                    options["from"] = from_ if from_ >= 0 else 0
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
        """q + scopes + options -> query object

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
        return Search().query("query_string", query=q, default_operator="AND", lenient=True)

    def default_match_query(self, q, scopes, options):
        """
        Override this to customize default match query.
        By default it implements a multi_match query.
        """
        assert isinstance(q, (str, int, float, bool))
        assert isinstance(scopes, (list, tuple, str)) and scopes
        _params = dict(query=q, fields=scopes, operator="AND", lenient=True)
        if options.analyzer:
            _params["analyzer"] = options.analyzer
        return Search().query("multi_match", **_params)

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
                if self.allow_nested_query and "(" in term and term.endswith(")"):
                    _term, term = term[:-1].split("(", 1)
                else:
                    _term, term = term, ""
                bucket = bucket.bucket(_term, "terms", field=_term, size=facet_size)

        # add es params
        if isinstance(options.sort, list):
            # accept '-' prefixed field names
            search = search.sort(*options.sort)
        if isinstance(options._source, list):
            if "all" not in options._source:
                fields_with_minus = [field.lstrip("-") for field in options._source if field.startswith("-")]
                fields_without_minus = [field for field in options._source if not field.startswith("-")]
                search = search.source(includes=fields_without_minus, excludes=fields_with_minus)
        for key in ("from", "size", "explain", "version"):
            if key in options:
                search = search.extra(**{key: options[key]})

        # the valid values for from and size depend on the
        # index.max_result_window elasticsearch setting.

        # more about this constraint on:
        # https://www.elastic.co/guide/en/elasticsearch/
        # reference/current/index-modules.html

        # Feature: filter
        # apply extra filter (as query_string query) to filter results
        # Ref: https://www.elastic.co/guide/en/elasticsearch/reference/8.10/query-dsl-bool-query.html
        if options.filter:
            search = search.filter("query_string", query=options.filter)

        # Feature: post_filter
        # -- implementation using query string matching
        # Ref: https://www.elastic.co/guide/en/elasticsearch/reference/8.10/filter-search-results.html#post-filter
        if options.post_filter:
            search = search.post_filter("query_string", query=options["post_filter"])

        return search


class MongoQueryBuilder:
    def __init__(self, default_scopes=("_id",)):
        self.parser = QStringParser(default_scopes)

    def build(self, q, **options):
        fields = options.get("scopes", ())
        if not fields and q:
            q, fields = self.parser.parse(q)

        assert isinstance(fields, (list, tuple))
        assert q is None and not fields or q and isinstance(q, str)
        assert all((isinstance(field, str) for field in fields))

        filter_ = {field: 1 for field in options.get("_source", ())} or None  # project fields to return

        query = {"$or": [{field: q} for field in fields]} if fields else {}

        if options.get("rawquery"):
            raise RawQueryInterrupt((query, filter_))

        return (query, filter_)


class SQLQueryBuilder:
    # PROOF OF CONCEPT
    # INPUT NOT SANITIZED
    # INTERNAL USE ONLY

    def __init__(self, tables, default_scopes=("id",), default_limit=10):
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
            "SELECT {}".format(", ".join(options.get("_source", ())) or "*"),
            "FROM {}".format(self.tables[options.get("biothing_type")]),
        ]

        scopes = options.get("scopes")
        if not scopes:
            q, scopes = self.parser.parse(q)

        if scopes and q:
            assert isinstance(q, str)
            selections = ['{} = "{}"'.format(field, q) for field in scopes]
            statements.append("WHERE")
            statements.append(" OR ".join(selections))

        # limit result window
        statements.append("LIMIT {}".format(options.get("size", self.default_limit)))

        if "from_" in options:
            statements.append("OFFSET {}".format(options["from_"]))

        if options.get("rawquery"):
            raise RawQueryInterrupt(statements)

        return " ".join(statements)
