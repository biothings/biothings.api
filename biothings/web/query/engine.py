"""
Search Execution Engine

Take the output of the query builder and feed
to the corresponding database engine. This stage
typically resolves the db destination from a
biothing_type and applies presentation and/or
networking parameters.

Example:

>>> from biothings.web.query import ESQueryBackend
>>> from elasticsearch import Elasticsearch
>>> from elasticsearch.dsl import Search

>>> backend = ESQueryBackend(Elasticsearch())
>>> backend.execute(Search().query("match", _id="1017"))

>>> _["hits"]["hits"][0]["_source"].keys()
dict_keys(['taxid', 'symbol', 'name', ... ])

"""

import asyncio
import base64
import hashlib
import json
import logging
from dataclasses import dataclass

from elasticsearch import ApiError, NotFoundError, RequestError
from elasticsearch.dsl import MultiSearch, Search

from biothings.web.query.builder import ESScrollID

logger = logging.getLogger(__name__)


def parse_api_error(exc):
    """
    Extract (error_type, reason, root_cause_types) from an elasticsearch-py
    ApiError's response body, e.g. ("search_phase_execution_exception",
    "all shards failed", {"search_context_missing_exception"}).

    Returns (None, "", set()) if the body doesn't have the expected shape -
    ES's error body is a best-effort diagnostic, not a validated contract,
    so a differently-shaped or absent "error"/"root_cause" must fall back
    cleanly rather than raise out of here.
    """
    try:
        error = exc.info.get("error", {})
        if not isinstance(error, dict):
            return None, "", set()
        root_cause = error.get("root_cause") or []  # covers both absent and explicit null
        root_causes = {cause.get("type") for cause in root_cause if isinstance(cause, dict)}
        return error.get("type"), error.get("reason", ""), root_causes
    except (AttributeError, TypeError):
        return None, "", set()


class ResultInterrupt(Exception):
    def __init__(self, data):
        super().__init__()
        self.data = data


class RawResultInterrupt(ResultInterrupt):
    pass


class EndScrollInterrupt(ResultInterrupt):
    def __init__(self):
        super().__init__({"success": False, "error": "No more results to return."})


# the search_after value that starts a new search
SEARCH_AFTER_START = "*"

# query keys that change what each page returns but not which hits
# match or their order, they may differ between pages of a search
_PAGE_ONLY_KEYS = ("size", "from", "_source", "explain", "version")

_INVALID_SEARCH_AFTER = (
    "Invalid search_after value. Start a new search with search_after=*, "
    "then pass back the _search_after value of each response to get the next page."
)


def search_after_fingerprint(index, query):
    """
    Summarize the index and the query body a search_after cursor belongs to,
    so a cursor sent with a different query is rejected instead of returning
    pages of the wrong hits.
    """
    query = {key: value for key, value in query.items() if key not in _PAGE_ONLY_KEYS}
    serialized = json.dumps([index, query], sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class SearchAfterCursor:
    """
    The "_search_after" value returned with a page of hits, passed
    back as the search_after parameter to get the next page.
    """

    pit: str  # id of the point in time the pages are read from
    after: list  # sort values of the last hit returned
    fingerprint: str  # see search_after_fingerprint
    total: int  # number of hits, counted on the first page
    returned: int  # number of hits returned so far

    def encode(self):
        data = {"pit": self.pit, "after": self.after, "fp": self.fingerprint, "total": self.total, "n": self.returned}
        return base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode()).decode().rstrip("=")

    @classmethod
    def decode(cls, value):
        try:
            data = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
            cursor = cls(data["pit"], data["after"], data["fp"], data["total"], data["n"])
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError(_INVALID_SEARCH_AFTER) from exc

        def is_count(number):
            return isinstance(number, int) and not isinstance(number, bool) and number >= 0

        if not (
            isinstance(cursor.pit, str)
            and cursor.pit
            and isinstance(cursor.after, list)
            and cursor.after
            and isinstance(cursor.fingerprint, str)
            and is_count(cursor.total)
            and is_count(cursor.returned)
        ):
            raise ValueError(_INVALID_SEARCH_AFTER)
        return cursor


class ESQueryBackend:
    def __init__(self, client, indices=None):
        self.client = client
        self.indices = indices or {None: "_all"}

        # a list of biothing_type -> index pattern mapping
        # ---------------------------------------------------
        # {
        #   None: "hg19_current",
        #   "hg19": "hg19_current",
        #   "hg38": "hg38_index1,hg38_index2",
        #   "_internal": "hg*_current"
        # }

        if None not in self.indices:  # set default index pattern
            self.indices[None] = next(iter(self.indices.values()))

    def execute(self, query, **options):
        assert isinstance(query, Search)
        index = self.indices[options.get("biothing_type")]
        # index can be further adjusted (e.g. based on options) if necessary
        index = self.adjust_index(index, query, **options)
        return self.client.search(query.to_dict(), index)

    def adjust_index(self, original_index, query, **options):
        """
        Override to get specific ES index.
        """
        return original_index


class AsyncESQueryBackend(ESQueryBackend):
    """
    Execute an Elasticsearch query
    """

    def __init__(
        self,
        client,
        indices=None,
        scroll_time="1m",
        scroll_size=1000,
        multisearch_concurrency=5,
        total_hits_as_int=True,
    ):
        super().__init__(client, indices)

        # for scroll queries
        self.scroll_time = scroll_time  # scroll context expiration timeout
        self.scroll_size = scroll_size  # result window size override value

        # concurrency control
        self.semaphore = asyncio.Semaphore(multisearch_concurrency)

        # additional params
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes-7.0.html
        # #hits-total-now-object-search-response
        self.total_hits_as_int = total_hits_as_int

    async def execute(self, query, **options):
        """
        Execute the corresponding query. Must return an awaitable.
        May override to add more. Handle uncaught exceptions.

        Options:
            fetch_all: also return a scroll_id for this query (default: false)
            search_after: page through all hits in a point in time, "*" for the first page,
                then the "_search_after" value returned with the previous page
            biothing_type: which type's corresponding indices to query (default in config.py)
        """
        assert isinstance(
            query,
            (
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html
                Search,
                MultiSearch,
                ESScrollID,
            ),
        )

        if isinstance(query, ESScrollID):
            try:
                res = await self.client.scroll(
                    scroll_id=query.data, scroll=self.scroll_time, rest_total_hits_as_int=self.total_hits_as_int
                )
            except (
                RequestError,  # the id is not in the correct format of a context id
                NotFoundError,  # the id does not correspond to any search context
            ):
                raise ValueError("Invalid or stale scroll_id.")
            else:
                if options.get("raw"):
                    raise RawResultInterrupt(res)

                if not res["hits"]["hits"]:
                    scroll_id=query.data
                    try:
                        await self.client.clear_scroll(scroll_id=scroll_id)
                        logger.info("Scroll context cleared: %s", scroll_id)
                    except NotFoundError as e:
                        logger.warning("Scroll context not found (ID: %s): %s", scroll_id, str(e))
                    # Always raise this exception regardless of whether clear_scroll succeeds
                    raise EndScrollInterrupt()

                return res

        # everything below require us to know which indices to query
        index = self.indices[options.get("biothing_type")]
        # index can be further adjusted (e.g. based on options) if necessary
        index = self.adjust_index(index, query, **options)

        if isinstance(query, Search) and options.get("search_after"):
            res = await self._search_after(query, index, options["search_after"])

        elif isinstance(query, Search):
            if options.get("fetch_all"):
                query = query.extra(size=self.scroll_size)
                query = query.params(scroll=self.scroll_time)
            if self.total_hits_as_int:
                query = query.params(rest_total_hits_as_int=True)
            query_kwargs = query.to_dict()
            query_kwargs.update(query._params)
            if "from" in query_kwargs:
                query_kwargs["from_"] = query_kwargs.pop("from")
            res = await self.client.search(index=index, **query_kwargs)

        elif isinstance(query, MultiSearch):
            await self.semaphore.acquire()
            try:
                res = await self.client.msearch(body=query.to_dict(), index=index)
            finally:
                self.semaphore.release()
            res = res["responses"]

        if options.get("raw"):
            raise RawResultInterrupt(res)

        return res

    async def _search_after(self, query, index, search_after):
        """
        Search a page of hits in a point in time (PIT), the deep pagination
        elasticsearch recommends over scroll. search_after is "*" to open a
        point in time for the first page, or the cursor returned with the
        previous page to continue after its last hit. Hits follow the query's
        sort, which elasticsearch ends with a _shard_doc tiebreaker, or come
        in index order (_shard_doc) when the query has no sort.
        https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results
        """
        body = query.to_dict()
        fingerprint = search_after_fingerprint(index, body)
        first_page = search_after == SEARCH_AFTER_START

        if first_page:
            pit = await self.client.open_point_in_time(index=index, keep_alive=self.scroll_time)
            cursor = SearchAfterCursor(pit["id"], [], fingerprint, 0, 0)
        else:
            cursor = SearchAfterCursor.decode(search_after)
            if cursor.fingerprint != fingerprint:
                raise ValueError(
                    "This search_after value belongs to a different query. Send the same q, sort "
                    "and filters with every page, or start a new search with search_after=*."
                )
            body["search_after"] = cursor.after

        if not body.get("sort"):
            body["sort"] = ["_shard_doc"]
        if body.get("size") is None:
            body["size"] = self.scroll_size
        # the index is part of the point in time
        body["pit"] = {"id": cursor.pit, "keep_alive": self.scroll_time}
        # the hits are counted once, on the first page, the point in time
        # does not change and later pages are faster without counting
        body["track_total_hits"] = first_page
        # fail rather than silently skip the hits of a failed shard
        body["allow_partial_search_results"] = False
        if self.total_hits_as_int:
            body["rest_total_hits_as_int"] = True

        try:
            res = await self.client.search(**body)
        except Exception as exc:
            if first_page:
                # the cursor to continue in this point in time is never returned
                await self._close_point_in_time(cursor.pit)
            elif isinstance(exc, NotFoundError) or (
                # how a multi node cluster can report it
                isinstance(exc, ApiError)
                and "search_context_missing_exception" in parse_api_error(exc)[2]
            ):
                raise ValueError(
                    f"Invalid or expired search_after value, a value expires {self.scroll_time} "
                    "after the page it came with. Start a new search with search_after=*."
                ) from exc
            raise

        page = getattr(res, "body", res)  # the dict of an ObjectApiResponse
        hits = page["hits"]["hits"]
        if first_page:
            total = page["hits"]["total"]
            total = total["value"] if isinstance(total, dict) else total
        else:
            total = cursor.total
            page["hits"]["total"] = total if self.total_hits_as_int else {"value": total, "relation": "eq"}
        returned = cursor.returned + len(hits)
        pit_id = page.get("pit_id", cursor.pit)  # the id can change between pages

        if hits and len(hits) >= body["size"] and returned < total:
            page["_search_after"] = SearchAfterCursor(pit_id, hits[-1]["sort"], fingerprint, total, returned).encode()
        else:  # the last page
            await self._close_point_in_time(pit_id)

        return res

    async def _close_point_in_time(self, pit_id):
        # best effort, a point in time left open expires after its keep_alive
        try:
            await self.client.close_point_in_time(id=pit_id)
        except Exception as exc:
            logger.warning("Point in time not closed: %s", exc)


class MongoQueryBackend:
    def __init__(self, client, collections):
        self.client = client
        self.collections = collections

        if None not in self.collections:  # set default collection pattern
            self.collections[None] = next(iter(self.collections.values()))

    def execute(self, query, **options):
        client = self.client[self.collections[options.get("biothing_type")]]
        return list(client.find(*query).skip(options.get("from", 0)).limit(options.get("size", 10)))


class SQLQueryBackend:
    def __init__(self, client):
        self.client = client

    def execute(self, query, **options):
        result = self.client.execute(query)
        return result.keys(), result.all()
