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
import logging

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

    async def _scroll(self, scroll_id, _retry=True):
        try:
            return await self.client.scroll(
                scroll_id=scroll_id, scroll=self.scroll_time, rest_total_hits_as_int=self.total_hits_as_int
            )
        except (
            RequestError,  # the id is not in the correct format of a context id
            NotFoundError,  # the id does not correspond to any search context
        ):
            raise ValueError("Invalid or stale scroll_id.")
        except ApiError as exc:
            # note: must catch ApiError, not TransportError - elasticsearch-py
            # 8.x/9.x raises ApiError (elastic_transport.ApiError) for a plain
            # 500 like search_phase_execution_exception; TransportError
            # (elastic_transport.TransportError) is a disjoint hierarchy for
            # client/connection-level failures and is never raised here.
            error_type, _reason, root_causes = parse_api_error(exc)

            if error_type != "search_phase_execution_exception" or not root_causes:
                raise  # unrecognized shape, let the caller's generic handler classify it

            if "illegal_state_exception" in root_causes and _retry:
                # a cluster node was briefly unreachable while the scroll context
                # lived partly on it; elasticsearch-py doesn't retry scroll calls
                # on its own, so retry once before giving up
                logger.warning("Scroll hit an unavailable node, retrying once: %s", exc)
                return await self._scroll(scroll_id, _retry=False)

            if "search_context_missing_exception" in root_causes:
                # same outcome as NotFoundError above, just reported by ES as a
                # partial-shard failure instead of a clean 404. checking
                # membership rather than exact equality so this still applies
                # when a context-missing shard is mixed with some other,
                # unrelated shard failure.
                raise ValueError("Invalid or stale scroll_id.")

            raise

    async def execute(self, query, **options):
        """
        Execute the corresponding query. Must return an awaitable.
        May override to add more. Handle uncaught exceptions.

        Options:
            fetch_all: also return a scroll_id for this query (default: false)
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
            res = await self._scroll(query.data)

            if options.get("raw"):
                raise RawResultInterrupt(res)

            if not res["hits"]["hits"]:
                scroll_id = query.data
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

        if isinstance(query, Search):
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
