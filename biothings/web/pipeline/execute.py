"""
    Elasticsearch Query Execution
"""
import asyncio

from biothings.web.handlers.exceptions import BadRequest, EndRequest
from elasticsearch import (ConnectionError, ConnectionTimeout, NotFoundError,
                           RequestError, TransportError)
from tornado.web import HTTPError


class ESQueryBackend(object):
    '''
    Execute an Elasticsearch query
    '''

    def __init__(self, web_settings):

        self.client = web_settings.connections.async_client

        # es indices
        self.indices = web_settings.ES_INDICES
        self.default_index = web_settings.ES_INDEX
        self.default_type = web_settings.ES_DOC_TYPE

        # for scroll queries
        self.scroll_time = web_settings.ES_SCROLL_TIME
        self.scroll_size = web_settings.ES_SCROLL_SIZE

    async def execute(self, query, options):
        '''
        Execute the corresponding query. Must return an awaitable.
        May override to add more. Handle uncaught exceptions.

        Options:
            Required: either an es-dsl query object or scroll_id
            Optional:
                fetch_all: also return a scroll_id for this query (default: false)
                biothing_type: which type's corresponding indices to query (default in config.py)
        '''
        if options.scroll_id:
            try:
                res = await self.client.scroll(
                    scroll_id=options.scroll_id,
                    scroll=self.scroll_time)
            except ConnectionError:
                raise HTTPError(503)
            except (NotFoundError, RequestError, TransportError):
                raise BadRequest(reason="Invalid or stale scroll_id.")
            else:
                if not res['hits']['hits']:
                    raise EndRequest(reason="No more results to return.")
                return res

        if query:
            biothing_type = options.get('biothing_type', None) or self.default_type
            query = query.index(self.indices.get(biothing_type, self.default_index))

            if options.get('fetch_all', False):
                query = query.params(scroll=self.scroll_time)
                query = query.extra(size=self.scroll_size)
            try:
                res = await query.using(self.client).execute()
            except (ConnectionError, ConnectionTimeout):
                raise HTTPError(503)
            except RequestError as exc:
                raise BadRequest(_es_error=exc)
            except TransportError as exc:
                if exc.error == 'search_phase_execution_exception':
                    raise EndRequest(500, _es_error=exc, **exc.info)
                elif exc.error == 'index_not_found_exception':
                    raise HTTPError(500, reason=exc.error)
                elif exc.status_code == 'N/A':
                    raise HTTPError(503)
                else:  # unexpected
                    raise
            else:  # format to {} or [{}...]
                if isinstance(res, list):
                    return [res_.to_dict() for res_ in res]
                return res.to_dict()

        return asyncio.sleep(0, {})
