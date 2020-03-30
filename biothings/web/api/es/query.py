"""
    Elasticsearch Query Execution
"""
import asyncio

import elasticsearch
from elasticsearch import NotFoundError, RequestError, TransportError
from elasticsearch_dsl import A, MultiSearch, Q, Search
from elasticsearch_dsl.connections import get_connection
from elasticsearch_dsl.response import Response
from tornado.web import Finish

from biothings.utils.common import dotdict


class BiothingScrollError(Exception):
    ''' Error thrown when an ES scroll process errs '''
    pass

class BiothingSearchError(Exception):
    ''' Error thrown when given query errs (either from ES ``search_phase_exception``, or other errors). '''
    pass

class AsyncMultiSearch(MultiSearch):  # TODO maybe these two belongs to query class

    async def execute(self, ignore_cache=False, raise_on_error=True):
        """
        Execute the multi search request and return a list of search results.
        """
        if ignore_cache or not hasattr(self, '_response'):
            es = get_connection(self._using)

            responses = await es.msearch(
                index=self._index,
                body=self.to_dict(),
                **self._params
            )

            out = []
            for s, r in zip(self._searches, responses['responses']):
                if r.get('error', False):
                    if raise_on_error:
                        raise TransportError('N/A', r['error']['type'], r['error'])
                    r = None
                else:
                    r = Response(s, r)
                out.append(r)

            self._response = out

        return self._response

class AsyncSearch(Search):

    async def execute(self, ignore_cache=False):
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.

        :arg ignore_cache: if set to ``True``, consecutive calls will hit
            ES, while cached result will be ignored. Defaults to `False`
        """
        if ignore_cache or not hasattr(self, '_response'):
            es = get_connection(self._using)

            self._response = self._response_class(
                self,
                await es.search(
                    index=self._index,
                    body=self.to_dict(),
                    **self._params
                )
            )
        return self._response

class ESQuery(object):
    '''
    Execute an Elasticsearch query
    '''
    ES_VERSION = elasticsearch.__version__[0]

    def __init__(self, web_settings):

        self.client = web_settings.get_async_es_client()

        # es indices
        self.indices = web_settings.ES_INDICES
        self.default_index = web_settings.ES_INDEX
        self.default_type = web_settings.ES_DOC_TYPE

        # for scroll queries
        self.scroll_time = web_settings.ES_SCROLL_TIME
        self.scroll_size = web_settings.ES_SCROLL_SIZE

    async def execute(self, query, options, biothing_type, callback):
        '''
        Execute the corresponding query.
        May override to add more. Handle uncaught exceptions.
        Must return an awaitable.
        '''
        if options.scroll_id:
            try:
                res = await self.client.scroll(
                    scroll_id=options.scroll_id,
                    scroll=self.scroll_time)
            except (NotFoundError, RequestError, TransportError):
                callback(400, reason="Invalid or stale scroll_id.")
                raise Finish()
            else:
                if not res['hits']['hits']:
                    callback(200, reason="No more results to return.")
                    raise Finish()
                return res

        if query:
            biothing_type = biothing_type or self.default_type
            query = query.index(self.indices.get(biothing_type, self.default_index))

            if options.pop('fetch_all', False):
                query = query.params(scroll=self.scroll_time)
                query = query.extra(size=self.scroll_size)

            if options.sort:  # accept '-' prefixed field names
                query = query.sort(*options.pop('sort'))

            try:
                return await self.dsl_query(query, **options)
            except elasticsearch.RequestError as err:
                callback(400, root_cause=err.info['error']['root_cause'][0]['reason'])
                raise Finish()
            except Exception:  # TODO
                callback(400, reason='request error')
                raise Finish()

        return asyncio.sleep(0, {})

    async def dsl_query(self, query, **options):

        query = query.extra(**options)
        res = await query.using(self.client).execute()
        if isinstance(res, list):
            return [res_.to_dict() for res_ in res]
        return res.to_dict()
