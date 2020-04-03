"""
    Override of elasticsearch-dsl modules to support async operations.
"""

from elasticsearch import NotFoundError, RequestError, TransportError
from elasticsearch_dsl import A, MultiSearch, Q, Search
from elasticsearch_dsl.connections import get_connection
from elasticsearch_dsl.response import Response


class AsyncMultiSearch(MultiSearch):

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
