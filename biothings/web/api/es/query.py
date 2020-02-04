from biothings.utils.common import dotdict
import logging

class BiothingScrollError(Exception):
    ''' Error thrown when an ES scroll process errs '''
    pass

class BiothingSearchError(Exception):
    ''' Error thrown when given query errs (either from ES ``search_phase_exception``, or other errors). '''
    pass

class ESQuery(object):
    ''' This class contains functions to execute the *query* section of all handler pipelines.
    The inputs to it are an Elasticsearch client (from `BiothingESWebSettings`_), and any options
    from the URL string.  Each handler calls a different query function, though they all do essentially
    the same thing: get the query generated in the ESQueryBuilder stage of the pipeline (``query_kwargs``), and run it
    using the supplied Elasticsearch client.'''

    def __init__(self, client, options=dotdict()):
        self.client = client
        self.options = options

    async def _common_POST_query(self, query_kwargs):
        from elasticsearch import RequestError
        try:
            res = await self.client.msearch(**query_kwargs)
        except RequestError as e:
            if e.args[1] == 'search_phase_execution_exception' and "error" in e.args[2] and "root_cause" in e.args[2]["error"]:
                _root_causes = [
                    '{} {}'.format(c['type'],
                                   c['reason']) for c in e.args[2]['error']['root_cause']
                    if 'reason' in c and 'type' in c]
                raise BiothingSearchError(
                    'Could not execute query due to the following exception(s): {}'.format(_root_causes))
            else:
                raise Exception('{0}'.format(e))

        _root_causes = []
        for i in res['responses']:
            if 'error' in i:
                _root_causes.extend(['{} {}'.format(c['type'], c['reason'])
                                     for c in i['error']['root_cause']])

        if _root_causes:
            raise BiothingSearchError(
                'Could not execute query due to the following exception(s): {}'.format(_root_causes))

        return res

    async def annotation_GET_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of annotation lookup GET query on ES client.'''
        if query_kwargs.get('id', None):
            # these query kwargs should be to an es.get
            from elasticsearch import NotFoundError
            try:
                return await self.client.get(**query_kwargs)
            except NotFoundError:
                return {}
        else:
            return await self.client.search(**query_kwargs)

    async def annotation_POST_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of annotation lookup POST query on ES client.'''
        return await self._common_POST_query(query_kwargs)

    async def query_GET_query(self, query_kwargs, *args, **kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of query GET on ES client.'''
        from elasticsearch import RequestError
        try:
            return await self.client.search(**query_kwargs)
        except RequestError as e:
            if e.args[1] == 'search_phase_execution_exception' and "error" in e.args[2] and "root_cause" in e.args[2]["error"]:
                _root_causes = [
                    '{} {}'.format(c['type'],
                                   c['reason']) for c in e.args[2]['error']['root_cause']
                    if 'reason' in c and 'type' in c]
                raise BiothingSearchError(
                    'Could not execute query due to the following exception(s): {}'.format(_root_causes))
            else:
                raise Exception('{0}'.format(e))

    async def query_POST_query(self, query_kwargs, *args, **kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of query POST on ES client.'''
        return await self._common_POST_query(query_kwargs)

    async def _metadata_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of metadata query on ES client.'''
        return await self.client.indices.get_mapping(**query_kwargs)

    async def scroll(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of a scroll on ES client - returns next batch of results. '''
        ''' Returns the next scroll batch for the given scroll id '''
        from elasticsearch import NotFoundError, RequestError, TransportError
        try:
            return await self.client.scroll(**query_kwargs)
        except (NotFoundError, RequestError, TransportError):
            raise BiothingScrollError("Invalid or stale scroll_id")
