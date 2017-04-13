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
        
    def _scroll(self, query_kwargs):
        ''' Returns the next scroll batch for the given scroll id '''
        from elasticsearch import NotFoundError, RequestError, TransportError
        try:
            return self.client.scroll(**query_kwargs)
        except (NotFoundError, RequestError, TransportError):
            raise BiothingScrollError("Invalid or stale scroll_id")

    def _annotation_GET_query(self, query_kwargs):
        if query_kwargs.get('id', None):
            # these query kwargs should be to an es.get
            return self.get_biothing(query_kwargs)
        else:
            return self.client.search(**query_kwargs)

    def _annotation_POST_query(self, query_kwargs):
        return self.client.msearch(**query_kwargs)
   
    def _query_GET_query(self, query_kwargs):
        try:
            return self.client.search(**query_kwargs)
        except RequestError as e:
            if e.args[1] == 'search_phase_execution_exception' and "error" in e.args[2] and "root_cause" in e.args[2]["error"]:
                _root_causes = ['{} {}'.format(c['type'], c['reason']) for c in e.args[2]['error']['root_cause'] if 'reason' in c and 'type' in c]
                raise BiothingSearchError('Could not execute query due to the following exception(s): {}'.format(_root_causes))
            else:
                raise Exception('{0}'.format(e))
    
    def _query_POST_query(self, query_kwargs):
        try:
            return self.client.msearch(**query_kwargs)
        except RequestError as e:
            if e.args[1] == 'search_phase_execution_exception' and "error" in e.args[2] and "root_cause" in e.args[2]["error"]:
                _root_causes = ['{} {}'.format(c['type'], c['reason']) for c in e.args[2]['error']['root_cause'] if 'reason' in c and 'type' in c]
                raise BiothingSearchError('Could not execute query due to the following exception(s): {}'.format(_root_causes))
            else:
                raise Exception('{0}'.format(e))

    def _metadata_query(self, query_kwargs):
        return self.client.indices.get_mapping(**query_kwargs)

    def get_biothing(self, query_kwargs):
        ''' Return a biothing using the Elasticsearch client.get function '''
        try:
            return self.client.get(**query_kwargs)
        except NotFoundError:
            return {}

    def annotation_GET_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of annotation lookup GET query on ES client.'''
        return self._annotation_GET_query(query_kwargs)

    def annotation_POST_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of annotation lookup POST query on ES client.'''
        return self._annotation_POST_query(query_kwargs)

    def query_GET_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of query GET on ES client.'''
        return self._query_GET_query(query_kwargs)

    def query_POST_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of query POST on ES client.'''
        return self._query_POST_query(query_kwargs)

    def metadata_query(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of metadata query on ES client.'''
        return self._metadata_query(query_kwargs)

    def scroll(self, query_kwargs):
        ''' Given ``query_kwargs`` from ESQueryBuilder, return results of a scroll on ES client - returns next batch of results. '''
        return self._scroll(query_kwargs)
