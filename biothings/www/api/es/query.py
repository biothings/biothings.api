from elasticsearch import NotFoundError, RequestError, TransportError
from biothings.utils.common import dotdict
import logging

class BiothingScrollError(Exception):
    pass

class BiothingSearchError(Exception):
    pass

class ESQuery(object):
    def __init__(self, client, options=dotdict()):
        self.client = client
        self.options = options
        
    def _scroll(self, query_kwargs):
        ''' Returns the next scroll batch for the given scroll id '''
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
        try:
            return self.client.get(**query_kwargs)
        except NotFoundError:
            return {}

    def annotation_GET_query(self, query_kwargs):
        return self._annotation_GET_query(query_kwargs)

    def annotation_POST_query(self, query_kwargs):
        return self._annotation_POST_query(query_kwargs)

    def query_GET_query(self, query_kwargs):
        return self._query_GET_query(query_kwargs)

    def query_POST_query(self, query_kwargs):
        return self._query_POST_query(query_kwargs)

    def metadata_query(self, query_kwargs):
        return self._metadata_query(query_kwargs)

    def scroll(self, query_kwargs):
        return self._scroll(query_kwargs)
