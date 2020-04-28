

from biothings.web.api.es.handlers import ESRequestHandler
from biothings.web.api.handler import BadRequest


class QueryHandler(ESRequestHandler):
    '''
    Request handlers for requests to the query endpoint
    '''
    name = 'query'

    def prepare(self):
        """
        Perform a quick check to ensure required arguments are present.
        """
        if self.request.method == 'GET':
            args = self.request.arguments
            if 'q' not in args and 'scroll_id' not in args:
                raise BadRequest(
                    missing={'or': ['q', 'scroll_id']}
                )
        super().prepare()

    def pre_query_builder_hook(self, options):
        '''
        Set GA tracking object.
        '''
        options = super().pre_query_builder_hook(options)
        if self.request.method == 'GET':
            self.ga_event_object({'total': 0})
        elif self.request.method == 'POST':
            self.ga_event_object({'qsize': len(options.esqb.q)})
        return options

    def pre_finish_hook(self, options, res):
        '''
        TODO
        '''
        if self.request.method == 'GET':
            if options.esqb.fetch_all:
                self.ga_event_object_ret['action'] = 'fetch_all'
                self.ga_event_object({'total': res.get('total', 0)})  # TODO DSL rewrite

        return res
