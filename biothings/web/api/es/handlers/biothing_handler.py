
from tornado.web import Finish

from biothings.web.api.es.handlers.base_handler import ESRequestHandler


class BiothingHandler(ESRequestHandler):
    '''
    Handle requests to the annotation lookup endpoint.
    '''
    name = 'annotation'

    def pre_query_builder_hook(self, options):
        '''
        Inject bid(s) in esqb_kwargs.
        Set GA tracking object.
        '''
        if self.request.method == 'GET':
            options.esqb['bid'] = self.path_args[0]  # TODO
        elif self.request.method == 'POST':
            self.ga_event_object({'qsize': len(options.esqb.ids)})
        return options

    def pre_finish_hook(self, options, res):
        '''
        Return single result for GET.
        Also does not return empty results.
        '''
        if isinstance(res, dict):
            if not res.get('hits'):
                self.send_error(404, reason=self.web_settings.ID_NOT_FOUND_TEMPLATE.format(bid=options.esqb.bid))
                raise Finish()
            res = res['hits'][0]
            res.pop('_score')

        elif isinstance(res, list):
            for hit in res:
                hit.pop('_score', None)

        return res
