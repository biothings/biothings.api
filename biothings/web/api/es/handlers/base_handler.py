import logging

from tornado.web import Finish

from biothings.web.api.handler import BaseAPIHandler


class BaseESRequestHandler(BaseAPIHandler):
    '''
    Parent class of all Elasticsearch-based Request handlers, subclass of `BaseHandler`_.
    Contains handling for Elasticsearch-specific query params (like ``fields``, ``size``, etc)
    '''
    name = 'api'
    out_format = 'json'
    kwarg_types = ('control', )
    kwarg_methods = ('get', 'post')

    def initialize(self, biothing_type=None):
        '''
        Request Level initialization.
        '''
        super(BaseESRequestHandler, self).initialize()
        self.biothing_type = biothing_type or self.web_settings.ES_DOC_TYPE
        self.query_builder = self.web_settings.query_builder
        self.query_backend = self.web_settings.query_backend
        self.query_transform = self.web_settings.query_transform

        # Configure Google Analytics

        action_key = f'GA_ACTION_{self.name.upper()}_{self.request.method}'
        if hasattr(self.web_settings, action_key):
            self.ga_event_object_ret['action'] = getattr(self.web_settings, action_key)
        else:
            self.ga_event_object_ret['action'] = self.request.method

        logging.debug("Google Analytics Base object: %s", self.ga_event_object_ret)

    def parse_exception(self, exception):

        message = super().parse_exception(exception)

        if '_es_error' in message:
            _es_error = message.pop('_es_error')
            message['error'] = _es_error.error
            try:
                root_cause = _es_error.info['error']['root_cause'][0]['reason']
                root_cause = root_cause.replace('\"', '\'').split('\n')
                for index, cause in enumerate(root_cause):
                    message['root_cuase_line_'+f'{index:02}'] = cause
            except Exception:
                pass

        return message


class ESRequestHandler(BaseESRequestHandler):
    '''
    Default Implementation of ES Query Pipelines
    '''
    kwarg_types = ('control', 'esqb', 'es', 'transform')
    kwarg_methods = ('get', 'post')

    async def get(self, *args, **kwargs):
        return await self.execute_pipeline(*args, **kwargs)

    async def post(self, *args, **kwargs):
        return await self.execute_pipeline(*args, **kwargs)

    async def execute_pipeline(self, *args, **kwargs):

        options = self.pre_query_builder_hook(self.kwargs)

        ###################################################
        #                   Build query
        ###################################################

        self._query = self.query_builder.build(options.esqb.q, options.esqb)
        self._query = self.pre_query_hook(options, self._query)

        ###################################################
        #                   Execute query
        ###################################################

        self._res = await self.query_backend.execute(self._query, options.es)
        self._res = self.pre_transform_hook(options, self._res)

        ###################################################
        #                 Transform result
        ###################################################

        res = self.query_transform.transform(self._res, options.transform)
        res = self.pre_finish_hook(options, res)

        self.finish(res)

    def pre_query_builder_hook(self, options):
        '''
        Override this in subclasses.
        At this stage, we have the cleaned user input available.
        Might be a good place to implement input based tracking.
        '''
        self.out_format = self.kwargs.control.out_format or 'json'
        return options

    def pre_query_hook(self, options, query):
        '''
        Override this in subclasses.
        By default, return raw query, if requested.
        Might want to persist this behavior by calling super().
        '''
        if options.control.rawquery:
            raise Finish(query.to_dict())

        options.es.biothing_type = self.biothing_type

        # by default scroll ignores sort and size
        if options.es.fetch_all:
            options.esqb.pop('sort', None)
            options.esqb.pop('size', None)

        return query

    def pre_transform_hook(self, options, res):
        '''
        Override this in subclasses.
        By default, return query response, if requested.
        Might want to persist this behavior by calling super().

        POST /v3/gene
        {
            "ids": ["0"]
        }
        >>> [
                {
                    "query": "0",
                    "notfound": true
                }
            ]

        '''
        if options.control.raw:
            raise Finish(res)

        options.transform.biothing_type = self.biothing_type

        if self.request.method == 'POST':
            queries = options.esqb.q
            options.transform.templates = (dict(query=q) for q in queries)
            options.transform.template_miss = dict(notfound=True)
            options.transform.template_hit = dict()

        return res

    def pre_finish_hook(self, options, res):
        '''
        Override this in subclasses.
        Could implement additional high-level result translation.
        '''
        return res
