import json
import logging
import re
from collections import defaultdict
from itertools import chain, product
from pprint import pformat

from tornado.web import Finish, HTTPError

from biothings.utils.common import dotdict
from biothings.web.api.handler import BaseAPIHandler, BadRequest


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
        self.query_backend = self.web_settings.query_backend()
        self.query_transform = self.web_settings.query_transform

        # Configure Google Analytics

        action_key = f'GA_ACTION_{self.name.upper()}_{self.request.method}'
        if hasattr(self.web_settings, action_key):
            self.ga_event_object_ret['action'] = getattr(self.web_settings, action_key)
        else:
            self.ga_event_object_ret['action'] = self.request.method

        logging.debug("Google Analytics Base object: %s", self.ga_event_object_ret)

    def prepare(self):

        super().prepare()
        self.out_format = self.kwargs.control.out_format or 'json'

    def write_error(self, status_code, **kwargs):

        reason = kwargs.pop('reason', self._reason)
        assert '\n' not in reason

        message = {
            "code": status_code,
            "success": False,
            "error": reason
        }
        if 'exc_info' in kwargs:
            exception = kwargs['exc_info'][1]
            if isinstance(exception, BadRequest) and exception.kwargs:
                message.update(exception.kwargs)

        self.finish(message)

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

        _query = self.query_builder.build(options.esqb.q, options.esqb)
        _query = self.pre_query_hook(options, _query)

        ###################################################
        #                   Execute query
        ###################################################

        _res = await self.query_backend.execute(_query, options.es)
        _res = self.pre_transform_hook(options, _res)

        ###################################################
        #                 Transform result
        ###################################################

        res = self.query_transform.transform(_res, options.transform)
        res = self.pre_finish_hook(options, res)

        self.finish(res)

    def pre_query_builder_hook(self, options):
        '''
        Override this in subclasses.
        At this stage, we have the cleaned user input available.
        Might be a good place to implement input based tracking.
        '''
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
            options.es.pop('sort', None)
            options.es.pop('size', None)

        return query

    def pre_transform_hook(self, options, res):
        '''
        Override this in subclasses.
        By default, return query response, if requested.
        Might want to persist this behavior by calling super().
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
