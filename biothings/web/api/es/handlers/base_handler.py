import json
import logging
import re
from collections import defaultdict
from itertools import chain, product
from pprint import pformat

from tornado.web import Finish, HTTPError

from biothings.utils.common import dotdict, is_str
from biothings.web.api.es.query import BiothingSearchError
from biothings.web.api.helper import BaseHandler, BiothingsQueryParamInterpreter


class BaseESRequestHandler(BaseHandler):
    '''
    Parent class of all Elasticsearch-based Request handlers, subclass of `BaseHandler`_.
    Contains handling for Elasticsearch-specific query params (like ``fields``, ``size``, etc)
    '''
    name = ''
    kwarg_types = ('control', 'esqb', 'es', 'transform')
    kwarg_methods = ('get', 'post')
    options = dotdict()

    @classmethod
    def setup(cls, web_settings):
        '''
        Class level setup. Called in generate API.
        Populate relevent kwarg settings in _kwarg_settings.
        Access with attribute kwarg_settings.
        '''
        super().setup(web_settings)

        if not cls.name:
            return

        cls._kwarg_settings = defaultdict(dict)
        for method, kwarg_type in product(cls.kwarg_methods, cls.kwarg_types):
            key = '_'.join((cls.name, method, kwarg_type, 'kwargs')).upper()
            if hasattr(web_settings, key):
                setting = cls._kwarg_settings[method.upper()]
                setting[kwarg_type] = getattr(web_settings, key)

    def initialize(self, biothing_type=None):
        '''
        Request Level initialization.
        '''
        super(BaseESRequestHandler, self).initialize()
        self.biothing_type = biothing_type or self.web_settings.ES_DOC_TYPE

        # Google Analytics

        action_key = f'GA_ACTION_{self.name.upper()}_{self.request.method}'
        if hasattr(self.web_settings, action_key):
            self.ga_event_object_ret['action'] = getattr(self.web_settings, action_key)
        else:
            self.ga_event_object_ret['action'] = self.request.method

        logging.debug("Google Analytics Base object: {}".format(self.ga_event_object_ret))

    def prepare(self):  # TODO query param should have dominance
        '''
        Extract, typify, and sanitize query arguments from URL and request body.

        * Inputs are combined and then separated into functional catagories.
        * Duplicated query or body arguments will overwrite the previous value.
        * JSON body input will not overwrite query arguments in URL. # TODO
        * Path arguments can overwirte all other existing values.
        '''
        super().prepare()
        args = dict(self.json_arguments)
        args.update({key: self.get_argument(key) for key in self.request.arguments})

        for catagory, settings in self.kwarg_settings.items():
            self.options[catagory] = options = {}
            for keyword, setting in settings.items():

                # retrieve from url and body arguments
                value = args.get(keyword)
                if 'alias' in setting and not value:
                    if not isinstance(setting['alias'], list):
                        setting['alias'] = [setting['alias']]
                    for _alias in setting['alias']:
                        if _alias in args:
                            value = args[_alias]
                            break

                # overwrite with path args (regex captures)
                if 'path' in setting:
                    if isinstance(setting['path'], int):
                        if len(self.path_args) <= setting['path']:
                            self.send_error(400, missing=keyword)
                            raise Finish()
                        value = self.path_args[setting['path']]
                    elif isinstance(setting['path'], str):
                        if setting['path'] not in self.path_kwargs:
                            self.send_error(400, missing=keyword)
                            raise Finish()
                        value = self.path_kwargs[setting['path']]

                # fallback to default values or raise error
                if value is None:
                    if setting.get('required'):
                        self.send_error(400, missing=keyword, received=args)
                        raise Finish()
                    value = setting.get('default')
                    if value is not None:
                        options[keyword] = value
                    continue

                # convert to the desired value type and format
                if not isinstance(value, setting['type']):  # TODO type should exist , use double dispatch
                    param = BiothingsQueryParamInterpreter(args.get('jsoninput', ''))  # TODO undocumented
                    value = param.convert(value, setting['type'])
                if 'translations' in setting and isinstance(value, str):
                    for (regex, translation) in setting['translations']:
                        value = re.sub(regex, translation, value)

                # list size and int value validation
                global_max = getattr(self.web_settings, 'LIST_SIZE_CAP', 1000)
                if isinstance(value, list):
                    max_allowed = setting.get('max', global_max)
                    if len(value) > max_allowed:
                        self.send_error(400, keyword=keyword, lst=value, max=max_allowed)
                        raise Finish()
                elif isinstance(value, (int, float, complex)) and not isinstance(value, bool):
                    if 'max' in setting:
                        if value > setting['max']:
                            self.send_error(400, keyword=keyword, num=value, max=setting['max'])
                            raise Finish()

                # ignroe None value
                if value is not None:
                    options[keyword] = value

        logging.debug("Kwarg settings:\n{}".format(pformat(self.kwarg_settings, width=150)))
        logging.debug("Kwarg received:\n{}".format(pformat(args, width=150)))
        logging.debug("Processed options:\n{}".format(pformat(self.options, width=150)))

    @property
    def kwarg_settings(self):
        '''
        Return the appropriate kwarg settings basing on the request method.
        '''
        if hasattr(self, '_kwarg_settings'):
            if self.request.method in self._kwarg_settings:
                return self._kwarg_settings[self.request.method]
        return {}

    def get_cleaned_options(self, options):  # TODO change docstring: additional cleanings besides common things you can write to config
        """
        Clean up inherent logic between keyword arguments.
        For example, enforce mutual exclusion relationships.
        """

        ### ESQB Stage ###

        # facet_size only relevent for aggs
        if not options.esqb.aggs:
            options.esqb.pop('facet_size', None)

        ### ES Backend Stage ###

        # no sorting when scrolling
        if options.es.fetch_all:
            options.es.pop('sort', None)
            options.es.pop('size', None)

        # fields=all should return all fields
        if options.es._source is not None:
            if not options.es._source:
                options.es.pop('_source')
            elif 'all' in options.es._source:
                options.es._source = True

        ### Transform Stage ###

        options.transform.biothing_type = self.biothing_type

        # inject original query terms
        if self.request.method == 'POST':
            queries = options.esqb.ids or options.esqb.q
            options.transform.templates = (dict(query=q) for q in queries)
            options.transform.template_miss = dict(notfound=True)
            options.transform.template_hit = dict()

        logging.debug("Cleaned options:\n{}".format(pformat(options, width=150)))
        return options

    def write_error(self, status_code, **kwargs):

        reason = kwargs.pop('reason', self._reason)
        assert '\n' not in reason

        message = {
            "code": status_code,
            "success": False,
            "error": reason}
        message.update(kwargs)

        # TODO track

        self.finish(message)  # TODO return different formats basing on control keywords

class ESRequestHandler(BaseESRequestHandler):
    '''
    Default Implementation of ES Query Pipelines
    '''

    async def get(self, *args, **kwargs):
        return await self.execute_pipeline(*args, **kwargs)

    async def post(self, *args, **kwargs):
        return await self.execute_pipeline(*args, **kwargs)

    async def execute_pipeline(self, *args, **kwargs):

        options = self.get_cleaned_options(self.options)
        options = self.pre_query_builder_hook(options)

        ###################################################
        #                   Build query
        ###################################################

        _query = self.web_settings.query_builder.build(options.esqb)
        _query = self.pre_query_hook(options, _query)

        ###################################################
        #                   Execute query
        ###################################################

        res = await self.web_settings.query_backend.execute(
            _query, options.es, self.biothing_type, self.send_error)
        res = self.pre_transform_hook(options, res)

        ###################################################
        #                 Transform result
        ###################################################

        res = self.web_settings.query_transform.transform(
            res, options.transform)
        res = self.pre_finish_hook(options, res)

        self.return_object(res, _format=options.control.out_format)

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
            self.return_object(query.to_dict())
            raise Finish()
        return query

    def pre_transform_hook(self, options, res):
        '''
        Override this in subclasses.
        By default, return query response, if requested.
        Might want to persist this behavior by calling super().
        '''
        if options.control.raw:
            self.return_object(res, _format=options.control.out_format)
            raise Finish()
        return res

    def pre_finish_hook(self, options, res):
        '''
        Override this in subclasses.
        Could implement additional high-level result translation.
        '''
        return res
