"""
    Biothings Web Settings
"""
import asyncio
import importlib.util
import json
import logging
import os
import re
import socket
import types
from collections import defaultdict
from copy import deepcopy
from functools import partial
from importlib import import_module
from pprint import pformat

import elasticsearch
from elasticsearch import ConnectionSelector
from elasticsearch_async.transport import AsyncTransport
from elasticsearch_dsl import A, MultiSearch, Q, Search
from elasticsearch_dsl.connections import Connections

import biothings.web.settings.default
from biothings.web.api.handler import BaseAPIHandler

from .userquery import ESUserQuery

try:
    from raven.contrib.tornado import AsyncSentryClient
except ImportError:
    __SENTRY_INSTALLED__ = False
else:
    __SENTRY_INSTALLED__ = True

class BiothingConfigError(Exception):
    pass

class BiothingWebSettings():
    '''
    A container for the settings that configure the web API.

    * Environment variables can override settings of the same names.
    * Default values are defined in biothings.web.settings.default.

    '''

    def __init__(self, config=None, **kwargs):
        '''
        :param config: a module that configures this biothing
            or its fully qualified name,
            or its module file path.
        '''
        self._default = biothings.web.settings.default
        self._user = self.load_module(config)
        logging.info("Loaded: %s", self._user)

        # process keyword setting override
        for key, value in kwargs.items():
            setattr(self._user, key, value)

        # process environment variable override of named settings
        for name in os.environ:
            if hasattr(self, name) and isinstance(getattr(self, name), str):
                logging.info("Env %s = %s", name, os.environ[name])
                setattr(self._user, name, os.environ[name])

        # for metadata dev details
        if os.path.isdir(os.path.join(self.APP_GIT_REPOSITORY, '.git')):
            self._git_repo_path = self.APP_GIT_REPOSITORY
        else:
            self._git_repo_path = None

        self.validate()

    @staticmethod
    def load_module(config):
        """
        Ensure config is a module.
        """
        if isinstance(config, types.ModuleType):
            return config
        elif isinstance(config, str) and config.endswith('.py'):
            spec = importlib.util.spec_from_file_location("config", config)
            config = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config)
            return config
        elif isinstance(config, str) and config:
            return import_module(config)
        elif not config:
            return biothings.web.settings.default
        else:
            raise BiothingConfigError()

    def __getattr__(self, name):
        if hasattr(self._user, name):
            return getattr(self._user, name)
        elif hasattr(self._default, name):
            return getattr(self._default, name)
        else:  # not provided and no default
            raise AttributeError()

    def generate_app_settings(self):
        """
        Generates settings for tornado.web.Application. This result and the
        method below can define a tornado application to start a web server.
        """
        supported_keywords = (
            'default_handler_class', 'default_handler_args',
            'log_function', 'compress_response', 'cookie_secret',
            'login_url', 'static_path', 'static_url_prefix')

        settings = {}
        for setting in supported_keywords:
            if hasattr(self, setting.upper()):
                settings[setting] = getattr(self, setting.upper())

        if __SENTRY_INSTALLED__ and self.SENTRY_CLIENT_KEY:
            # Setup error monitoring with Sentry. More on:
            # https://docs.sentry.io/clients/python/integrations/#tornado
            settings['sentry_client'] = AsyncSentryClient(self.SENTRY_CLIENT_KEY)

        return settings

    def generate_app_handlers(self):
        '''
        Generates the tornado.web.Application `(regex, handler_class, options) tuples
        <http://www.tornadoweb.org/en/stable/web.html#application-configuration>`_.
        '''
        handlers = []
        for rule in self.APP_LIST:
            if issubclass(rule[1], BaseAPIHandler):
                pattern = rule[0]
                handler = rule[1]
                setting = rule[2] if len(rule) == 3 else {}
                handler.setup(self)
                if '{typ}' in pattern:
                    for biothing_type in self.BIOTHING_TYPES:
                        pattern = pattern.format(
                            pre=self.API_PREFIX,
                            ver=self.API_VERSION,
                            typ=biothing_type).replace('//', '/')
                        setting['biothing_type'] = biothing_type
                    handlers.append((pattern, handler, setting))
                else:
                    pattern = pattern.format(
                        pre=self.API_PREFIX,
                        ver=self.API_VERSION).replace('//', '/')
                    handlers.append((pattern, handler, setting))
            else:
                handlers.append(rule)

        logging.debug('API Handlers:\n%s', pformat(handlers, width=200))
        return handlers

    def get_git_repo_path(self):
        '''
        Return the path of the codebase if the specified folder in settings exists or `None`.
        '''
        return self._git_repo_path

    def validate(self):
        '''
        Validate the settings defined for this web server.
        '''
        assert self.API_VERSION or self.API_PREFIX
        assert isinstance(self.LIST_SIZE_CAP, int)
        assert isinstance(self.ACCESS_CONTROL_ALLOW_METHODS, str)
        assert isinstance(self.ACCESS_CONTROL_ALLOW_HEADERS, str)

    #### COMPATIBILITY METHODS ####

    def set_debug_level(self, debug=False):
        pass

    @property
    def git_repo_path(self):
        return self._git_repo_path

    @property
    def _app_git_repo(self):
        return self._git_repo_path

class KnownLiveSelecter(ConnectionSelector):
    """
    Select the first connection all the time
    """

    def select(self, connections):
        return connections[0]

class BiothingESWebSettings(BiothingWebSettings):
    '''
    `BiothingWebSettings`_ subclass with functions specific to an elasticsearch backend.

    * Use the known live ES connection if more than one is specified.
    * Cache source metadata stored under the _meta field in es indices.

    '''

    ES_VERSION = elasticsearch.__version__[0]

    def __init__(self, config=None, **kwargs):
        '''
        The ``config`` init parameter specifies a module that configures
        this biothing.  For more information see `config module`_ documentation.
        '''
        super(BiothingESWebSettings, self).__init__(config, **kwargs)

        # elasticsearch connections
        self._connections = Connections()

        connection_settings = {
            "hosts": self.ES_HOST,
            "timeout": self.ES_CLIENT_TIMEOUT,
            "max_retries": 1,  # maximum number of retries before an exception is propagated
            "timeout_cutoff": 1,  # timeout freezes after this number of consecutive failures
            "selector_class": KnownLiveSelecter}
        self._connections.create_connection(alias='sync', **connection_settings)
        connection_settings.update(transport_class=AsyncTransport)
        self._connections.create_connection(alias='async', **connection_settings)

        # cached index mappings # TODO
        self.source_metadata = defaultdict(dict)
        self.source_properties = defaultdict(dict)

        # populate field notes if exist
        try:
            inf = open(self.AVAILABLE_FIELDS_NOTES_PATH, 'r')
            self._fields_notes = json.load(inf)
            inf.close()
        except Exception:
            self._fields_notes = {}

        # user query data
        self.userquery = ESUserQuery(self.USERQUERY_DIR)

        # query pipelines
        self.query_builder = self.ES_QUERY_BUILDER(self)
        self.query_backend = partial(self.ES_QUERY, self)
        self.query_transform = self.ES_RESULT_TRANSFORMER(self)

        # initialize payload for standalone tracking batch
        self.tracking_payload = []

        self.ES_INDICES[self.ES_DOC_TYPE] = self.ES_INDEX
        self.BIOTHING_TYPES = list(self.ES_INDICES.keys())

        # populate source mappings
        for biothing_type in self.ES_INDICES:
            self.read_index_mappings(biothing_type)

    def validate(self):
        '''
        Additional ES settings to validate.
        '''
        super().validate()

        assert isinstance(self.ES_INDEX, str)
        assert isinstance(self.ES_DOC_TYPE, str)
        assert isinstance(self.ES_INDICES, dict)
        assert '*' not in self.ES_DOC_TYPE

    def get_es_client(self):
        '''
        Return the default blocking elasticsearch client.
        The connection is created upon first call.
        '''
        return self._connections.get_connection('sync')

    def get_async_es_client(self):
        '''
        Return the async elasitcsearch client.
        The connection is created upon first call.
        API calls return awaitable objects.
        '''
        return self._connections.get_connection('async')

    def read_index_mappings(self, biothing_type=None):
        """
        Read ES index mappings for the corresponding biothing_type,
        Populate datasource info and field properties from mappings.
        Return ES raw response. This implementation combines indices.

        The ES response would look like: (for es7+)
        {
            'index_1': {
                'properties': { ... },  <----- Extract source_properties
                '_meta': {
                    "src" : { ... }     <----- Extract source_licenses
                    ...
                },              <----- Extract source_metadata
                ...
            },
            'index_2': {
                ...     <----- Combine with results above
            }
        }
        """

        biothing_type = biothing_type or self.ES_DOC_TYPE
        try:
            mappings = self.get_es_client().indices.get_mapping(
                index=self.ES_INDICES[biothing_type],
                allow_no_indices=True,
                ignore_unavailable=True,
                local=False)
        except Exception:
            return None

        metadata = self.source_metadata[biothing_type]
        properties = self.source_properties[biothing_type]
        licenses = self.query_transform.source_licenses[biothing_type]

        metadata.clear()
        properties.clear()
        licenses.clear()

        for index in mappings:

            if self.ES_VERSION < 7:
                mapping = mappings[index]['mappings'][biothing_type]
            else:
                mapping = mappings[index]['mappings']

            if '_meta' in mapping:
                for key, val in mapping['_meta'].items():
                    if key in metadata and isinstance(val, dict) \
                            and isinstance(metadata[key], dict):
                        metadata[key].update(val)
                    else:
                        metadata[key] = val
                metadata.update(mapping['_meta'])

                if 'src' in mapping['_meta']:
                    for src, info in mapping['_meta']['src'].items():
                        if 'license_url_short' in info:
                            licenses[src] = info['license_url_short']
                        elif 'license_url' in info:
                            licenses[src] = info['license_url']

            if 'properties' in mapping:
                properties.update(mapping['properties'])

        return mappings

    def get_field_notes(self):
        '''
        Return the cached field notes associated with this instance.
        '''
        return self._fields_notes

    ##### COMPATIBILITY METHODS #####

    @property
    def es_client(self):
        return self.get_es_client()

    @property
    def async_es_client(self):
        return self.get_async_es_client()

    def doc_url(self, bid):
        return os.path.join(self.URL_BASE, self.API_VERSION, self.ES_DOC_TYPE, bid)

    def available_fields_notes(self):
        return self._fields_notes
