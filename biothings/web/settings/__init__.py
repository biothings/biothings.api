# -*- coding: utf-8 -*-
'''Settings objects used to configure the web API
These settings get passed into the handler.initialize() function,
of each request, and configure the web API endpoint.  They are mostly
a container for the `Config module`_, and any other settings that
are the same across all handler types, e.g. the Elasticsearch client.'''

import asyncio
import json
import logging
import os
import socket
import types
from importlib import import_module

import elasticsearch
from elasticsearch import ConnectionSelector
from elasticsearch_async import AsyncElasticsearch
from elasticsearch_async.transport import AsyncTransport

from elasticsearch_dsl.connections import Connections


# Error class
class BiothingConfigError(Exception):
    pass

class BiothingWebSettings(object):
    '''
    A container for the settings that configure the web API.
    '''

    def __init__(self, config='biothings.web.settings.default'):
        '''
        The ``config`` init parameter specifies a module that configures
        this biothing.  For more information see `config module`_ documentation. '''

        self._user = config if isinstance(config, types.ModuleType) else import_module(config)
        self._default = import_module('biothings.web.settings.default')

        # for metadata dev details
        if os.path.isdir(os.path.join(self.APP_GIT_REPOSITORY, '.git')):
            self._git_repo_path = self.APP_GIT_REPOSITORY
        else:
            self._git_repo_path = None

        self.validate()

    def __getattr__(self, name):

        if hasattr(self._user, name) or hasattr(self._default, name):

            # environment variables can override named settings
            if name in os.environ:
                return os.environ[name]

            return getattr(self._user, name, getattr(self._default))

        raise AttributeError("No setting named '{}' in configuration file.".format(name))

    def generate_app_list(self):
        '''
        Generates the tornado.web.Application `(regex, handler_class, options) tuples
        <http://www.tornadoweb.org/en/stable/web.html#application-configuration>`_ for this project.
        '''
        handlers = []

        for rule in self.APP_LIST:
            settings = {"web_settings": self}
            if len(rule) == 3:
                settings.update(rule[-1])
            handlers.append((rule[0], rule[1], settings))

        return handlers

    def get_git_repo_path(self):

        return self._git_repo_path

    def validate(self):

        for rule in self.APP_LIST:
            if len(rule) == 2:
                pass
            elif len(rule) == 3:
                assert isinstance(rule[-1], dict)
            else:
                raise BiothingConfigError()

    #### COMPATIBILITY METHODS ####

    def set_debug_level(self, debug=False):
        pass

    @property
    def git_repo_path(self):
        return self._git_repo_path

class KnownLiveSelecter(ConnectionSelector):
    """
    Select the first connection all the time
    """

    def select(self, connections):
        return connections[0]

class BiothingESWebSettings(BiothingWebSettings):
    ''' `BiothingWebSettings`_ subclass with functions specific to an elasticsearch backend '''

    ES_VERSION = elasticsearch.__version__[0]

    def __init__(self, config='biothings.web.settings.default'):
        '''
        The ``config`` init parameter specifies a module that configures
        this biothing.  For more information see `config module`_ documentation.
        '''
        super(BiothingESWebSettings, self).__init__(config)

        # elasticsearch connections
        self._connections = Connections()

        connection_settings = {
            "hosts": self.ES_HOST,
            "timeout": self.ES_CLIENT_TIMEOUT,
            "max_retries": 1,  # maximum number of retries before an exception is propagated
            "timeout_cutoff": 1,  # number of consecutive failures after which the timeout doesn’t increase
            "selector_class": KnownLiveSelecter}
        self._connections.create_connection(alias='sync', **connection_settings)

        connection_settings.update(transport_class=AsyncTransport)
        self._connections.create_connection(alias='async', **connection_settings)

        # project metadata under index mappings
        self._source_metadata = {}

        # populate field notes if exist
        try:
            inf = open(self.AVAILABLE_FIELDS_NOTES_PATH, 'r')
            self._fields_notes = json.load(inf)
            inf.close()
        except Exception:
            self._fields_notes = {}

        # initialize payload for standalone tracking batch
        self.tracking_payload = []

    def validate(self):

        super().validate()

        assert isinstance(self.ES_INDEX, str)
        assert isinstance(self.ES_DOC_TYPE, str)
        assert isinstance(self.ES_INDICES, dict)
        assert '*' not in self.ES_DOC_TYPE

        self.ES_INDICES[self.ES_DOC_TYPE] = self.ES_INDEX

    def get_es_client(self):
        return self._connections.get_connection('sync')

    def get_async_es_client(self):
        return self._connections.get_connection('async')

    def get_source_metadata(self, biothing_type='doc', latest=True):

        cached = biothing_type in self._source_metadata

        if latest or not cached:
            kwargs = {
                'index': self.ES_INDICES[biothing_type],
                'allow_no_indices': True,
                'ignore_unavailable': True,
                'local': not latest
            }
            if self.ES_VERSION < 7:
                kwargs['doc_type'] = biothing_type

            mappings = self.get_es_client().get_mapping(**kwargs)
            metadata = {}

            for index in mappings:

                if self.ES_VERSION < 7:
                    _meta = mappings[index]['mappings'][biothing_type].get('_meta', {})
                else:
                    _meta = mappings[index]['mappings'].get('_meta', {})

                metadata.update(_meta)

            self._source_metadata[biothing_type] = metadata

        return self._source_metadata[biothing_type]

    def get_field_notes(self):
        return self._fields_notes

    ##### COMPATIBILITY METHODS #####

    @property
    def es_client(self):
        return self.get_es_client()

    @property
    def async_es_client(self):
        return self.get_async_es_client()

    def source_metadata(self):
        pass

    def doc_url(self, bid):
        ''' Function to return a url on this biothing API to the biothing object specified by bid.'''
        return os.path.join(self.URL_BASE, self.API_VERSION, self.ES_DOC_TYPE, bid)

    def available_fields_notes(self):
        ''' Caches the available fields notes for this biothing '''
        return self._fields_notes
