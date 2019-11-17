# -*- coding: utf-8 -*-
'''Settings objects used to configure the web API
These settings get passed into the handler.initialize() function,
of each request, and configure the web API endpoint.  They are mostly
a container for the `Config module`_, and any other settings that
are the same across all handler types, e.g. the Elasticsearch client.'''

import logging
import os, types
import socket
from importlib import import_module
import json

# Error class
class BiothingConfigError(Exception):
    pass

class BiothingWebSettings(object):
    ''' A container for the settings that configure the web API '''

    def __init__(self, config='biothings.web.settings.default'):
        ''' The ``config`` init parameter specifies a module that configures 
        this biothing.  For more information see `config module`_ documentation.''' 
        self.config_mod = type(config) == types.ModuleType and config or import_module(config)
        try:
            with open(os.path.abspath(self.config_mod.JSONLD_CONTEXT_PATH), 'r') as json_file:
                self._jsonld_context = json.load(json_file)
        except:
            self._jsonld_context = {}

        # for metadata dev
        self._app_git_repo = os.path.abspath(self.APP_GIT_REPOSITORY) if hasattr(self, 'APP_GIT_REPOSITORY') else os.path.abspath('.')
        if not (self._app_git_repo and os.path.exists(self._app_git_repo) and 
            os.path.isdir(self._app_git_repo) and os.path.exists(os.path.join(self._app_git_repo, '.git'))):
            self._app_git_repo = None

        # validate these settings?
        self.validate()
    
    def __getattr__(self, name):
        try:
            return getattr(self.config_mod, name)
        except AttributeError:
            raise AttributeError("No setting named '{}' was found, check configuration module.".format(name))

    def set_debug_level(self, debug=False):
        '''Set if running API in debug mode.
        Should be called before passing ``self`` to handler initialization.'''
        self._DEBUG = debug
        return self
    
    def generate_app_list(self):
        ''' Generates the tornado.web.Application `(regex, handler_class, options) tuples <http://www.tornadoweb.org/en/stable/web.html#application-configuration>`_ for this project.'''
        return self.UNINITIALIZED_APP_LIST + [
            (endpoint_regex, handler, {"web_settings": self})
            for (endpoint_regex, handler) in self.APP_LIST]

    def validate(self):
        ''' Validate these settings '''
        pass

class BiothingESWebSettings(BiothingWebSettings):
    ''' `BiothingWebSettings`_ subclass with functions specific to an elasticsearch backend '''
    def __init__(self, config='biothings.web.settings.default'):
        ''' The ``config`` init parameter specifies a module that configures 
        this biothing.  For more information see `config module`_ documentation.''' 
        super(BiothingESWebSettings, self).__init__(config)

        # get es client for web
        self.es_client = self.get_es_client()

        # populate the metadata for this project
        self.source_metadata()
        
        # initialize payload for standalone tracking batch
        self.tracking_payload = []   

    def doc_url(self, bid):
        ''' Function to return a url on this biothing API to the biothing object specified by bid.'''
        return os.path.join(self.URL_BASE, self.API_VERSION, self.ES_DOC_TYPE, bid)

    def _source_metadata_object(self):
        ''' Override me to return metadata for your project '''
        _meta = {}
        try:
            _m = self.es_client.indices.get_mapping(index=self.ES_INDEX, doc_type=self.ES_DOC_TYPE)
            _meta = _m[list(_m.keys())[0]]['mappings'][self.ES_DOC_TYPE]['_meta']['src']
        except:
            pass
        return _meta

    def source_metadata(self):
        ''' Function to cache return of the source metadata stored in _meta of index mappings '''
        if getattr(self, '_source_metadata', False):
            return self._source_metadata

        self._source_metadata = self._source_metadata_object()

        return self._source_metadata

    def available_fields_notes(self):
        ''' Caches the available fields notes for this biothing '''
        try:
            return self._available_fields_notes
        except:
            pass

        self._available_fields_notes = {}

        if os.path.exists(os.path.abspath(self.AVAILABLE_FIELDS_NOTES_PATH)):
            try:
                with open(os.path.abspath(self.AVAILABLE_FIELDS_NOTES_PATH), 'r') as inf:
                    self._available_fields_notes = json.load(inf)
            except:
                pass

        return self._available_fields_notes

    def get_es_client(self):
        '''Get the `Elasticsearch client <https://elasticsearch-py.readthedocs.io/en/master/>`_
        for this app, only called once on invocation of server. '''
        from elasticsearch import Elasticsearch
        return Elasticsearch(self.ES_HOST, timeout=getattr(self, 'ES_CLIENT_TIMEOUT', 120))
