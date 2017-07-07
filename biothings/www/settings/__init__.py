# -*- coding: utf-8 -*-
'''Settings objects used to configure the www API
These settings get passed into the handler.initialize() function,
of each request, and configure the web API endpoint.  They are mostly
a container for the `Config module`_, and any other settings that
are the same across all handler types, e.g. the Elasticsearch client.'''

import logging
import os
import socket
from importlib import import_module
from biothings.utils.www.log import get_hipchat_logger
import json

# Error class
class BiothingConfigError(Exception):
    pass

class BiothingWebSettings(object):
    ''' A container for the settings that configure the web API '''

    def __init__(self, config='biothings.www.settings.default'):
        ''' The ``config`` init parameter specifies a module that configures 
        this biothing.  For more information see `config module`_ documentation.''' 
        self.config_mod = import_module(config)
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
        
        # for logging exceptions to hipchat
        if self.HIPCHAT_ROOM and self.HIPCHAT_AUTH_TOKEN:
            try:
                _socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                _socket.connect(self.HIPCHAT_AUTO_FROM_SOCKET_CONNECTION)  # set up socket
                _from = _socket.getsockname()[0] # try to get local ip as the "from" key
            except:
                _from = None
            self._hipchat_logger = get_hipchat_logger(hipchat_room=self.HIPCHAT_ROOM, 
                hipchat_auth_token=self.HIPCHAT_AUTH_TOKEN, hipchat_msg_from=_from, 
                hipchat_log_format=getattr(self, 'HIPCHAT_MESSAGE_FORMAT', None), 
                hipchat_msg_color=self.HIPCHAT_MESSAGE_COLOR)
        else:
            self._hipchat_logger = None

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
        return [(endpoint_regex, handler, {"web_settings": self}) for (endpoint_regex, handler) in self.APP_LIST]

    def validate(self):
        ''' Validate these settings '''
        pass

class BiothingESWebSettings(BiothingWebSettings):
    ''' `BiothingWebSettings`_ subclass with functions specific to an elasticsearch backend '''
    def __init__(self, config='biothings.www.settings.default'):
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
        return {}

    def source_metadata(self):
        ''' Function to cache return of the source metadata stored in _meta of index mappings '''
        if getattr(self, '_source_metadata', False):
            return self._source_metadata

        self._source_metadata = self._source_metadata_object()

        return self._source_metadata

    def get_es_client(self):
        '''Get the `Elasticsearch client <https://elasticsearch-py.readthedocs.io/en/master/>`_
        for this app, only called once on invocation of server. '''
        from elasticsearch import Elasticsearch
        return Elasticsearch(self.ES_HOST, timeout=getattr(self, 'ES_CLIENT_TIMEOUT', 120))
