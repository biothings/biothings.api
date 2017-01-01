# -*- coding: utf-8 -*-
import logging
from importlib import import_module
from biothings.utils.es import get_es
import json
from os.path import abspath

# Error class
class BiothingConfigError(Exception):
    pass

class BiothingWebSettings(object):
    def __init__(self, config='biothings.www.settings.default'): 
        self.config_mod = import_module(config)
        try:
            with open(abspath(self.config_mod.JSONLD_CONTEXT_PATH), 'r') as json_file:
                self._jsonld_context = json.load(json_file)
        except:
            self._jsonld_context = {}
        # validate these settings?
        self.validate()
    
    def __getattr__(self, name):
        try:
            return getattr(self.config_mod, name)
        except AttributeError:
            raise AttributeError("No setting named '{}' was found, check configuration module.".format(name))

    def set_debug_level(self, debug=False):
        ''' Are we debugging? '''
        self._DEBUG = debug
        return self
    
    def generate_app_list(self):
        ''' Generates the APP_LIST for tornado for this project, basically just adds the settings 
            to kwargs in every handler's initialization. '''
        return [(endpoint_regex, handler, {"web_settings": self}) for (endpoint_regex, handler) in self.APP_LIST]

    def validate(self):
        ''' validates this object '''
        pass

class BiothingESWebSettings(BiothingWebSettings):
    ''' subclass with functions specific to elasticsearch backend '''
    def __init__(self, config='biothings.www.settings.default'):
        super(BiothingESWebSettings, self).__init__(config)

        # get es client for web
        self.es_client = self.get_es_client()

    def get_es_client(self):
        ''' get the es client for this app '''
        return get_es(self.ES_HOST)
