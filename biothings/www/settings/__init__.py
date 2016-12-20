# -*- coding: utf-8 -*-
import os
from importlib import import_module

# Error class
class BiothingConfigError(Exception):
    pass

# Import the module
try:
    config_module = os.environ['BIOTHING_CONFIG']
except:
    raise BiothingConfigError("Make sure BIOTHING_CONFIG environment variable is set with config module.")

config = import_module(config_module)
default = import_module('biothings.settings.default')

class BiothingSettings(object):
    config_vars = vars(config)
    default_vars = vars(default)

    def _return_var(self, key):
        # return variable named key
        try:
            return self.config_vars[key]
        except KeyError:
            return self.default_vars[key]

    @property
    def static_path(self):
        return self._return_var('STATIC_PATH')

    @property
    def _annotation_endpoint(self):
        return self._return_var('ANNOTATION_ENDPOINT')

    @property
    def _query_endpoint(self):
        return self._return_var('QUERY_ENDPOINT')

    @property
    def _graph_query_endpoint(self):
        return self._return_var('GRAPH_QUERY_ENDPOINT')

    @property
    def _api_version(self):
        if self._return_var('API_VERSION'):
            return self._return_var('API_VERSION')
        else:
            return ''

    @property
    def status_check_id(self):
        return self._return_var('STATUS_CHECK_ID')

    @property
    def field_notes_path(self):
        return self._return_var('FIELD_NOTES_PATH')

    @property
    def jsonld_context_path(self):
        return self._return_var('JSONLD_CONTEXT_PATH')

    @property
    def nosetest_settings(self):
        return self._return_var('NOSETEST_SETTINGS')

    # *************************************************************************
    # * Elasticsearch functions and properties
    # *************************************************************************

    @property
    def es_query_module(self):
        return self._return_var('ES_QUERY_MODULE')


    @property
    def es_host(self):
        return self._return_var('ES_HOST')

    @property
    def es_index(self):
        return self._return_var('ES_INDEX_NAME')

    @property
    def es_doc_type(self):
        return self._return_var('ES_DOC_TYPE')

    @property
    def allowed_options(self):
        return self._return_var('ALLOWED_OPTIONS')

    @property
    def scroll_time(self):
        return self._return_var('ES_SCROLL_TIME')

    @property
    def scroll_size(self):
        return self._return_var('ES_SCROLL_SIZE')

    @property
    def size_cap(self):
        return self._return_var('ES_SIZE_CAP')

    @property
    def userquery_dir(self):
        return self._return_var('USERQUERY_DIR')
    
    # *************************************************************************
    # * neo4j settings wrappers
    # *************************************************************************
    
    @property
    def neo4j_query_module(self):
        return self._return_var('NEO4J_QUERY_MODULE')

    @property
    def is_neo4j_app(self):
        iga = self._return_var('NEO4J_APP')
        if (isinstance(iga, int) and iga == 1) or (isinstance(iga, str) and iga.lower() == 'true') or (isinstance(iga, bool) and iga):
            return True
        return False

    @property
    def neo4j_host(self):
        return self._return_var('NEO4J_CYPHER_ENDPOINT')

    @property
    def neo4j_username(self):
        return self._return_var('NEO4J_USERNAME')

    @property
    def neo4j_password(self):
        return self._return_var('NEO4J_PASSWORD')

    # *************************************************************************
    # * Google Analytics API tracking object functions
    # *************************************************************************

    @property
    def private_module(self):
        return self._return_var('PRIVATE_MODULE')

    @property
    def ga_event_for_get_action(self):
        return self._return_var('GA_EVENT_GET_ACTION')

    @property
    def ga_event_for_post_action(self):
        return self._return_var('GA_EVENT_POST_ACTION')

    @property
    def ga_event_category(self):
        return self._return_var('GA_EVENT_CATEGORY')

    @property
    def ga_is_prod(self):
        return self._return_var('GA_RUN_IN_PROD')

    @property
    def ga_account(self):
        # Get from config file first
        try:
            return self.config_vars['GA_ACCOUNT']
        except KeyError:
            pass
        
        # Try to get from a private submodule on the server
        try:
            return vars(import_module(self.private_module))['ANALYTICS'][self.es_doc_type]
        except (ImportError, KeyError):
            pass
        
        # Fallback to default
        return self.default_vars['GA_ACCOUNT']

    @property
    def ga_tracker_url(self):
        return self._return_var('GA_TRACKER_URL')

    # This function returns the object that is sent to google analytics for an API call
    def ga_event_object(self, endpoint, action, data):
        ret = {}
        ret['category'] = self.ga_event_category
        if action == 'GET':
            ret['action'] = '_'.join([endpoint, self.ga_event_for_get_action])
        elif action == 'POST':
            ret['action'] = '_'.join([endpoint, self.ga_event_for_post_action])
        else:
            ret['action'] = action
        if data:
            ret['label'] = list(data.keys()).pop() 
            ret['value'] = list(data.values()).pop()
        return ret
