# -*- coding: utf-8 -*-
import os
from biothings.settings import default
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
    def _annotation_endpoint(self):
        return self._return_var('ANNOTATION_ENDPOINT')

    @property
    def _query_endpoint(self):
        return self._return_var('QUERY_ENDPOINT')

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

    # *************************************************************************
    # * Elasticsearch functions and properties
    # *************************************************************************

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

    # *************************************************************************
    # * Google Analytics API tracking object functions
    # *************************************************************************

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
        return self._return_var('GA_ACCOUNT')

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
        for (k,v) in data.items():
            ret['label'] = k
            ret['value'] = v
        return ret