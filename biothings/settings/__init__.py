# -*- coding: utf-8 -*-
import os
import configparser
import ast

# Allowed options in config.ini/default.ini
REQUIRED_OPTIONS = {
                  'ElasticsearchSettings': ['ES_HOST', 'ES_INDEX_NAME', 'ES_DOC_TYPE', 'ALLOWED_OPTIONS', 'ES_SCROLL_TIME', 'ES_SCROLL_SIZE'],
                  'GoogleAnalyticsSettings': ['GA_ACCOUNT', 'GA_RUN_IN_PROD', 'GA_EVENT_CATEGORY', 'GA_EVENT_GET_ACTION', 'GA_EVENT_POST_ACTION', 'GA_TRACKER_URL'],
                  'URLSettings': ['ANNOTATION_ENDPOINT', 'QUERY_ENDPOINT', 'API_VERSION', 'STATUS_CHECK_ID', 'FIELD_NOTES_PATH']
                 }



class BiothingConfigError(Exception):
    pass

class BiothingSettings():
    def __init__(self, config_path = None):
        if not config_path:
            config_path = os.environ["BIOTHING_SETTINGS"]
        try:
            default_values = configparser.ConfigParser()
            default_values.read('biothings/settings/default.ini')
            config = configparser.ConfigParser()
            config.read(config_path)
        except:
            raise BiothingConfigError("Incorrect or missing config file.  See https://docs.python.org/3.4/library/configparser.html?highlight=config#module-configparser for config file format.")

        c = {}

        for section in config.sections():
            for option in config.options(section):
                c[section].append((option, ast.literal_eval(config.get(section, option))))
        c[section] = dict(c[section])

        # Replace missing required values with defaults
        for (section, olist) in REQUIRED_OPTIONS.items():
            for option in olist:
                if option not in c[section]:
                    c[section][option] = ast.literal_eval(default_values.get(section, option))

        self._config_dict = c


    def get_config_value(self, section, key):
        # Get a value from the config file
        if key in self._config_dict[section]:
            return self._config_dict[section][key]
        return None

    @property
    def _annotation_endpoint(self):
        return self.get_config_value('URLSettings', 'ANNOTATION_ENDPOINT')

    @property
    def _query_endpoint(self):
        return self.get_config_value('URLSettings', 'QUERY_ENDPOINT')

    @property
    def _api_version(self):
        if self.get_config_value('URLSettings', 'API_VERSION'):
            return self.get_config_value('URLSettings', 'API_VERSION')
        else:
            return ''

    @property
    def status_check_id(self):
        return self.get_config_value('URLSettings', 'STATUS_CHECK_ID')

    @property
    def field_notes_path(self):
        return self.get_config_value('URLSettings', 'FIELD_NOTES_PATH')

    # *************************************************************************
    # * Elasticsearch functions and properties
    # *************************************************************************

    @property
    def es_host(self):
        return self.get_config_value('ElasticsearchSettings', 'ES_HOST')

    @property
    def es_index(self):
        return self.get_config_value('ElasticsearchSettings', 'ES_INDEX')

    @property
    def es_doc_type(self):
        return self.get_config_value('ElasticsearchSettings', 'ES_DOC_TYPE')

    @property
    def allowed_options(self):
        return self.get_config_value('ElasticsearchSettings', 'ALLOWED_OPTIONS')

    @property
    def scroll_time(self):
        return self.get_config_value('ElasticsearchSettings', 'ES_SCROLL_TIME')

    @property
    def scroll_size(self):
        return self.get_config_value('ElasticsearchSettings', 'ES_SCROLL_SIZE')

    # *************************************************************************
    # * Google Analytics API tracking object functions
    # *************************************************************************

    @property
    def ga_event_for_get_action(self):
        return self.get_config_value('GoogleAnalyticsSettings', 'GA_EVENT_GET_ACTION')

    @property
    def ga_event_for_post_action(self):
        return self.get_config_value('GoogleAnalyticsSettings', 'GA_EVENT_POST_ACTION')

    @property
    def ga_event_category(self):
        return self.get_config_value('GoogleAnalyticsSettings', 'GA_EVENT_CATEGORY')

    @property
    def ga_is_prod(self):
        return self.get_config_value('GoogleAnalyticsSettings', 'GA_RUN_IN_PROD')

    @property
    def ga_account(self):
        return self.get_config_value('GoogleAnalyticsSettings', 'GA_ACCOUNT')

    @property
    def ga_tracker_url(self):
        return self.get_config_value('GoogleAnalyticsSettings', 'GA_TRACKER_URL')

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