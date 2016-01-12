# -*- coding: utf-8 -*-
# *****************************************************************************
# Elasticsearch variables
# *****************************************************************************

# elasticsearch server transport url
ES_HOST = 'localhost:9200'
# elasticsearch index name
ES_INDEX_NAME = '${src_package}_current'
# elasticsearch document type
ES_DOC_TYPE = '${es_doctype}'
# Only these options are passed to the elasticsearch query from kwargs
ALLOWED_OPTIONS = ['_source', 'start', 'from_', 'size',
                   'sort', 'explain', 'version', 'facets', 'fetch_all', 'host']
ES_SCROLL_TIME = '1m'
ES_SCROLL_SIZE = 1000

# *****************************************************************************
# Google Analytics Settings
# *****************************************************************************

# Google Analytics Account ID
GA_ACCOUNT = ''
# Turn this to True to start google analytics tracking
GA_RUN_IN_PROD = False

# 'category' in google analytics event object
GA_EVENT_CATEGORY = 'v1_api'
# 'action' for get request in google analytics event object
GA_EVENT_GET_ACTION = 'get'
# 'action' for post request in google analytics event object
GA_EVENT_POST_ACTION = 'post'
# url for google analytics tracker
GA_TRACKER_URL = '${base_url}'

# *****************************************************************************
# URL settings
# *****************************************************************************

# For URL stuff
ANNOTATION_ENDPOINT = '${annotation_endpoint}'
QUERY_ENDPOINT = '${query_endpoint}'
API_VERSION = 'v1'
# TODO Fill in a status id here
STATUS_CHECK_ID = ''
# Path to a file containing a json object with information about elasticsearch fields
FIELD_NOTES_PATH = ''

# *****************************************************************************
#
# *****************************************************************************


# *****************************************************************************
# Settings class
# *****************************************************************************

class ${settings_class}():
    def __init__(self):
        pass

    @property
    def _annotation_endpoint(self):
        return ANNOTATION_ENDPOINT

    @property
    def _query_endpoint(self):
        return QUERY_ENDPOINT

    @property
    def _api_version(self):
        if API_VERSION:
            return API_VERSION
        else:
            return ''

    @property
    def status_check_id(self):
        return STATUS_CHECK_ID

    @property
    def field_notes_path(self):
        if FIELD_NOTES_PATH:
            return FIELD_NOTES_PATH
        else:
            return None

    @property
    def es_host(self):
        return ES_HOST

    @property
    def es_index(self):
        return ES_INDEX_NAME

    @property
    def es_doc_type(self):
        return ES_DOC_TYPE

    @property
    def allowed_options(self):
        return ALLOWED_OPTIONS

    @property
    def scroll_time(self):
        return ES_SCROLL_TIME

    @property
    def scroll_size(self):
        return ES_SCROLL_SIZE

    # *************************************************************************
    # * Google Analytics API tracking object functions
    # *************************************************************************

    @property
    def ga_event_for_get_action(self):
        return GA_EVENT_GET_ACTION

    @property
    def ga_event_for_post_action(self):
        return GA_EVENT_POST_ACTION

    @property
    def ga_event_category(self):
        return GA_EVENT_CATEGORY

    @property
    def ga_is_prod(self):
        return GA_RUN_IN_PROD

    @property
    def ga_account(self):
        return GA_ACCOUNT

    @property
    def ga_tracker_url(self):
        return GA_TRACKER_URL

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


