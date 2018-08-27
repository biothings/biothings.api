from biothings.web.api.es.query import ESQuery as DefaultESQuery
from biothings.web.api.es.query_builder import ESQueryBuilder as DefaultESQueryBuilder
from biothings.web.api.es.transform import ESResultTransformer as DefaultESResultTransformer
from biothings.web.templates import HTML_OUT_TEMPLATE
import re

# *****************************************************************************
# Elasticsearch variables
# *****************************************************************************
# elasticsearch server transport url
ES_HOST = 'localhost:9200'
# timeout for python es client (global request timeout)
ES_CLIENT_TIMEOUT = 120
# elasticsearch index name
ES_INDEX = 'mybiothing_current'
# elasticsearch document type
ES_DOC_TYPE = 'biothing'
# Amount of time a scroll request is kept open
ES_SCROLL_TIME = '1m'
# Size of each scroll request return
ES_SCROLL_SIZE = 1000
# Maximum size of result return
ES_SIZE_CAP = 1000
# Maximum result window => maximum for "from" parameter
ES_RESULT_WINDOW_SIZE_CAP = 10000

# For the userquery folder for this app
USERQUERY_DIR = ''

# default static path, relative to current working dir
# (from where app is launched)
STATIC_PATH = "static"

# api version in the URL patterns and elsewhere
API_VERSION = 'v1'
# project URL routing
APP_LIST = []

# *****************************************************************************
# Subclass of biothings.web.api.es.query_builder.ESQueryBuilder to build
# queries for this app
# *****************************************************************************
ES_QUERY_BUILDER = DefaultESQueryBuilder
# *****************************************************************************
# Subclass of biothings.web.api.es.query.ESQuery to execute queries for this app
# *****************************************************************************
ES_QUERY = DefaultESQuery
# *****************************************************************************
# Subclass of biothings.web.api.es.transform.ESResultTransformer to transform
# ES results for this app
# *****************************************************************************
ES_RESULT_TRANSFORMER = DefaultESResultTransformer

OUTPUT_KEY_ALIASES = {}
#OUTPUT_KEY_ALIASES = {'cadd':'schmadd', 'cadd/gene/ccds_id': 'cces_id'}

# Global default cap for list inputs
LIST_SIZE_CAP = 1000

# For Returning a custom message in 4xx responses (or really any response)
ID_REQUIRED_MESSAGE = 'ID required'
ID_NOT_FOUND_TEMPLATE = "ID '{bid}' not found"

# TODO: Need to describe
#ANNOTATION_ID_REGEX_LIST = [(re.compile(r'rs[0-9]+', re.I), 'dbsnp.rsid')]
ANNOTATION_ID_REGEX_LIST = []

# USERQUERY KWARGS

# regex to identify a userquery arg
USERQUERY_KWARG_REGEX = re.compile(r'^uq_\w+$')
# transform to use on the userquery arg
USERQUERY_KWARG_TRANSFORM = lambda x: x[3:]

# kwargs passed into Elasticsearch.get() for /status endpoint
STATUS_CHECK = {
    'id': '',
    'index': '',
    'doc_type': ''
}

# These kwarg descriptions are used in generating a swagger API spec
KWARG_DESCRIPTIONS = {
    '_source': {'name': 'fields', 'text_template': 'a comma-separated list of fields (in dotfield notation) used to limit the fields returned from the matching {biothing_object} hit(s). The supported field names can be found from any {biothing_object} object or from the /metadata/fields endpoint. If "fields=all", all available fields will be returned.{param_type}{param_default_value}{param_max}'},

    'size': {'name': 'size', 'text_template': 'the maximum number of matching {biothing_object} hits to return per batch.{param_type}{param_default_value}{param_max}'},

    'from': {'name': 'from', 'text_template': 'the number of matching {biothing_object} hits to skip, starting from 0.  This can be useful for paging in combination with the "size" parameter.{param_type}{param_default_value}{param_max}'},

    'sort': {'name': 'sort', 'text_template':'the comma-separated list of fields to sort on. Prefix each with "-" for descending order, otherwise in ascending order. Default: sort by descending score.' },

    'dotfield': {'name': 'dotfield', 'text_template': 'control the format of the returned {biothing_object} object. If "true" or "1", all fields will be collapsed into a single level deep object (all nested objects will be a single level deep, using dotfield notation to signify the nested structure){param_type}{param_default_value}{param_max}'},

    'callback': {'name': 'callback', 'text_template': 'you can pass a "callback" parameter to make a JSONP call. Type: string.'},

    'email': {'name': 'email', 'text_template': 'If you are regular users of our services, we encourage you to provide us with an email, so that we can better track the usage or follow up with you.'},

    'out_format': {'name': 'format', 'text_template': 'controls output format of server response, currently supports: "json", "jsonld", "html".{param_type}{param_default_value}{param_max}'},

    'aggs': {'name': 'facets', 'text_template': 'a comma-separated list of fields to return facets on.  In addition to query hits, the fields notated in "facets" will be aggregated by value and bucklet counts will be displayed in the "facets" field of the response object.{param_type}{param_default_value}{param_max}'},

    'facet_size': {'name': 'facet_size', 'text_template': 'the number of facet buckets to return in the response.{param_type}{param_default_value}{param_max}'},
    
    'ids': {'name': 'ids', 'text_template': 'multiple {biothing_object} ids separated by comma. Note that currently we only take the input ids up to 1000 maximum, the rest will be omitted.{param_type}{param_default_value}{param_max}'},

    'q': {'name': 'q', 'text_template': 'Query string.  The detailed query syntax can be found from our [docs]{doc_query_syntax_url}'},

    'scopes': {'name': 'scopes', 'text_template': 'a comma-separated list of fields as the search "scopes" (fields to search through for query term). The available "fields" that can be passed to the "scopes" parameter are listed in the **/metadata/fields** endpoint.{param_type} Default: "scopes=_id".{param_max}'},

    'search': {'name': 'search', 'text_template': 'Pass a search term to filter the available fields.{param_type}{param_default_value}{param_max}'},

    'prefix': {'name': 'prefix', 'text_template': 'Pass a prefix string to filter the available fields.{param_type}{param_default_value}{param_max}'}
}
# Keyword Argument Control
# 
# These parameters control which input kwargs go to which kwarg group for each
# endpoint and operation (e.g. query GET, annotation POST, etc).
# This allows explicit grouping of parameters into inputs of the pipeline
# section that they are germane to.
# CONTROL_KWARGS - general category, used for handler parameters (e.g. raw, rawquery)
# ES_KWARGS go directly to the ESQuery function (e.g. fields, size...)
# ESQB_KWARGS are used to instantiate a query builder class
# TRANSFORM_KWARGS are used to instantiate a result transformer class
# 
# keys are parameter names, values are default values.  If value == None, no
# default is inserted.

# For annotation GET endpoint
ANNOTATION_GET_CONTROL_KWARGS = {'raw': {'default': False, 'type': bool}, 
                                 'rawquery': {'default': False, 'type': bool},
                                 'out_format': {'default': 'json', 'type': str, 'alias':'format'}}
ANNOTATION_GET_ES_KWARGS = {'_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'filter']}}
ANNOTATION_GET_ESQB_KWARGS = {}
ANNOTATION_GET_TRANSFORM_KWARGS = {'dotfield': {'default': False, 'type': bool}, 
                                   'jsonld': {'default': False, 'type': bool},
                                   '_sorted': {'default': True, 'type': bool},
                                    'always_list': {'default': [], 'type': list, 'max': 1000},
                                    'allow_null': {'default': [], 'type': list, 'max': 1000}}

# For annotation POST endpoint
ANNOTATION_POST_CONTROL_KWARGS = {'raw': {'default': False, 'type': bool},
                                  'rawquery': {'default': False, 'type': bool},
                                  'ids': {'default': None, 'type': list, 'max': 1000},
                                  'out_format': {'default': 'json', 'type': str, 'alias': 'format'}}
ANNOTATION_POST_ES_KWARGS = {'_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'filter']}}
ANNOTATION_POST_ESQB_KWARGS = {}
ANNOTATION_POST_TRANSFORM_KWARGS = {'dotfield': {'default': False, 'type': bool},
                                    'jsonld': {'default': False, 'type': bool},
                                    '_sorted': {'default': True, 'type': bool},
                                    'always_list': {'default': [], 'type': list, 'max': 1000},
                                    'allow_null': {'default': [], 'type': list, 'max': 1000}}

# For query GET endpoint
QUERY_GET_CONTROL_KWARGS = {'raw': {'default': False, 'type': bool},
                            'rawquery': {'default': False, 'type': bool},
                            'q': {'default': None, 'type': str, 
                                'translations': [
                                    #(re.compile(r'chr:', re.I), r'chrom:')
                                ]
                            },
                            'scroll_id': {'default': None, 'type': str},
                            'fetch_all': {'default': False, 'type': bool},
                            'out_format': {'default': 'json', 'type': str, 'alias': 'format'}}
QUERY_GET_ES_KWARGS = {'_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'filter']},
                       'from': {'default': None, 'type': int, 'alias': 'skip'},
                       'size': {'default': None, 'type': int, 'alias': 'limit'},
                       'explain': {'default': None, 'type': bool},
                       'aggs': {'default': None, 'type': list, 'max': 1000, 'alias': 'facets'},
                       'sort': {'default': None, 'type': list, 'max': 1000}}
QUERY_GET_ESQB_KWARGS = {'fetch_all': {'default': False, 'type': bool},
                         'userquery': {'default': None, 'type': str, 'alias': ['userfilter']},
                         'facet_size': {'default': 10, 'type': int, 'max': 1000}}
QUERY_GET_TRANSFORM_KWARGS = {'dotfield': {'default': False, 'type': bool},
                              'jsonld': {'default': False, 'type': bool},
                              '_sorted': {'default': True, 'type': bool},
                              'always_list': {'default': [], 'type': list, 'max': 1000},
                              'allow_null': {'default': [], 'type': list, 'max': 1000}}

# For query POST endpoint
QUERY_POST_CONTROL_KWARGS = {'q': {'default': None, 'type': list},
                             'raw': {'default': False, 'type': bool},
                             'rawquery': {'default': False, 'type': bool},
                             'out_format': {'default': 'json', 'type': str, 'alias': 'format'}}
QUERY_POST_ES_KWARGS = {'_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'filter']},
                        'size': {'default': None, 'type': int}}
QUERY_POST_ESQB_KWARGS = {'scopes': {'default': None, 'type': list, 'max': 1000, 
                            'translations': [

                            ]}}
QUERY_POST_TRANSFORM_KWARGS = {'dotfield': {'default': False, 'type': bool}, 
                               'jsonld': {'default': False, 'type': bool},
                               '_sorted': {'default': True, 'type': bool},
                               'always_list': {'default': [], 'type': list, 'max': 1000},
                               'allow_null': {'default': [], 'type': list, 'max': 1000}}

# For metadata GET endpoint
METADATA_GET_CONTROL_KWARGS = {'out_format': {'default': 'json', 'type': str, 'alias': 'format'}}
METADATA_GET_ES_KWARGS = {}
METADATA_GET_ESQB_KWARGS = {}
METADATA_GET_TRANSFORM_KWARGS = {'dev': {'default': False, 'type': bool}, 
                                 'search': {'default': None, 'type': str},
                                 'prefix': {'default': None, 'type': str}}

# *****************************************************************************
# Google Analytics Settings
# *****************************************************************************

# Google Analytics Account ID
GA_ACCOUNT = ''

# Turn this to True to start google analytics tracking
GA_RUN_IN_PROD = False

# url for google analytics tracker
GA_TRACKER_URL = 'mybiothing.info'
GA_ACTION_QUERY_GET = 'query_get'
GA_ACTION_QUERY_POST = 'query_post'
GA_ACTION_ANNOTATION_GET = 'biothing_get'
GA_ACTION_ANNOTATION_POST = 'biothing_post'

# for standalone instance tracking
STANDALONE_TRACKING_URL = ''
# dictionary with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY k,v for standalone user AWS IAM
STANDALONE_AWS_CREDENTIALS = {}
# batch size for standalone tracking (sending requests to AWS lambda)
STANDALONE_TRACKING_BATCH_SIZE = 1000

# override with url for specific project
URL_BASE = 'http://mybiothing.info'

# parameter for JSONP
JSONP_PARAMETER = 'callback'

# Caching behavior
# should caching be disabled by default on handlers?
DISABLE_CACHING = False
CACHE_MAX_AGE = 604800

# Sentry project address
SENTRY_CLIENT_KEY = ''

# JSON-LD PATH
JSONLD_CONTEXT_PATH = ''

# Can turn msgpack functionality off here, will still load msgpack module if available, just won't
# use it to compress requests
ENABLE_MSGPACK = True

LIST_SPLIT_REGEX = re.compile('[\s\r\n+|,]+')

DEFAULT_SCOPES = ['_id']

# path to the git repository for the app-specific code, override
APP_GIT_REPOSITORY = '../'

# ***************************************************************
# * For Hipchat exception logging
# ***************************************************************
HIPCHAT_ROOM=''
HIPCHAT_AUTH_TOKEN=''
HIPCHAT_MESSAGE_COLOR='yellow'
HIPCHAT_AUTO_FROM_SOCKET_CONNECTION=('8.8.8.8', 53) # google DNS server
HIPCHAT_MESSAGE_FORMAT=None

# For format=html
HTML_OUT_HEADER_IMG = "//:0"
HTML_OUT_TITLE = "<p>MyBioThing.info</p>"
METADATA_DOCS_URL = "javascript:;"
QUERY_DOCS_URL = "javascript:;"
ANNOTATION_DOCS_URL = "javascript:;"
