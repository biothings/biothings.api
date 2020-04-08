"""
    Biothings Web Settings Default
"""

import re

from biothings.web.templates import HTML_OUT_TEMPLATE

from .templates import KWARG_DESCRIPTIONS

# *****************************************************************************
# Elasticsearch Settings
# *****************************************************************************
# elasticsearch server transport url
ES_HOST = 'localhost:9200'
# timeout for python es client (global request timeout)
ES_CLIENT_TIMEOUT = 120
# elasticsearch index name
ES_INDEX = '_all'
# elasticsearch document type for es<7, also biothing type
ES_DOC_TYPE = 'doc'
# additional index support
ES_INDICES = {
    # "biothing_type_1": "index1",
    # "biothing_type_2": "index1,alias1,pattern_*"
}
# Amount of time a scroll request is kept open
ES_SCROLL_TIME = '1m'
# Size of each scroll request return
ES_SCROLL_SIZE = 1000
# Maximum size of result return
ES_SIZE_CAP = 1000
# Maximum result window => maximum for "from" parameter
ES_RESULT_WINDOW_SIZE_CAP = 10000

# *****************************************************************************
# Web Application & Base Handler
# *****************************************************************************
BIOTHING_TYPES = []

# api version in the URL patterns and elsewhere
API_PREFIX = ''
API_VERSION = 'v1'

# project URL routing
APP_LIST = [
    (r"/{pre}/status", 'biothings.web.api.es.handlers.StatusHandler'),
    (r"/{pre}/metadata/?", 'biothings.web.api.es.handlers.MetadataSourceHandler'),
    (r"/{pre}/metadata/fields/?", 'biothings.web.api.es.handlers.MetadataFieldHandler'),
    (r"/{pre}/{ver}/query", 'biothings.web.api.es.handlers.QueryHandler'),
    (r"/{pre}/{ver}/{typ}/query", 'biothings.web.api.es.handlers.QueryHandler'),
    (r"/{pre}/{ver}/{typ}/?", 'biothings.web.api.es.handlers.BiothingHandler'),
    (r"/{pre}/{ver}/{typ}/([^\/]+)/?", 'biothings.web.api.es.handlers.BiothingHandler'),
    (r"/{pre}/{ver}/metadata/?", 'biothings.web.api.es.handlers.MetadataSourceHandler'),
    (r"/{pre}/{ver}/metadata/fields/?", 'biothings.web.api.es.handlers.MetadataFieldHandler'),
    (r"/{pre}/{ver}/{typ}/metadata/?", 'biothings.web.api.es.handlers.MetadataSourceHandler'),
    (r"/{pre}/{ver}/{typ}/metadata/fields/?", 'biothings.web.api.es.handlers.MetadataFieldHandler'),
]

# string used in headers to support CORS
ACCESS_CONTROL_ALLOW_METHODS = 'GET,POST,OPTIONS'
ACCESS_CONTROL_ALLOW_HEADERS = ('Content-Type, Depth, User-Agent, If-Modified-Since,'
                                'Cache-Control, X-File-Size, X-Requested-With, X-File-Name')
# Caching behavior
DISABLE_CACHING = False
CACHE_MAX_AGE = 604800  # default 7 days

# Global default cap for list inputs
LIST_SIZE_CAP = 1000

# override with url for specific project
URL_BASE = 'http://mybiothing.info'

# Can turn msgpack functionality off here,
# will still load msgpack module if available, just won't
# use it to compress requests
ENABLE_MSGPACK = True

# For format=html
HTML_OUT_HEADER_IMG = "//:0"
HTML_OUT_TITLE = "<p>MyBioThing.info</p>"
METADATA_DOCS_URL = "javascript:;"
QUERY_DOCS_URL = "javascript:;"
ANNOTATION_DOCS_URL = "javascript:;"

# path to the git repository for the app-specific code, override
APP_GIT_REPOSITORY = '.'

# default static path, relative to current working dir
# (from where app is launched)
STATIC_PATH = "static"

# parameter for JSONP
JSONP_PARAMETER = 'callback'

# TODO
LIST_SPLIT_REGEX = re.compile(r'[\s\r\n+|,]+')

# *****************************************************************************
# User Input Control
# *****************************************************************************
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

COMMON_CONTROL_KWARGS = {
    'raw': {'default': False, 'type': bool},
    'rawquery': {'default': False, 'type': bool},
    'out_format': {'default': 'json', 'type': str, 'alias': ['format']}}
COMMON_TRANSFORM_KWARGS = {
    'dotfield': {'default': False, 'type': bool},
    'jsonld': {'default': False, 'type': bool},
    '_sorted': {'default': True, 'type': bool},  # alaphabetically
    'always_list': {'default': [], 'type': list, 'max': 1000},
    'allow_null': {'default': [], 'type': list, 'max': 1000}}

# For annotation GET endpoint
ANNOTATION_GET_CONTROL_KWARGS = dict(COMMON_CONTROL_KWARGS)
ANNOTATION_GET_ESQB_KWARGS = {
    'id': {'default': '', 'type': str, 'path': 0},
    '_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'filter']}}
ANNOTATION_GET_ES_KWARGS = {}
ANNOTATION_GET_TRANSFORM_KWARGS = dict(COMMON_TRANSFORM_KWARGS)

# For annotation POST endpoint
ANNOTATION_POST_CONTROL_KWARGS = dict(COMMON_CONTROL_KWARGS)
ANNOTATION_POST_ESQB_KWARGS = {
    'ids': {'default': None, 'type': list, 'max': 1000, 'required': True},
    '_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'filter']}}
ANNOTATION_POST_ES_KWARGS = {}
ANNOTATION_POST_TRANSFORM_KWARGS = dict(COMMON_TRANSFORM_KWARGS)

# For query GET endpoint
QUERY_GET_CONTROL_KWARGS = dict(COMMON_CONTROL_KWARGS)
QUERY_GET_ESQB_KWARGS = {
    'q': {'default': '', 'type': str,
          'translations': []},  # (re.compile(r'chr:', re.I), r'chrom:')
    'aggs': {'default': None, 'type': list, 'max': 1000, 'alias': 'facets'},
    'facet_size': {'default': 10, 'type': int, 'max': 1000},
    '_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'field', 'filter']},
    'from': {'default': None, 'type': int, 'max': 10000, 'alias': 'skip'},
    'size': {'default': None, 'type': int, 'max': 1000, 'alias': 'limit'},
    'explain': {'default': None, 'type': bool},
    'sort': {'default': None, 'type': list, 'max': 1000},
    'userquery': {'default': None, 'type': str, 'alias': ['userfilter']}}
QUERY_GET_ES_KWARGS = {
    'fetch_all': {'default': None, 'type': bool},
    'scroll_id': {'default': None, 'type': str}}
QUERY_GET_TRANSFORM_KWARGS = dict(COMMON_TRANSFORM_KWARGS)

# For query POST endpoint
QUERY_POST_CONTROL_KWARGS = dict(COMMON_CONTROL_KWARGS)
QUERY_POST_ESQB_KWARGS = {
    'q': {'default': None, 'type': list, 'required': True},
    'scopes': {'default': ['_id'], 'type': list, 'max': 1000, 'translations': []},
    '_source': {'default': None, 'type': list, 'max': 1000, 'alias': ['fields', 'filter']},
    'size': {'default': None, 'type': int}}
QUERY_POST_ES_KWARGS = {}
QUERY_POST_TRANSFORM_KWARGS = dict(COMMON_TRANSFORM_KWARGS)

# For metadata GET endpoint
METADATA_GET_CONTROL_KWARGS = {
    'out_format': {'default': 'json', 'type': str, 'alias': ['format']}}
METADATA_GET_SOURCE_KWARGS = {
    'dev': {'default': False, 'type': bool}, }
METADATA_GET_FIELDS_KWARGS = {
    'raw': {'default': False, 'type': bool},
    'search': {'default': None, 'type': str},
    'prefix': {'default': None, 'type': str}}

# *****************************************************************************
# Elasticsearch Query Builder
# *****************************************************************************
ES_QUERY_BUILDER = 'biothings.web.api.es.pipelines.ESQueryBuilder'
# For the userquery folder for this app
USERQUERY_DIR = ''
# Allow the __any__ random doc retrieval
ALLOW_RANDOM_QUERY = False
# Allow facets to be nested with ( )
ALLOW_NESTED_AGGS = False

# *****************************************************************************
# Elasticsearch Query Execution
# *****************************************************************************
ES_QUERY_BACKEND = 'biothings.web.api.es.pipelines.ESQueryBackend'

# *****************************************************************************
# Elasticsearch Result Transform
# *****************************************************************************
ES_RESULT_TRANSFORM = 'biothings.web.api.es.pipelines.ESResultTransform'

OUTPUT_KEY_ALIASES = {}
#OUTPUT_KEY_ALIASES = {'cadd':'schmadd', 'cadd/gene/ccds_id': 'cces_id'}

# A list of fields to exclude from metadata/fields endpoint
AVAILABLE_FIELDS_EXCLUDED = ['all']

# A path to the available fields notes
AVAILABLE_FIELDS_NOTES_PATH = ''

LICENSE_TRANSFORM = {
    # "alias" :  "datasource",
    # "dot.field" :  "datasource"
}

# *****************************************************************************
# Analytics Settings
# *****************************************************************************

# Sentry project address
SENTRY_CLIENT_KEY = ''

# Google Analytics Account ID
GA_ACCOUNT = ''

# Turn this to True to start google analytics tracking
GA_RUN_IN_PROD = False

# url for google analytics tracker
GA_TRACKER_URL = 'mybiothing.info'  # TODO
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


# *****************************************************************************
# Endpoints Specifics & Others
# *****************************************************************************
### Annotation ###

ANNOTATION_DEFAULT_SCOPES = ['_id']
#ANNOTATION_ID_REGEX_LIST = [(re.compile(r'rs[0-9]+', re.I), 'dbsnp.rsid')]
ANNOTATION_ID_REGEX_LIST = []

### Status ###

# https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-get.html
STATUS_CHECK = {
    # 'id': '',
    # 'index': '',
    # 'doc_type': ''
}

### Biothing ###

# For Returning a custom message in 4xx responses (or really any response)
ID_REQUIRED_MESSAGE = 'ID required'
ID_NOT_FOUND_TEMPLATE = "ID '{bid}' not found"
