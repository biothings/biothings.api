"""
    Biothings Web Settings Default
"""


from .descriptions import KWARG_DESCRIPTIONS

# *****************************************************************************
# Elasticsearch Settings
# *****************************************************************************
# elasticsearch server transport url
ES_HOST = 'localhost:9200'
# load balancing by connecting to all nodes in the cluster
ES_SNIFF = False
# timeout for python es client (global request timeout)
ES_CLIENT_TIMEOUT = 120
# elasticsearch index name
ES_INDEX = '_all'
# elasticsearch document type for es<7, also biothing type
ES_DOC_TYPE = 'all'
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
BIOTHING_TYPES = ()

# api version in the URL patterns and elsewhere
API_PREFIX = ''
API_VERSION = 'v1'

# project URL routing
APP_LIST = [
    (r"/", 'biothings.web.handlers.FrontPageHandler'),
    (r"/({pre})/", 'tornado.web.RedirectHandler', {"url": "/{0}"}),
    (r"/{pre}/status", 'biothings.web.handlers.StatusHandler'),
    (r"/{pre}/metadata/fields/?", 'biothings.web.handlers.MetadataFieldHandler'),
    (r"/{pre}/metadata/?", 'biothings.web.handlers.MetadataSourceHandler'),
    (r"/{pre}/{ver}/spec/?", 'biothings.web.handlers.APISpecificationHandler'),
    (r"/{pre}/{ver}/{typ}/metadata/fields/?", 'biothings.web.handlers.MetadataFieldHandler'),
    (r"/{pre}/{ver}/{typ}/metadata/?", 'biothings.web.handlers.MetadataSourceHandler'),
    (r"/{pre}/{ver}/{typ}/query/?", 'biothings.web.handlers.QueryHandler'),
    (r"/{pre}/{ver}/{typ}/([^\/]+)/?", 'biothings.web.handlers.BiothingHandler'),
    (r"/{pre}/{ver}/{typ}/?", 'biothings.web.handlers.BiothingHandler'),
    (r"/{pre}/{ver}/metadata/fields/?", 'biothings.web.handlers.MetadataFieldHandler'),
    (r"/{pre}/{ver}/metadata/?", 'biothings.web.handlers.MetadataSourceHandler'),
    (r"/{pre}/{ver}/query/?", 'biothings.web.handlers.QueryHandler'),
]

# string used in headers to support CORS
ACCESS_CONTROL_ALLOW_METHODS = 'GET,POST,OPTIONS'
ACCESS_CONTROL_ALLOW_HEADERS = (
    'Content-Type, Depth, User-Agent, If-Modified-Since,'
    'Cache-Control, X-File-Size, X-Requested-With, X-File-Name'
)
# Caching behavior
DISABLE_CACHING = False
CACHE_MAX_AGE = 604800  # default 7 days

# Global default cap for list inputs
LIST_SIZE_CAP = 1000

# For format=html
HTML_OUT_HEADER_IMG = "https://biothings.io/static/favicon.ico"
HTML_OUT_TITLE = "<p>Biothings API</p>"
METADATA_DOCS_URL = "javascript:;"
QUERY_DOCS_URL = "javascript:;"
ANNOTATION_DOCS_URL = "javascript:;"

# path to the git repository for the app-specific code, override
APP_GIT_REPOSITORY = '.'

# default static path, relative to current working dir
# (from where app is launched)
STATIC_PATH = "static"

# color support is provided by tornado.log
LOGGING_FORMAT = "%(color)s[%(levelname)s %(name)s %(module)s:%(lineno)d]%(end_color)s %(message)s"

# *****************************************************************************
# User Input Control
# *****************************************************************************
COMMON_KWARGS = {
    # control group
    'raw': {'type': bool, 'default': False, 'group': 'control'},
    'rawquery': {'type': bool, 'default': False, 'group': 'control'},
    # esqb group
    '_source': {'type': list, 'group': 'esqb', 'max': 1000, 'alias': ['fields', 'field', 'filter']},
    'size': {'type': int, 'group': 'esqb', 'max': 1000, 'alias': 'limit'},
    # transform group
    'dotfield': {'type': bool, 'default': False, 'group': 'transform'},
    '_sorted': {'type': bool, 'default': True, 'group': 'transform'},  # alaphabetically
    'always_list': {'type': list, 'group': 'transform', 'max': 1000},
    'allow_null': {'type': list, 'group': 'transform', 'max': 1000}
}
ANNOTATION_KWARGS = {
    '*': COMMON_KWARGS.copy(),
    'GET': {'id': {'type': str, 'path': 0, 'required': True}},
    'POST': {'ids': {'type': list, 'max': 1000, 'required': True, 'strict': False}}
}
QUERY_KWARGS = {
    '*': COMMON_KWARGS.copy(),
    'GET': {'q': {'type': str, 'default': '__all__', 'group': 'esqb', 'strict': False},
            'aggs': {'type': list, 'max': 1000, 'group': 'esqb', 'alias': 'facets'},
            'facet_size': {'type': int, 'default': 10, 'max': 1000, 'group': 'esqb'},
            'from': {'type': int, 'max': 10000, 'group': 'esqb', 'alias': 'skip'},
            'userquery': {'type': str, 'group': 'esqb', 'alias': ['userfilter']},
            'sort': {'type': list, 'group': 'esqb', 'max': 1000},
            'explain': {'type': bool, 'group': 'esqb'},
            'fetch_all': {'type': bool, 'group': 'es'},
            'scroll_id': {'type': str, 'group': 'es'}},
    'POST': {'q': {'type': list, 'required': True, 'group': 'esqb', 'strict': False},
             'scopes': {'type': list, 'default': ['_id'], 'group': 'esqb', 'max': 1000, 'strict': False}}
}
# *****************************************************************************
# Elasticsearch Query Pipeline
# *****************************************************************************
ES_QUERY_BUILDER = 'biothings.web.pipeline.ESQueryBuilder'
# For the userquery folder for this app
USERQUERY_DIR = 'userquery'
# Allow the __any__ random doc retrieval
ALLOW_RANDOM_QUERY = False
# Allow facets to be nested with ( )
ALLOW_NESTED_AGGS = False

ES_QUERY_BACKEND = 'biothings.web.pipeline.ESQueryBackend'
ES_RESULT_TRANSFORM = 'biothings.web.pipeline.ESResultTransform'

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
# Annotation #
ANNOTATION_DEFAULT_SCOPES = ['_id']
ANNOTATION_ID_REGEX_LIST = []  # [(re.compile(r'rs[0-9]+', re.I), 'dbsnp.rsid')]
#
# Status #
# https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-get.html
STATUS_CHECK = {
    # 'id': '',
    # 'index': '',
    # 'doc_type': ''
}
#
# Biothing #
ID_REQUIRED_MESSAGE = 'ID required'
ID_NOT_FOUND_TEMPLATE = "ID '{bid}' not found"
