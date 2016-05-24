# *****************************************************************************
# Elasticsearch variables
# *****************************************************************************
# elasticsearch server transport url
ES_HOST = 'localhost:9200'
# elasticsearch index name
ES_INDEX_NAME = 'mybiothing_current'
# elasticsearch document type
ES_DOC_TYPE = 'biothing'
# Only these options are passed to the elasticsearch query from kwargs
ALLOWED_OPTIONS = ['_source', 'start', 'from_', 'size', 'sort', 'explain', 'version', 'aggs', 'fetch_all']
ES_SCROLL_TIME = '1m'
ES_SCROLL_SIZE = 1000
ES_SIZE_CAP = 1000
ES_QUERY_MODULE = 'biothings.www.api.es'

# Graph defaults
# By default turn graph app off
NEO4J_APP = False
# Default neo4j query module
NEO4J_QUERY_MODULE = 'biothings.www.api.neo4j'
# Address to query neo4j cypher endpoint
NEO4J_CYPHER_ENDPOINT = 'http://localhost:7474/db/data/cypher'
# Username/password for neo4j
NEO4J_USERNAME = 'neo4j'
NEO4J_PASSWORD = 'neo4j'

# default static path, relative to current working dir
# (from where app is launched)
STATIC_PATH = "static"

# *****************************************************************************
# Google Analytics Settings
# *****************************************************************************
# Google Analytics Account ID
GA_ACCOUNT = 'google_account'
# Turn this to True to start google analytics tracking
GA_RUN_IN_PROD = False

# 'category' in google analytics event object
GA_EVENT_CATEGORY = 'v1_api'
# 'action' for get request in google analytics event object
GA_EVENT_GET_ACTION = 'get'
# 'action' for post request in google analytics event object
GA_EVENT_POST_ACTION = 'post'
# url for google analytics tracker
GA_TRACKER_URL = 'mybiothing.info'

# *****************************************************************************
# URL settings
# *****************************************************************************
# For URL stuff
ANNOTATION_ENDPOINT = 'biothing'
QUERY_ENDPOINT = 'query'
GRAPH_QUERY_ENDPOINT = 'graph_query'
API_VERSION = 'v1'
# TODO Fill in a status id here
STATUS_CHECK_ID = 'status_id'
# Path to a file containing a json object with information about elasticsearch fields
FIELD_NOTES_PATH = 'field_notes_path'
# For the path to the json-ld context
JSONLD_CONTEXT_PATH = 'context'
# Module that contains the nosetest config
NOSETEST_SETTINGS = 'tests.nosetest_config'
