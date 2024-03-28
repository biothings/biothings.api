"""
    Biothings Web Settings Default
"""

import re

# *****************************************************************************
# biothings.web.launcher
# *****************************************************************************
# color support is provided by tornado.log
LOGGING_FORMAT = "%(color)s[%(levelname)s %(name)s:%(lineno)d]%(end_color)s %(message)s"

# *****************************************************************************
# Elasticsearch Settings
# *****************************************************************************
ES_HOST = "http://localhost:9200"
ES_INDICES = {
    None: "_all",
    "doc": "_all",
    # "biothing_type_1": "index1",
    # "biothing_type_2": "index1,alias1,pattern_*"
}
ES_ARGS = {
    # https://elasticsearch-py.readthedocs.io/en/v7.12.1/connection.html
    "sniff": False,  # this is a shortcut to configure multiple values
    "request_timeout": 60,  # increase from default (10s) to support heavy query
}

# *****************************************************************************
# MongoDB Settings
# *****************************************************************************
# mongodb://username:password@host/dbname
MONGO_URI = ""
MONGO_COLS = {
    # "biothing_type_1": "collectionA",
    # "biothing_type_2": "collectionB"
}
MONGO_ARGS = {
    # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html \
    # #pymongo.mongo_client.MongoClient
    "connect": False,  # lazy connection to speed up initialization
    "tz_aware": True,  # to maintain consistency with the hub design
}

# *****************************************************************************
# SQL Settings
# *****************************************************************************
# https://docs.sqlalchemy.org/en/14/core/engines.html
# dialect[+driver]://username:password@host/dbname

# mysql+pymysql://username:password@host/dbname
# postgresql://username:password@host/dbname
# sqlite:///filepath
SQL_URI = ""
SQL_TBLS = {
    # "biothing_type_1": "customers",
    # "biothing_type_2": "students, classes",
    # "biothing_type_3": "orders JOIN customers ON orders.cid = customers.id",
}
SQL_ARGS = {
    # https://docs.sqlalchemy.org/en/14/core/engines.html
    # #sqlalchemy.create_engine
    # #custom-dbapi-args
}


# *****************************************************************************
# Web Application
# *****************************************************************************

# Routing
APP_PREFIX = ""
APP_VERSION = "v1"
APP_LIST = [
    (r"/", "biothings.web.handlers.FrontPageHandler"),
    (r"/({pre})/", "tornado.web.RedirectHandler", {"url": "/{0}"}),
    (r"/{pre}/status", "biothings.web.handlers.StatusHandler"),
    (r"/{pre}/metadata/fields/?", "biothings.web.handlers.MetadataFieldHandler"),
    (r"/{pre}/metadata/?", "biothings.web.handlers.MetadataSourceHandler"),
    (r"/{pre}/{ver}/spec/?", "biothings.web.handlers.APISpecificationHandler"),
    (r"/{pre}/{ver}/{tps}/metadata/fields/?", "biothings.web.handlers.MetadataFieldHandler"),
    (r"/{pre}/{ver}/{tps}/metadata/?", "biothings.web.handlers.MetadataSourceHandler"),
    (r"/{pre}/{ver}/{tps}/query/?", "biothings.web.handlers.QueryHandler"),
    (r"/{pre}/{ver}/{typ}(?:/([^/]+))?/?", "biothings.web.handlers.BiothingHandler"),
    (r"/{pre}/{ver}/metadata/fields/?", "biothings.web.handlers.MetadataFieldHandler"),
    (r"/{pre}/{ver}/metadata/?", "biothings.web.handlers.MetadataSourceHandler"),
    (r"/{pre}/{ver}/query/?", "biothings.web.handlers.QueryHandler"),
]

# *****************************************************************************
# Authentication
# *****************************************************************************
AUTHN_PROVIDERS = ()

# *****************************************************************************
# User Input Control
# *****************************************************************************
COMMON_KWARGS = {
    # control flow interrupt
    "raw": {"type": bool, "default": False},
    "rawquery": {"type": bool, "default": False},
    # query builder stage
    "_source": {"type": list, "max": 1000, "alias": ("fields", "field")},
    "size": {"type": int, "max": 1000, "alias": "limit"},
    # formatter stage
    "dotfield": {"type": bool, "default": False},
    "_sorted": {"type": bool, "default": True},  # alaphabetically
    "always_list": {"type": list, "max": 1000},
    "allow_null": {"type": list, "max": 1000},
    "jmespath": {"type": str, "default": None},  # jmespath transformation
    # final handler write method stage:
    "format": {
        "type": str,
        "default": "json",
        "enum": ("json", "yaml", "html", "msgpack"),
    },
}
ANNOTATION_KWARGS = {
    "*": COMMON_KWARGS.copy(),
    "GET": {"id": {"type": str, "path": 0, "required": True}},
    "POST": {"id": {"type": list, "max": 1000, "required": True, "alias": "ids"}},
}
QUERY_KWARGS = {
    "*": {
        **COMMON_KWARGS.copy(),
        **{
            "from": {"type": int, "max": 10000, "alias": "skip"},
            "sort": {"type": list, "max": 10},
            # use to set extra filter, as a filter clause in a boolean query
            "filter": {"type": str, "default": None},
            # use to set post_filter query, this one does not impact facets
            "post_filter": {"type": str, "default": None},
        },
    },  # for py3.9+, we can just use `|` operator like `COMMON_KWARGS.copy() | {...}`
    "GET": {
        "q": {"type": str, "default": None},
        "aggs": {"type": list, "max": 1000, "alias": "facets"},
        "facet_size": {"type": int, "default": 10, "max": 1000},
        "userquery": {"type": str, "alias": ["userfilter"]},
        "explain": {"type": bool},
        "fetch_all": {"type": bool},
        "scroll_id": {"type": str},
    },
    "POST": {
        "q": {"type": list, "required": True},
        "scopes": {"type": list, "default": ["_id"], "max": 1000},
        "with_total": {"type": bool},
        "analyzer": {"type": str},  # any of built-in analyzer (overrides default index-time analyzer)
        # Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-analyzers.html
    },
}

# LONG TERM GOAL: REMOVE THESE COMPATIBILITY SETTINGS
# ONCE BIOTHINGS.CLIENT OLDER VERSIONS ARE NO LONGER USED
COMMON_KWARGS["_source"]["strict"] = False
COMMON_KWARGS["always_list"]["strict"] = False
COMMON_KWARGS["allow_null"]["strict"] = False
ANNOTATION_KWARGS["POST"]["id"]["strict"] = False
QUERY_KWARGS["GET"]["q"]["strict"] = False
QUERY_KWARGS["POST"]["q"]["strict"] = False
QUERY_KWARGS["POST"]["scopes"]["strict"] = False


# *****************************************************************************
# Elasticsearch Query Pipeline
# *****************************************************************************
ES_QUERY_PIPELINE = "biothings.web.query.AsyncESQueryPipeline"
ES_QUERY_BUILDER = "biothings.web.query.ESQueryBuilder"
ES_QUERY_BACKEND = "biothings.web.query.AsyncESQueryBackend"
ES_RESULT_TRANSFORM = "biothings.web.query.ESResultFormatter"

# Pipeline
# --------
ANNOTATION_MAX_MATCH = 1000

# Builder Stage
# -------------
# For the userquery folder for this app
USERQUERY_DIR = "userquery"
# Allow "truly" random order for q= __any__
ALLOW_RANDOM_QUERY = False
# Allow facets to be nested with ( )
ALLOW_NESTED_AGGS = False

# Backend Stage
# -------------
# Amount of time a scroll request is kept open
ES_SCROLL_TIME = "1m"
# Size of each scroll request return
ES_SCROLL_SIZE = 1000

# Transform Stage
# ---------------
# A list of fields to exclude from metadata/fields endpoint
AVAILABLE_FIELDS_EXCLUDED = ["all"]
# A path to the available fields notes
AVAILABLE_FIELDS_NOTES_PATH = ""
# Add "_license" fields in results
LICENSE_TRANSFORM = {
    # "alias" :  "datasource",
    # "dot.field" :  "datasource"
}

# *****************************************************************************
# Analytics Settings
# *****************************************************************************

# Sentry project address
SENTRY_CLIENT_KEY = ""

# Google Analytics Account ID
GA_ACCOUNT = ""

# *****************************************************************************
# Endpoints Specifics & Others
# *****************************************************************************
#
# Search
HTML_OUT_TITLE = ""  # HTML
HTML_OUT_HEADER_IMG = ""  # URL
HTML_OUT_ANNOTATION_DOCS = ""  # URL
HTML_OUT_METADATA_DOCS = ""  # URL
HTML_OUT_QUERY_DOCS = ""  # URL
#
# Annotation
ANNOTATION_DEFAULT_SCOPES = ["_id"]
ANNOTATION_ID_REGEX_LIST = []  # [(re.compile(r'rs[0-9]+', re.I), 'dbsnp.rsid')]

# The default pattern matches up to the first ":" character to represent the scope
# of the query. The scope represents the associated field to query against on the
# elasticsearch backend. The term is the "value" we wish to search for immediately
# preceding the ":" character. In this case we look for any number of word (\w) or
# or non-word (\W) characters to match against in the group to represent the value
ANNOTATION_DEFAULT_REGEX_PATTERN = (re.compile(r"(?P<scope>[^:]+):(?P<term>[\W\w]+)"), ())

#
# Status
# https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-get.html
STATUS_CHECK = {
    # 'index': ''
    # 'id': '',
}

# the default max-age value in the "Cache-Control" header for all BaseAPIHandler subclasses
DEFAULT_CACHE_MAX_AGE = 604800  # 7 days
