"""
    Web settings to override for testing.
"""
import os

from custom_cache_handler import *

from biothings.web.settings.default import APP_LIST, QUERY_KWARGS

# *****************************************************************************
# Elasticsearch Variables
# *****************************************************************************
ES_INDEX = "bts_test"
ES_DOC_TYPE = "gene"
ES_SCROLL_SIZE = 60

# *****************************************************************************
# User Input Control
# *****************************************************************************
# use a smaller size for testing
QUERY_KWARGS["GET"]["facet_size"]["default"] = 3
QUERY_KWARGS["GET"]["facet_size"]["max"] = 5

# *****************************************************************************
# Elasticsearch Query Pipeline
# *****************************************************************************
ALLOW_RANDOM_QUERY = True
ALLOW_NESTED_AGGS = True
USERQUERY_DIR = os.path.join(os.path.dirname(__file__), "userquery")

LICENSE_TRANSFORM = {"interpro": "pantherdb", "pantherdb.ortholog": "pantherdb"}  # For testing only.

# *****************************************************************************
# Endpoints Specifics
# *****************************************************************************
STATUS_CHECK = {
    "id": "1017",
    "index": "bts_test",
}

APP_LIST += [
    (r"/case/1", CustomCacheHandler, {"cache": 100}),
    (r"/case/2", CustomCacheHandler),
    (r"/case/3", DefautlAPIHandler),
]
