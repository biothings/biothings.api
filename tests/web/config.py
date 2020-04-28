"""
    Web settings to override for testing.
"""
import os

from biothings.web.settings.default import QUERY_GET_ESQB_KWARGS

# *****************************************************************************
# Elasticsearch Variables
# *****************************************************************************
ES_INDEX = 'bts_test'
ES_DOC_TYPE = 'gene'
ES_SCROLL_SIZE = 60

# *****************************************************************************
# User Input Control
# *****************************************************************************
QUERY_GET_ESQB_KWARGS = dict(QUERY_GET_ESQB_KWARGS)
QUERY_GET_ESQB_KWARGS.update({
    'facet_size': {'default': 3, 'type': int, 'max': 5},  # use a smaller size for testing
})

# *****************************************************************************
# Elasticsearch Query Builder
# *****************************************************************************
ALLOW_RANDOM_QUERY = True
ALLOW_NESTED_AGGS = True
USERQUERY_DIR = os.path.join(os.path.dirname(__file__), 'userquery')

# *****************************************************************************
# Endpoints Specifics
# *****************************************************************************
STATUS_CHECK = {
    'id': '1017',
    'index': 'bts_test',
    'doc_type': '_all'
}
