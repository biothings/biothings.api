"""
    Web settings to override for testing.
"""
import os

from biothings.web.settings.default import QUERY_KWARGS

# *****************************************************************************
# Elasticsearch Variables
# *****************************************************************************
ES_INDEX = 'bts_test'
ES_DOC_TYPE = 'gene'
ES_SCROLL_SIZE = 60

# *****************************************************************************
# User Input Control
# *****************************************************************************
# use a smaller size for testing
QUERY_KWARGS['GET']['facet_size']['default'] = 3
QUERY_KWARGS['GET']['facet_size']['max'] = 5

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
}
