# pylint: disable=unexpected-keyword-arg
"""
Manage sample dataset and mapping for testing

- Run this file directly to (re)generate sample files.
- Define pytest session level fixture.

"""
import copy
import pathlib
import sys

import pytest

from biothings.utils.common import DummyConfig
from biothings.web.settings.default import APP_LIST, QUERY_KWARGS
from biothings.web.handlers import BaseAPIHandler


TEST_INDEX = "bts_test"
TEST_DOC_TYPE = "gene"
TEST_HOST = "http://localhost:9200"


class CustomCacheHandler(BaseAPIHandler):
    cache = 999

    async def get(self):
        res = {
            "success": True,
            "status": "yellow",
        }
        self.finish(res)


class DefautlAPIHandler(BaseAPIHandler):
    async def get(self):
        res = {
            "success": True,
            "status": "yellow",
        }
        self.finish(res)


@pytest.fixture(scope="module")
def userquery() -> pathlib.Path:
    """
    Generates the userquery data directory required for testing
    """
    data_directory = pathlib.Path(__file__).parent.joinpath("data")
    userquery_directory = data_directory.joinpath("userquery")
    assert userquery_directory.exists()

    filter_data = userquery_directory / "exrna/filter.txt"
    assert filter_data.exists()

    query_data = userquery_directory / "prefix/query.txt"
    assert query_data.exists()

    return userquery_directory


@pytest.fixture(scope="module", autouse=True)
def handler_configuration(userquery: pathlib.Path):

    config_mod = DummyConfig(name="config")

    # *****************************************************************************
    # Elasticsearch Variables
    # *****************************************************************************
    config_mod.ES_INDICES = {TEST_DOC_TYPE: TEST_INDEX}
    config_mod.ES_DOC_TYPE = TEST_DOC_TYPE
    config_mod.ES_SCROLL_SIZE = 60

    # *****************************************************************************
    # User Input Control
    # *****************************************************************************
    # use a smaller size for testing
    TEST_HANDLER_QUERY_KWARGS = copy.deepcopy(QUERY_KWARGS)
    TEST_HANDLER_QUERY_KWARGS["GET"]["facet_size"]["default"] = 3
    TEST_HANDLER_QUERY_KWARGS["GET"]["facet_size"]["max"] = 5
    config_mod.QUERY_KWARGS = TEST_HANDLER_QUERY_KWARGS

    # *****************************************************************************
    # Elasticsearch Query Pipeline
    # *****************************************************************************
    config_mod.ALLOW_RANDOM_QUERY = True
    config_mod.ALLOW_NESTED_AGGS = True

    config_mod.USERQUERY_DIR = userquery_directory
    config_mod.LICENSE_TRANSFORM = {"interpro": "pantherdb", "pantherdb.ortholog": "pantherdb"}  # For testing only.

    # *****************************************************************************
    # Endpoints Specifics
    # *****************************************************************************
    config_mod.STATUS_CHECK = {
        "id": "1017",
        "index": "bts_test",
    }

    TEST_HANDLER_APP_LIST = [
        *APP_LIST,
        (r"/case/1", CustomCacheHandler, {"cache": 100}),
        (r"/case/2", CustomCacheHandler),
        (r"/case/3", DefautlAPIHandler),
    ]

    config_mod.APP_LIST = TEST_HANDLER_APP_LIST

    prior_config = sys.modules.get("config", None)
    prior_biothings_config = sys.modules.get("biothings.config", None)

    sys.modules["config"] = config_mod
    sys.modules["biothings.config"] = config_mod
    yield config_mod
    sys.modules["config"] = prior_config
    sys.modules["biothings.config"] = prior_biothings_config
