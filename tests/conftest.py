""" 
Ref: https://docs.pytest.org/en/latest/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files

The conftest.py file serves as a means of providing fixtures for an entire directory.
Fixtures defined in a conftest.py can be used by any test in that package without needing to import them
(pytest will automatically discover them).
"""

import logging
import os.path
import shutil
import sys
from pathlib import Path

import pytest

from biothings.utils.common import DummyConfig

config_mod = DummyConfig(name="config")
config_mod.HUB_DB_BACKEND = {
    "module": "biothings.utils.es",
    "host": "http://localhost:9200",
}
config_mod.DATA_ARCHIVE_ROOT = "/tmp/testhub/datasources"
config_mod.ES_HOST = "http://localhost:9200"  # optional
config_mod.ES_INDICES = {"dev": "main_build_configuration"}
config_mod.ANNOTATION_DEFAULT_SCOPES = ["_id", "symbol"]
config_mod.LOG_FOLDER = os.path.join(config_mod.DATA_ARCHIVE_ROOT, "logs")

sys.modules["config"] = config_mod
sys.modules["biothings.config"] = config_mod


TEST_DATA_DIRECTORIES = ["tests/hub/dataplugin/data", "tests/hub/datainspect/schemas"]


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def temporary_data_storage(tmp_path_factory, request) -> Path:
    """
    Builds a session level test structure to avoid potentially modifying
    repository test data

    > Takes the session level items discovered as tests and iterates over
      them
    > Each test item will have the following:
        > test_location
        > test_node
        > test_name
    > We search for a potential hardcoded data directory relative to the
      discovered test to add to our collection of test data directories
    > Takes these discovered directories and copies all the corresponding
      test data into a temporary location for usage during the tests
    > Fixture yields the temporary directory structure to each test that
      calls it so the test can utilize any potential data it requires
      without modifying the stored test data within the repository
    > Cleans up the temporary directory after the test session has completed
    """
    for test_function in request.session.items:
        test_location, test_node, test_name = test_function.location
        logger.info(f"Discovered {test_name}@{test_location} given node #{test_node}")

    module_root_path = request.config.rootpath
    test_data_locations = [module_root_path / test_data_directory for test_data_directory in TEST_DATA_DIRECTORIES]

    temp_directory_name = "state_alchemist"
    temp_directory = tmp_path_factory.mktemp(temp_directory_name)
    for data_directory in test_data_locations:
        if data_directory.is_dir():
            shutil.copytree(
                src=str(data_directory),
                dst=str(temp_directory),
                dirs_exist_ok=True,
            )
            logger.info(f"Copied {data_directory} -> {temp_directory}")

    yield temp_directory
    shutil.rmtree(str(temp_directory))
