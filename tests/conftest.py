"""
Ref: https://docs.pytest.org/en/latest/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files

The conftest.py file serves as a means of providing fixtures for an entire directory.
Fixtures defined in a conftest.py can be used by any test in that package without needing to import them
(pytest will automatically discover them).
"""

import importlib
import logging
import shutil
import sys
import types
from pathlib import Path

import pytest
import _pytest

from biothings.utils.common import DummyConfig
from biothings.utils.loggers import setup_default_log

logger = logging.getLogger(__name__)


class TestConfig(types.ModuleType):
    """
    More robust version of the `DummyConfig` type in the biothings backend

    Allows for loading as a system module, along with the ability to copy
    and mutate the module for test group specific behavior
    """

    def __init__(self, name: str, properties: dict, doc: str = None):
        super().__init__(name, doc)
        self.properties = properties
        self.overridden_properties = {}
        for attribute, value in self.properties.items():
            setattr(self, attribute, value)

    def override(self, properties: dict) -> None:
        """
        Adds additional properties on top of the root configuration

        It stores them for when we want to reset the module behavior
        to the original behavior

        When we update the override the dictionary it will write over any existing
        properties, both the original and potentially newly added ones
        """
        self.overridden_properties.update(properties)
        for attribute, value in self.overridden_properties.items():
            setattr(self, attribute, value)

    def reset(self) -> None:
        """
        Resets the state to the original property definition

        Eliminates all overriden properties and then re-adds them from the orginal
        properties
        """
        for attribute in self.overridden_properties.keys():
            delattr(self, attribute)

        self.overridden_properties = {}

        for attribute, value in self.properties.items():
            setattr(self, attribute, value)


@pytest.hookimpl()
def pytest_sessionstart(session: _pytest.main.Session):
    """
    Setup the default root configuration before any tests have been collected.
    We don't want to leverage a fixture as test collection occurs before fixture
    collection, so we want to ensure the configuration is established
    """
    root_mapping = {
        "DATA_SRC_SERVER": "localhost",
        "DATA_SRC_PORT": 27017,
        "DATA_SRC_DATABASE": "testhub_source",
        "DATA_SRC_SERVER_USERNAME": "",
        "DATA_SRC_SERVER_PASSWORD": "",
        "DATA_TARGET_SERVER": "localhost",
        "DATA_TARGET_PORT": 27017,
        "DATA_TARGET_DATABASE": "testhub_target",
        "DATA_TARGET_SERVER_USERNAME": "",
        "DATA_TARGET_SERVER_PASSWORD": "",
        "HUB_DB_BACKEND": {
            "module": "biothings.utils.es",
            "host": "http://localhost:9200",
        },
        "HUB_ENV": "",
        "ACTIVE_DATASOURCES": [],
        "DATA_HUB_DB_DATABASE": ".hubdb",
        "DATA_PLUGIN_FOLDER": "/tmp/testhub/plugins",
        "DATA_ARCHIVE_ROOT": "/tmp/testhub/datasources",
        "DIFF_PATH": "/tmp/testhub/datasources/diff",
        "RELEASE_PATH": "/tmp/testhub/datasources/release",
        "LOG_FOLDER": "/tmp/testhub/datasources/logs",
        "ES_HOST": "http://localhost:9200",
        "ES_INDICES": {"dev": "main_build_configuration"},
        "APITEST_PATH": str(Path(__file__).parent.absolute().resolve()),
        "ANNOTATION_DEFAULT_SCOPES": ["_id", "symbol"],
        "logger": setup_default_log("hub", "/tmp/testhub/datasources/logs"),
        "hub_db": importlib.import_module("biothings.utils.es"),
    }

    try:
        config_mod = TestConfig(name="root_config", properties=root_mapping, doc="Biothings SDK Root Configuration")
        sys.modules["config"] = config_mod
        sys.modules["biothings.config"] = config_mod
    except Exception:
        pytest.exit("Unexpected error while creating root test configuration")


@pytest.fixture(scope="session", autouse=True)
def root_configuration() -> DummyConfig:
    """
    Loads the root configuration from the system modules for override in
    lower-level classes
    """
    root_config = sys.modules.get("config", None)
    root_biothings_config = sys.modules.get("biothings.config", None)

    assert isinstance(root_config, TestConfig)
    assert isinstance(root_biothings_config, TestConfig)
    assert root_config == root_biothings_config

    yield root_config


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
    TEST_DATA_DIRECTORIES = ["tests/hub/dataplugin/data", "tests/hub/datainspect/schemas"]
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
