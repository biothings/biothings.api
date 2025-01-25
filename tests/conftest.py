""" 
Ref: https://docs.pytest.org/en/latest/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files

The conftest.py file serves as a means of providing fixtures for an entire directory.
Fixtures defined in a conftest.py can be used by any test in that package without needing to import them
(pytest will automatically discover them).
"""

import importlib
import os.path
import sys

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
config_mod.DATA_SRC_DATABASE = "data_src_database"
config_mod.DATA_TARGET_DATABASE = "testing_target.db"
config_mod.DATA_SRC_SERVER = "localhost"
config_mod.DATA_TARGET_SERVER = "testing_target.server"
config_mod.HUB_DB_BACKEND = {
    "module": "biothings.utils.sqlite3",
    "sqlite_db_folder": ".biothings_hub",
}
config_mod.hub_db = importlib.import_module(config_mod.HUB_DB_BACKEND["module"])

sys.modules["config"] = config_mod
sys.modules["biothings.config"] = config_mod
