import importlib.util
import textwrap

import pytest

from biothings.hub import default_config
from biothings.utils.configuration import ConfigurationWrapper


class FakeHubConfig:
    """hub_config collection of the hub db, in memory"""

    def __init__(self):
        self.docs = {}

    def find_one(self, query):
        return self.docs.get(query["_id"])

    def find(self, query=None):
        return list(self.docs.values())

    def count(self):
        return len(self.docs)

    def update_one(self, query, what, upsert=False):
        self.docs.setdefault(query["_id"], {"_id": query["_id"]}).update(what["$set"])

    def remove(self, query):
        for _id in [_id for _id in self.docs if query.get("_id", _id) == _id]:
            del self.docs[_id]
        return object()  # eg. a pymongo DeleteResult, not JSON-serializable


@pytest.fixture
def config(tmp_path):
    path = tmp_path / "config_for_tests.py"
    path.write_text(textwrap.dedent("""
            import logging
            logger = logging.getLogger("test_configuration")
            DATA_ARCHIVE_ROOT = %r
            DATA_SRC_SERVER = DATA_TARGET_SERVER = "localhost"
            DATA_SRC_DATABASE, DATA_TARGET_DATABASE = "test_src", "test"
            HUB_DB_BACKEND = {"module": "biothings.utils.sqlite3"}
            S3_SNAPSHOT_BUCKET = S3_REGION = ""
            CONFIG_READONLY = False
            HUB_MAX_WORKERS = 2
            """ % str(tmp_path)))
    spec = importlib.util.spec_from_file_location("config_for_tests", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    wrapper = ConfigurationWrapper(default_config, module)
    wrapper._db = FakeHubConfig()
    wrapper._get_db_function = lambda: wrapper._db
    return wrapper


def test_show_one_parameter(config):
    assert config.show("HUB_MAX_WORKERS")["value"] == 2
    assert "HUB_MAX_WORKERS" in config.show()["scope"]["config"]
    with pytest.raises(ValueError, match="No configuration parameter named 'NOPE'"):
        config.show("NOPE")


def test_reset_parameters(config):
    config.store_value_to_db("HUB_MAX_WORKERS", 8)
    assert config.show("HUB_MAX_WORKERS")["value"] == 8
    assert config.reset("HUB_MAX_WORKERS") is True
    assert config.show("HUB_MAX_WORKERS")["value"] == 2
    config.store_value_to_db("HUB_MAX_WORKERS", 8)
    config.store_value_to_db("HUB_NAME", "renamed")
    assert config.reset() is True  # all of them
    assert config._db.docs == {}
