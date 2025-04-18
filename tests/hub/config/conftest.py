import importlib
import sys

import pytest


from biothings.utils.common import DummyConfig
from biothings.utils.configuration import ConfigurationWrapper


@pytest.fixture(scope="session")
def base_config(root_mapping: dict):
    base_config_mod = DummyConfig(name="base_eval_config")

    for attribute, value in root_mapping.items():
        setattr(base_config_mod, attribute, value)

    base_config_mod.HUB_DB_BACKEND = {"module": "biothings.utils.sqlite3", "sqlite_db_folder": "."}
    base_config_mod.DIFF_PATH = ""
    base_config_mod.RELEASE_PATH = ""
    base_config_mod.S3_SNAPSHOT_BUCKET = ""
    base_config_mod.S3_REGION = ""
    base_config_mod.DATA_HUB_DB_DATABASE = ".hubdb"

    # descONE
    base_config_mod.ONE = 1

    # * section alpha *#
    base_config_mod.B = "B"

    base_config_mod.C = "C"  # ends with space should be stripped descC

    # not a param, not uppercase
    base_config_mod.Two = 2

    # * section beta *#
    # descD_D
    base_config_mod.D_D = "D"

    # * section gamma *#

    # descE.
    base_config_mod.E = "E"

    # * section beta *#

    # descF.
    # back to beta section.
    base_config_mod.F = "F"

    # * *#
    # reset section
    base_config_mod.G = "G"

    # this is a secret param
    # - invisible -#
    base_config_mod.INVISIBLE = "hollowman"

    # hide the value, not the param
    # - hide -#
    base_config_mod.PASSWORD = "1234"

    # it's readonly
    # - readonly -#
    base_config_mod.READ_ONLY = "written in stone"

    # it's read-only and value is hidden, not the param
    # - readonly -#
    # - hide -#
    base_config_mod.RUN_DIR = "can't read the stone"

    # invisible has full power
    # read-only is not necessary anyways
    # - readonly
    # - invisible -#
    base_config_mod.INVISIBLE_READ_ONLY = "evaporated"

    # special param, by default config is read-only
    # but we want to test modification
    base_config_mod.CONFIG_READONLY = False

    base_config_mod.LOG_FOLDER = "/tmp/testhub/datasources/logs"

    base_config_mod.HUB_SSH_PORT = "123"

    prior_config = sys.modules.get("config", None)
    prior_biothings_config = sys.modules.get("biothings.config", None)

    sys.modules["config"] = base_config_mod
    sys.modules["biothings.config"] = base_config_mod

    from biothings.hub import default_config

    config_wrapper = ConfigurationWrapper(default_config, base_config_mod)

    import biothings.utils.hub_db

    config_wrapper.hub_db = importlib.import_module(base_config_mod.HUB_DB_BACKEND["module"])
    biothings.utils.hub_db.setup(config_wrapper)
    config_wrapper._db = biothings.utils.hub_db.get_hub_config()
    config_wrapper._get_db_function = biothings.utils.hub_db.get_hub_config
    # wrapper.APP_PATH = app_path
    yield config_wrapper

    sys.modules["config"] = prior_config
    sys.modules["biothings.config"] = prior_biothings_config


@pytest.fixture(scope="session")
def deep_config(root_mapping):
    deep_config_mod = DummyConfig(name="deep_eval_config")

    for attribute, value in root_mapping.items():
        setattr(deep_config_mod, attribute, value)

    # redefine some params

    # additional desc of read-only
    deep_config_mod.READ_ONLY = "written in titanium"

    # * run_dir section *#
    # - readonly -#
    # - hide -#
    # run_dir desc
    deep_config_mod.HUB_DB_BACKEND = {"module": "biothings.utils.sqlite3", "sqlite_db_folder": "."}
    deep_config_mod.DIFF_PATH = ""
    deep_config_mod.RELEASE_PATH = ""
    deep_config_mod.S3_SNAPSHOT_BUCKET = ""
    deep_config_mod.S3_REGION = ""
    deep_config_mod.DATA_HUB_DB_DATABASE = ".hubdb"

    # descONE
    deep_config_mod.ONE = 1

    # * section alpha *#
    deep_config_mod.B = "B"

    deep_config_mod.C = "C"  # ends with space should be stripped descC

    # not a param, not uppercase
    deep_config_mod.Two = 2

    # * section beta *#
    # descD_D
    deep_config_mod.D_D = "d"

    # * section gamma *#

    # additional description
    deep_config_mod.E = "heu"

    # * section beta *#

    # descF.
    # back to beta section.
    deep_config_mod.F = "F"

    # * *#
    # reset section
    deep_config_mod.G = "G"

    # this is a secret param
    # - invisible -#
    deep_config_mod.INVISIBLE = "hollowman"

    # hide the value, not the param
    # - hide -#
    deep_config_mod.PASSWORD = "1234"

    # it's readonly
    # - readonly -#
    deep_config_mod.READ_ONLY = "written in stone"

    # it's read-only and value is hidden, not the param
    # - readonly -#
    # - hide -#
    deep_config_mod.RUN_DIR = "run_dir"

    # invisible has full power
    # read-only is not necessary anyways
    # - readonly
    # - invisible -#
    deep_config_mod.INVISIBLE_READ_ONLY = "evaporated"

    # special param, by default config is read-only
    # but we want to test modification
    deep_config_mod.CONFIG_READONLY = False

    deep_config_mod.LOG_FOLDER = "/tmp/testhub/datasources/logs"

    deep_config_mod.HUB_SSH_PORT = "123"

    prior_config = sys.modules.get("config", None)
    prior_biothings_config = sys.modules.get("biothings.config", None)

    sys.modules["config"] = deep_config_mod
    sys.modules["biothings.config"] = deep_config_mod
    yield deep_config_mod
    sys.modules["config"] = prior_config
    sys.modules["biothings.config"] = prior_biothings_config
