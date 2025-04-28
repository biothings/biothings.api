import importlib
import pathlib
import sys

import pytest


from biothings.utils.configuration import ConfigurationWrapper


@pytest.fixture(scope="module")
def configuration_data_storage() -> dict:
    test_directory = pathlib.Path(__file__).resolve().absolute().parent
    data_location = test_directory.joinpath("data")
    file_storage_mapping = {
        "base_configuration.py": data_location.joinpath("base_configuration.py"),
        "deep_configuration.py": data_location.joinpath("deep_configuration.py"),
    }
    return file_storage_mapping


@pytest.fixture(scope="module")
def base_config(configuration_data_storage: dict) -> ConfigurationWrapper:
    from biothings.hub import default_config
    import biothings.utils.hub_db

    base_configuration = str(configuration_data_storage["base_configuration.py"])
    configuration_spec = importlib.util.spec_from_file_location("base_configuration", base_configuration)
    base_configuration_module = importlib.util.module_from_spec(configuration_spec)
    configuration_spec.loader.exec_module(base_configuration_module)

    prior_config = sys.modules.get("config", None)
    prior_biothings_config = sys.modules.get("biothings.config", None)

    sys.modules["config"] = base_configuration_module
    sys.modules["biothings.config"] = base_configuration_module

    config_wrapper = ConfigurationWrapper(default_config, base_configuration_module)
    config_wrapper.hub_db = importlib.import_module(base_configuration_module.HUB_DB_BACKEND["module"])
    biothings.utils.hub_db.setup(config_wrapper)
    config_wrapper._db = biothings.utils.hub_db.get_hub_config()
    config_wrapper._get_db_function = biothings.utils.hub_db.get_hub_config
    yield config_wrapper

    sys.modules["config"] = prior_config
    sys.modules["biothings.config"] = prior_biothings_config


@pytest.fixture(scope="module")
def deep_config(configuration_data_storage: dict) -> ConfigurationWrapper:
    from biothings.hub import default_config
    import biothings.utils.hub_db

    deep_configuration = str(configuration_data_storage["deep_configuration.py"])
    configuration_spec = importlib.util.spec_from_file_location("deep_configuration", deep_configuration)
    deep_configuration_module = importlib.util.module_from_spec(configuration_spec)
    configuration_spec.loader.exec_module(deep_configuration_module)

    prior_config = sys.modules.get("config", None)
    prior_biothings_config = sys.modules.get("biothings.config", None)

    sys.modules["config"] = deep_configuration_module
    sys.modules["biothings.config"] = deep_configuration_module

    config_wrapper = ConfigurationWrapper(default_config, deep_configuration_module)
    config_wrapper.hub_db = importlib.import_module(deep_configuration_module.HUB_DB_BACKEND["module"])
    biothings.utils.hub_db.setup(config_wrapper)
    config_wrapper._db = biothings.utils.hub_db.get_hub_config()
    config_wrapper._get_db_function = biothings.utils.hub_db.get_hub_config
    yield config_wrapper

    sys.modules["config"] = prior_config
    sys.modules["biothings.config"] = prior_biothings_config
