import importlib.util
import logging
import os
import types
from collections.abc import Collection
from importlib import import_module
from typing import NamedTuple

from . import default, validators

logger = logging.getLogger(__name__)


def load(config="config"):
    config = load_module(config)
    if config.__name__ == config.__package__:
        attrs = [getattr(config, attr) for attr in dir(config)]
        confs = [attr for attr in attrs if isinstance(attr, types.ModuleType)]
        valis = (
            validators.WebAPIValidator(),
            validators.DBParamValidator(),
            validators.SubmoduleValidator(),
        )
        return ConfigPackage(
            ConfigModule(config),
            [ConfigModule(conf, config, valis) for conf in confs],
        )
    else:  # config is a single file module, not a package
        return ConfigModule(
            config,
            validators=(
                validators.WebAPIValidator(),
                validators.DBParamValidator(),
            ),
        )


def load_module(config, default=None):
    """
    Load a config module.

    config:
        1. a module object
        2. a fully qualified module name
        3. a file path to a module
    """
    if isinstance(config, types.ModuleType):
        return config
    elif isinstance(config, str) and config.endswith(".py"):
        spec = importlib.util.spec_from_file_location("config", config)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        return config
    elif isinstance(config, str) and config:
        return import_module(config)
    elif not config and default:
        return default
    raise ValueError(str(config))


# Opinion on item access []
# The ConfigModule is intended to present a interface similar to
# types.ModuleType or types.SimpleNamespace, and hold primarily
# known attributes that persisit throughout the application. For
# those reasons, item-access interface is not supported.


class ConfigPackage(NamedTuple):
    root: object
    modules: Collection


class ConfigModule:
    """
    A wrapper for the settings that configure the web API.

    * Environment variables can override settings of the same names.
    * Default values are defined in biothings.web.settings.default.

    """

    def __init__(self, config=None, parent=None, validators=(), **kwargs):
        self._fallback = parent  # config package
        self._primary = config
        self._override = types.SimpleNamespace()

        logger.info("%s", self._primary)  # log file location

        # process keyword setting override
        for key, value in kwargs.items():
            setattr(self._override, key, value)

        # process environment variable override of named settings
        for name in os.environ:
            if hasattr(self, name):
                new_value = None
                if isinstance(getattr(self, name), str):
                    new_value = os.environ[name]
                elif isinstance(getattr(self, name), bool):
                    new_value = os.environ[name].lower() in ("true", "1")
                if new_value is not None:
                    logger.info("$ %s = %s", name, os.environ[name])
                    setattr(self._override, name, new_value)
                else:  # cannot override dict, array, object type...
                    logger.error("Env %s is not suppored.", name)

        for validator in validators:
            validator.validate(self)

    def __getattr__(self, name):
        # transient settings like envs
        if hasattr(self._override, name):
            return getattr(self._override, name)
        # user specified config module
        elif hasattr(self._primary, name):
            return getattr(self._primary, name)
        # shared settings in a config package
        elif hasattr(self._fallback, name):
            return getattr(self._fallback, name)
        # global default settings
        elif hasattr(default, name):
            return getattr(default, name)
        else:  # not provided and no default
            raise AttributeError(name)
