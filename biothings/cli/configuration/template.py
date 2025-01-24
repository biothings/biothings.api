def setup_config():
    """Setup a config module necessary to launch the CLI"""
    working_dir = pathlib.Path().resolve()
    _config = DummyConfig("config")
    _config.HUB_DB_BACKEND = {
        "module": "biothings.utils.sqlite3",
        "sqlite_db_folder": ".biothings_hub",
    }
    _config.DATA_SRC_DATABASE = ".data_src_database"
    _config.DATA_ARCHIVE_ROOT = ".biothings_hub/archive"
    # _config.LOG_FOLDER = ".biothings_hub/logs"
    _config.LOG_FOLDER = None  # disable file logging, only log to stdout
    _config.DATA_PLUGIN_FOLDER = f"{working_dir}"
    _config.hub_db = importlib.import_module(_config.HUB_DB_BACKEND["module"])
    try:
        config_mod = importlib.import_module("config")
        for attr in dir(config_mod):
            value = getattr(config_mod, attr)
            if isinstance(value, ConfigurationError):
                raise ConfigurationError("%s: %s" % (attr, str(value)))
            setattr(_config, attr, value)
    except ModuleNotFoundError:
        logger.debug("The config.py does not exists in the working directory, use default biothings.config")

    sys.modules["config"] = _config
    sys.modules["biothings.config"] = _config
