import importlib
import importlib.util
import logging
import pathlib
import sys

import typer

from biothings.utils.common import DummyConfig
from biothings.utils.configuration import ConfigurationError

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

cli = typer.Typer(
    rich_help_panel="Help and Others",
    rich_markup_mode="rich",
    context_settings=CONTEXT_SETTINGS,
    no_args_is_help=True,
)

logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)


@cli.callback()
def callback():
    """[green]Biothings Admin CLI.[/green]"""


def main():
    working_dir = pathlib.Path().resolve()
    _config = DummyConfig("config")
    _config.HUB_DB_BACKEND = {
        "module": "biothings.utils.sqlite3",
        "sqlite_db_folder": ".biothings_hub",
    }
    _config.DATA_SRC_DATABASE = ".data_src_database"
    _config.DATA_ARCHIVE_ROOT = ".biothings_hub/archive"
    _config.LOG_FOLDER = ".biothings_hub/logs"
    _config.DATA_PLUGIN_FOLDER = f"{working_dir}"
    try:
        config_mod = importlib.import_module("config")
        for attr in dir(config_mod):
            value = getattr(config_mod, attr)
            if isinstance(value, ConfigurationError):
                raise ConfigurationError("%s: %s" % (attr, str(value)))
            setattr(_config, attr, value)
    except Exception:
        logger.info(
            "The config.py does not exists in the working directory, use default biothings.config"
        )
    sys.modules["config"] = _config
    sys.modules["biothings.config"] = _config

    from .dataplugin import app as dataplugin_app
    from .dataplugin_localhub import app as dataplugin_localhub_app

    cli.add_typer(dataplugin_app, name="dataplugin")
    cli.add_typer(dataplugin_localhub_app, name="dataplugin-hub")
    return cli()
