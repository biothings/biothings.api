import importlib
import importlib.util
import logging
import os
import pathlib
import sys

try:
    import typer
    from rich.logging import RichHandler

    typer_avail = True
except ImportError:
    typer_avail = False

from biothings.utils.common import DummyConfig
from biothings.utils.configuration import ConfigurationError

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

if typer_avail:
    # prevent dimming the help text from the 2nd line
    # see: https://github.com/tiangolo/typer/issues/437#issuecomment-1224149402
    typer.rich_utils.STYLE_HELPTEXT = ""

    # Typer already support an env variable to disable rich tracebacks using _TYPER_STANDARD_TRACEBACK=1
    # This supports a similar BTCLI_RICH_TRACEBACK=1 env variable to turn it on when default is off in our case
    # Relevant ref: https://github.com/tiangolo/typer/issues/525 and https://github.com/tiangolo/typer/discussions/612
    # and BTCLI_DEBUG=1 env varible to turn on both rich tracebacks and show locals
    if os.environ.get("BTCLI_DEBUG"):
        pretty_exceptions_enable = True
        pretty_exceptions_show_locals = True
        default_logging_level = logging.DEBUG
    else:
        if os.environ.get("BTCLI_RICH_TRACEBACK"):
            pretty_exceptions_enable = True
        else:
            pretty_exceptions_enable = False
            sys.tracebacklimit = 1  # only show the last traceback, default is 1000
        pretty_exceptions_show_locals = False
        default_logging_level = logging.INFO

    cli = typer.Typer(
        help="[green]BioThings Admin CLI to test your local data plugins. See helps for each command for specific usage.[/green]",
        rich_help_panel="Help and Others",
        rich_markup_mode="rich",
        context_settings=CONTEXT_SETTINGS,
        no_args_is_help=True,
        pretty_exceptions_show_locals=pretty_exceptions_show_locals,
        pretty_exceptions_enable=pretty_exceptions_enable,
    )

    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            # we don't need to turn on rich_tracebacks since typer creates it already
            RichHandler(
                level=default_logging_level,
                rich_tracebacks=False,
                tracebacks_suppress=[typer],
                show_path=False,
            ),
        ],
    )
    logger = logging.getLogger("cli")


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


def main():
    """The main entry point for running the BioThings CLI to test your local data plugins."""
    if not typer_avail:
        logger.error('"typer" package is required for CLI feature. Use "pip install typer[all]" to install.')
        return

    setup_config()
    from .dataplugin import app as dataplugin_app
    from .dataplugin_hub import app as dataplugin_hub_app

    cli.add_typer(dataplugin_app, name="dataplugin")
    cli.add_typer(dataplugin_hub_app, name="dataplugin-hub")
    return cli()
