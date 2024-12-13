from typing import Literal
import importlib
import importlib.util
import inspect
import logging
import os
import pathlib
import sys

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
import typer


from biothings.utils.common import DummyConfig
from biothings.utils.configuration import ConfigurationError


def setup_commandline_configuration(debug: bool, rich_traceback: bool) -> typer.Typer:
    """
    Sets up the typer command line tooling
    """
    pretty_exceptions_show_locals = False
    pretty_exceptions_enable = False
    sys.tracebacklimit = 1

    if rich_traceback:
        pretty_exceptions_enable = True
        sys.tracebacklimit = 1000

    if debug:
        pretty_exceptions_enable = True
        pretty_exceptions_show_locals = True
        sys.tracebacklimit = 1000

    # prevent dimming the help text from the 2nd line
    # see: https://github.com/tiangolo/typer/issues/437#issuecomment-1224149402
    typer.rich_utils.STYLE_HELPTEXT = ""

    context_settings = {"help_option_names": ["-h", "--help"]}
    typer_instance = typer.Typer(
        help="[green]BioThings Admin CLI to test your local data plugins. See helps for each command for specific usage.[/green]",
        rich_help_panel="Help and Others",
        rich_markup_mode="rich",
        context_settings=context_settings,
        no_args_is_help=True,
        pretty_exceptions_show_locals=pretty_exceptions_show_locals,
        pretty_exceptions_enable=pretty_exceptions_enable,
    )

    return typer_instance


def setup_logging_configuration(logging_level: Literal[10, 20, 30, 40, 50]) -> None:
    """
    Configures the logging based off our environment configuration
    """
    rich_handler = RichHandler(
        level=logging_level,
        markup=True,
        rich_tracebacks=False,  # typer creates it already
        show_path=False,
        tracebacks_suppress=[typer],
    )
    logging.basicConfig(level=logging_level, format="%(message)s", datefmt="[%X]", handlers=[rich_handler])


def setup_biothings_configuration():
    """
    Setup a config module necessary to launch the CLI

    Depending on the backend hub database, the order of configuration
    matters. If we attempt to load a module that checks for the configuration
    we'll have to ensure that the configuration is properly configured prior
    to loading the module
    """
    working_dir = pathlib.Path().resolve()
    configuration_instance = DummyConfig("config")

    try:
        config_mod = importlib.import_module("config")
        for attr in dir(config_mod):
            value = getattr(config_mod, attr)
            if isinstance(value, ConfigurationError):
                raise ConfigurationError(f"{attr}: {value}")
            setattr(configuration_instance, attr, value)
    except ModuleNotFoundError:
        logging.debug(ModuleNotFoundError)
        logging.debug("Unable to find `config` module. Using the default configuration")
    finally:
        sys.modules["config"] = configuration_instance
        sys.modules["biothings.config"] = configuration_instance

    configuration_instance.HUB_DB_BACKEND = {
        "module": "biothings.utils.sqlite3",
        "sqlite_db_folder": ".biothings_hub",
    }
    configuration_instance.DATA_SRC_SERVER = "localhost"
    configuration_instance.DATA_SRC_DATABASE = "data_src_database"
    configuration_instance.DATA_ARCHIVE_ROOT = ".biothings_hub/archive"
    configuration_instance.LOG_FOLDER = ".biothings_hub/logs"
    configuration_instance.DATA_PLUGIN_FOLDER = f"{working_dir}"

    try:
        configuration_instance.hub_db = importlib.import_module(configuration_instance.HUB_DB_BACKEND["module"])
    except ImportError as import_err:
        logging.exception(import_err)
        raise import_err
    return configuration_instance

    configuration_member_collection = [
        f"[green]{configuration_key}:[/green][bold] [{configuration_value}[/bold]\n"
        for configuration_key, configuration_value in inspect.getmembers(configuration_instance)
        if configuration_value is not None
    ]
    configuration_debug_message = "[green]<biothings command-line configuration>[/green][bold]\n" "".join(
        configuration_member_collection
    )
    console = Console()
    console.print(
        Panel(
            configuration_debug_message,
            title="[bold]Dump[/bold]",
            title_align="left",
        )
    )
