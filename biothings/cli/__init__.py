"""
Entrypoint for the biothings-cli tool
"""

from typing import Literal
import importlib.util
import logging
import os
import sys

import typer
from rich.logging import RichHandler

from biothings.cli.commands.admin import build_admin_application
from biothings.cli.commands.config import config_application, load_configuration
from biothings.cli.commands.dataplugin import dataplugin_application


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


def main():
    """
    The entrypoint for running the BioThings CLI
    """
    module_specification = importlib.util.find_spec("typer")
    typer_status = module_specification is not None
    if not typer_status:
        logging.error(
            (
                "`typer` package is required for biothings-cli package. "
                "Use `pip install biothings[cli]` or `pip install typer[all]` to install."
            )
        )
        sys.exit(-1)

    # Typer already supports an environment variable to disable rich tracebacks
    # >>> TYPER_STANDARD_TRACEBACK=1
    # This supports a similar
    # >>> BTCLI_RICH_TRACEBACK=1 env variable to turn it on when default is off in our case
    # Relevant ref: https://github.com/tiangolo/typer/issues/525 and https://github.com/tiangolo/typer/discussions/612
    # and BTCLI_DEBUG=1 env variable to turn on both rich tracebacks and show locals
    cli_debug_flag = os.environ.get("BTCLI_DEBUG", False)
    cli_rich_traceback_flag = os.environ.get("BTCLI_RICH_TRACEBACK", False)

    admin_application = build_admin_application(debug=cli_debug_flag, rich_traceback=cli_rich_traceback_flag)
    logging_level = logging.WARNING
    if cli_debug_flag:
        logging_level = logging.DEBUG
    setup_logging_configuration(logging_level)
    load_configuration()

    admin_application.add_typer(dataplugin_application, name="dataplugin")
    admin_application.add_typer(config_application, name="config")
    return admin_application()
