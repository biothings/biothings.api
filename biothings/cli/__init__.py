"""
Entrypoint for the biothings-cli tool
"""

from typing import Literal
import importlib.util
import logging
import os
import sys


def setup_logging_configuration(logging_level: Literal[10, 20, 30, 40, 50]) -> None:
    """
    Configures the logging based off our environment configuration
    """
    import typer
    from rich.logging import RichHandler

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
    # the CLI's packages are optional (biothings[cli]): only imported once they're known to be installed
    missing = [package for package in ("typer", "rich") if importlib.util.find_spec(package) is None]
    if missing:
        sys.exit("The biothings-cli requires %s: pip install 'biothings[cli]'" % " and ".join(missing))
    # pylint: disable=import-outside-toplevel
    from biothings.cli.commands.admin import build_admin_application
    from biothings.cli.commands.config import config_application, load_configuration
    from biothings.cli.commands.dataplugin import dataplugin_application
    from biothings.cli.commands.hub import hub_application
    from biothings.cli.commands.pathing import path_application

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
    # "hub" commands talk to a running hub through its API and don't need a local configuration,
    # which could fail to load when run from a hub's folder (its config.py is the hub's own)
    if sys.argv[1:2] != ["hub"]:
        load_configuration()

    admin_application.add_typer(dataplugin_application, name="dataplugin")
    admin_application.add_typer(config_application, name="config")
    admin_application.add_typer(path_application, name="path")
    admin_application.add_typer(hub_application, name="hub")
    return admin_application()
