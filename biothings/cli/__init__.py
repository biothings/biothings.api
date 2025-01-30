"""
Entrypoint for the biothings-cli tool
"""

import importlib.util
import logging
import os
import sys


from biothings.cli.settings import (
    setup_biothings_configuration,
    setup_commandline_configuration,
    setup_logging_configuration,
)


def check_module_import_status(module: str) -> bool:
    """
    Verify that we can import a module prior to proceeding with creating our commandline
    tooling that depends on those modules
    """
    module_specification = importlib.util.find_spec(module)
    status = module_specification is not None
    return status


def main():
    """
    The entrypoint for running the BioThings CLI to test your local data plugin
    """
    typer_status = check_module_import_status("typer")
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

    cli = setup_commandline_configuration(debug=cli_debug_flag, rich_traceback=cli_rich_traceback_flag)
    logging_level = logging.INFO
    if cli_debug_flag:
        logging_level = logging.DEBUG
    setup_logging_configuration(logging_level)
    setup_biothings_configuration()

    from biothings.cli.dataplugin import dataplugin_application
    from biothings.cli.hub import hub_application

    cli.add_typer(dataplugin_application, name="dataplugin")
    cli.add_typer(hub_application, name="dataplugin-hub")
    return cli()
