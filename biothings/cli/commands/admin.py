"""
Configuration settings for the biothings-cli tool

> Logging
> Tool Configuration
    > Creates a mock config used in the biothings.api backend
"""

import sys

import typer
import typer.rich_utils


def build_admin_application(debug: bool, rich_traceback: bool) -> typer.Typer:
    """
    Builds the main administrative command line application for the
    biothings-cli application
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
