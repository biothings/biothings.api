import logging
import sys

import typer

cli = typer.Typer()


@cli.callback()
def callback():
    """
    Biothing Admin CLI app. Available subcommands:

    biothings-admin dataplugin --help
    biothings-admin dataplugin-localhub --help

    """


def main():
    module = ""
    if len(sys.argv) > 1:
        module = sys.argv[1]
    if module == "dataplugin":
        from .dataplugin import app as standalone_app

        cli.add_typer(standalone_app, name="dataplugin")
        return cli()
    elif module == "dataplugin-localhub":
        try:
            from .dataplugin_localhub import app as dp_app
        except Exception as ex:
            if "No module named 'config'" in str(ex):
                logging.error("You have to create the config.py in order to run this command")
                return
            else:
                logging.exception(ex, exc_info=True)
                return
        cli.add_typer(dp_app, name="dataplugin-localhub")
        return cli()
    return cli()
