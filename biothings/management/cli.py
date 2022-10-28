import logging
import sys

import typer

cli = typer.Typer()


@cli.callback()
def callback():
    """
    Biothing Admin CLI app.
    """


def main():
    module = ""
    if len(sys.argv) > 1:
        module = sys.argv[1]
    if module == "standalone-dataplugin":
        from .standalone import app as standalone_app

        cli.add_typer(standalone_app, name="standalone-dataplugin")
        return cli()
    elif module == "dataplugin":
        try:
            from .dataplugin import app as dp_app
        except Exception as ex:
            if "No module named 'config'" in str(ex):
                logging.error(
                    "This mode is require "
                    "You have to create the config.py in order to run this command"
                )
                return
            else:
                logging.exception(ex, exc_info=True)
                return
        cli.add_typer(dp_app, name="dataplugin")
        return cli()
    return cli()
