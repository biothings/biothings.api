import sys

import typer

cli = typer.Typer()


def main():
    module = sys.argv[1]
    if module == "standalone-dataplugin":
        from .standalone import app as standalone_app

        cli.add_typer(standalone_app, name="standalone-dataplugin")
        return cli()
    else:
        from .dataplugin import app as dp_app

        cli.add_typer(dp_app, name="dataplugin")
        return cli()
