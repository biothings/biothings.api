import typer

from .dataplugin import app as dp_appp

cli = typer.Typer()
cli.add_typer(dp_appp, name="dataplugin")
