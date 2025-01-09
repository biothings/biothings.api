import json
from pathlib import Path

import jsonschema
import typer
from rich import box, print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from biothings.cli import utils

logger = utils.get_logger("biothings-cli")


help_text = "[green]Tools for understanding how to build a manifest for your dataplugin.[/green]"

manifest_application = typer.Typer(
    help=help_text,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


@manifest_application.command(
    name="schema",
    help="Display the biothings manifest schema contents",
    no_args_is_help=False,
)
def display_schema():
    """
    *schema* command

    Displays the schema contents storied in the biothings project
    """
    from biothings.hub.dataplugin.loaders.schema import load_manifest_schema

    manifest_schema = load_manifest_schema()
    schema_validator = jsonschema.validators.validator_for(manifest_schema)
    valid_schema = False
    try:
        schema_validator.check_schema(manifest_schema)
        valid_schema = True
    except jsonschema.exceptions.SchemaError as schema_error:
        logger.exception(schema_error)

    schema_repr = json.dumps(manifest_schema, indent=2)

    console = Console()
    panel = Panel(
        "[bold green]Schema Information[/bold green]\n"
        f"* [italic]Valid Schema[/italic]: {valid_schema}\n"
        f"* [italic]Schema Contents[/italic]:\n{schema_repr}",
        title="[bold green]Biothings[JSONSchema][/bold green]",
        subtitle="[bold green]Biothings[JSONSchema][/bold green]",
        box=box.ASCII,
    )
    console.print(panel)


@manifest_application.command(
    name="validate",
    help="Validates a provided manfiest file against the json schema",
    no_args_is_help=True,
)
def validate_manifest(manifest_file: str) -> None:
    """
    *validate* command

    Displays the schema contents storied in the biothings project
    """
    from biothings.hub.dataplugin.loaders.loader import ManifestBasedPluginLoader

    manifest_file = Path(manifest_file).resolve().absolute()
    plugin_name = "ManifestValidation"
    manifest_loader = ManifestBasedPluginLoader(plugin_name=plugin_name)
    with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
        manifest = json.load(manifest_handle)
        manifest_loader.validate_manifest(manifest)
