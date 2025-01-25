"""
Commands for helping develop / debug manifest file creation
"""

import logging
import json
from pathlib import Path

import jsonschema
import typer
from rich import box
from rich.console import Console
from rich.panel import Panel


logger = logging.getLogger("biothings-cli")


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
        f"* [bold green]Valid Schema[/bold green]: {valid_schema}\n"
        f"* [bold green]Schema Contents[/bold green]:\n{schema_repr}",
        title="[bold green]Biothings JSONSchema Information[/bold green]",
        subtitle="[bold green]Biothings JSONSchema Information[/bold green]",
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
    plugin_name = manifest_file.parent.name
    manifest_loader = ManifestBasedPluginLoader(plugin_name=plugin_name)

    manifest_state = {"path": manifest_file, "valid": False, "repr": None, "error": None}

    try:
        with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
            manifest = json.load(manifest_handle)
    except json.JSONDecodeError as decode_error:
        logger.exception(decode_error)
        manifest_state["error"] = f"{manifest_file} is not valid JSON"

    manifest_state["repr"] = json.dumps(manifest, indent=2)

    try:
        manifest_loader.validate_manifest(manifest)
    except Exception as gen_exc:
        logger.exception(gen_exc)
        manifest_state["error"] = f"{manifest_file} doesn't conform to the schema"
    else:
        manifest_state["valid"] = True

    console = Console()
    panel_message = (
        f"* [bold green]Plugin Name[/bold green]: {plugin_name}\n"
        f"* [bold green]Manifest Path[/bold green]: {manifest_state['path']}\n"
        f"* [bold green]Valid Manifest[/bold green]: {manifest_state['valid']}\n"
    )

    if manifest_state["error"] is not None:
        panel_message += f"* [bold green]Manifest Error[/bold green]: {manifest_state['error']}\n"
    elif manifest_state["error"] is None and manifest_state["repr"] is not None:
        panel_message += f"* [bold green]Manifest Contents[/bold green]:\n{manifest_state['repr']}\n"

    panel = Panel(
        renderable=panel_message,
        title="[bold green]Biothings Manifest Validation[/bold green]",
        subtitle="[bold green]Biothings Manifest Validation[/bold green]",
        box=box.ASCII,
    )
    console.print(panel)
