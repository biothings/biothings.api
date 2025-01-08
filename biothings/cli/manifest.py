import json

import typer

from biothings.cli import utils
from biothings.hub.dataplugin.loaders.loader import ManifestBasedPluginLoader
from biothings.hub.dataplugin.loads.schema import load_manifest_schema

logger = utils.get_logger("biothings-cli")


help_text = "[green]Tools for understanding how to build a manifest for your dataplugin.[/green]"

app = typer.Typer(
    help=help_text,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


@app.command(
    name="schema",
    help="Display the biothings manifest schema contents",
    no_args_is_help=False,
)
def display_schema():
    """
    *schema* command

    Displays the schema contents storied in the biothings project
    """
    manifest_schema = load_manifest_schema()
    breakpoint()


@app.command(
    name="validate",
    help="Validates a provided manfiest file against the json schema",
    no_args_is_help=True,
)
def validate_manifest(manifest_file: Union[str, Path]) -> None:
    """
    *validate* command

    Displays the schema contents storied in the biothings project
    """
    plugin_name = "ManifestValidation"
    manifest_loader = ManifestBasedPluginLoader(plugin_name=plugin_name)
    with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
        manifest = json.load(manifest_handle)
        manifest_loader.validate_manifest(manifest)
