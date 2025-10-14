"""
Module for creating the cli interface for the path interface
"""

import asyncio
import pathlib
import sys
from typing import Optional

import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table

from biothings.cli.commands import operations

SHORT_HELP = "[green]CLI tool for viewing the python system path and adding external parsers to the system path[/green]"
FULL_HELP = (
    SHORT_HELP
    + "\n\n[magenta]   :sparkles: Run from an existing data plugin folder to evaluate a singular data plugin.[/magenta]"
)
path_application = typer.Typer(
    help=FULL_HELP,
    short_help=SHORT_HELP,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


@path_application.command(name="view")
def view_system_path() -> None:
    """
    View the system paths current discovered by python, along with potential hub parsers of interest
    that the user may wish to add to the system path for usage in data plugin testing
    """
    path_table = Table(title="Python System Path(s)")

    path_table.add_column("Index", style="cyan")
    path_table.add_column("Paths", style="green")

    system_paths = sys.path
    for index, system_path in enumerate(system_paths):
        path_table.add_row(str(index), str(system_path))

    parser_table = Table(title="External Parser Path(s)")

    parser_table.add_column("Index", style="cyan")
    parser_table.add_column("Paths", style="magenta")
    parser_table.add_column("On System Path?", style="steel_blue1")

    hub_parser_paths = find_hub_parsers()
    for index, parser_path in enumerate(hub_parser_paths):
        parser_table.add_row(str(index), str(parser_path), str(parser_path in system_paths))

    console = Console()
    console.print(path_table)
    console.print(parser_table)


@path_application.command(name="add")
def add_to_system_path() -> None:
    """
    Add file paths to the python system path for aiding in testing various data plugins
    """
    pass


def find_hub_parsers() -> list[pathlib.Path]:
    """
    Attempts to locate any potential hub-based parsers that are use across different plugins
    within a shared hub instance

    Will attempt to traverse recursively at most 3 levels above the present working directory
    The typical hub structure has the plugins directory at the same level as the hub directory

    pending.api structure:
    root
        ├── hub
        ├── plugins

    (mygene, mychem, myvariant, ...) structure
    root
    ├── src
    │   ├── hub
    │   ├── plugins

    In either structure, the user is expected to be operating within the directory of a specific
    plugin (root/plugin/plugin_directory/) or acting as a HUB within the (root/plugin) directory
    Either case we should be able to find the shared parsers within 3 upper levels
    """
    directory_pointer = pathlib.Path.cwd()

    traversal_counter = 0
    external_parser_paths = []

    # Match any path ending explicitly in hub. The bracket "[a]" matches the character literal
    # enclosed in the bracket, so [h][u][b] matches the literal hub
    match_expr = "**/[h][u][b]"
    while traversal_counter < 2:
        directory_pointer = directory_pointer.parent
        for hub_path in directory_pointer.glob(match_expr):
            hub_dataload = hub_path.joinpath("dataload")
            if hub_dataload.exists():
                external_parser_paths.append(hub_dataload.resolve().absolute())
        traversal_counter += 1
    return external_parser_paths
