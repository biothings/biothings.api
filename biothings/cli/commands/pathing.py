"""
Module for creating the cli interface for the path interface
"""

import logging
import pathlib
import sys

import typer
from rich.console import Console
from rich.table import Table

from biothings.cli.commands.decorators import cli_system_path, operation_mode


SHORT_HELP = "[green]CLI tool for viewing the python system path and adding external directories to the system path[/green]"
FULL_HELP = (
    SHORT_HELP
    + "\n\n[magenta] :sparkles: Run from an existing data plugin folder to evaluate a singular data plugin.[/magenta]"
)
path_application = typer.Typer(
    help=FULL_HELP,
    short_help=SHORT_HELP,
    no_args_is_help=True,
    rich_markup_mode="rich",
)

logger = logging.getLogger(name="biothings-cli")


@path_application.command(name="view")
def view_system_path() -> None:
    """
    View the system paths current discovered by python, along with potential hub directories of interest
    that the user may wish to add to the system path for usage in data plugin testing
    """
    display_system_paths()


@path_application.command(name="add")
def add_parser_to_system_path() -> None:
    """
    Add discovered hub directory paths to the python system path for aiding in testing various data plugins
    Creates the file "bt_custom.pth", in the same file extension used by the `site` module
    provided by python. It will create this file in the biothings_hub, and if found when running any
    command, it will add the files to the system path
    """
    update_system_paths()
    display_system_paths()


@path_application.command(name="remove")
def remove_parser_from_system_path() -> None:
    """
    Remove the hub parsers discovered from the python system path
    Simply removes the bt_custom.pth file from the biothings-cli directory
    """
    remove_system_paths()


@cli_system_path
@operation_mode
def display_system_paths() -> None:
    """
    Method for displaying the system path information used for the
    biothing-cli application

    External method so we can call it from multiple typer commands
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
        parser_table.add_row(str(index), str(parser_path), str(str(parser_path.parent) in system_paths))

    console = Console()
    console.print(path_table)
    console.print(parser_table)


@cli_system_path
@operation_mode
def update_system_paths() -> None:
    from biothings import config

    discovery_path = pathlib.Path(config.BIOTHINGS_CLI_PATH).resolve().absolute()
    discovery_path.mkdir(parents=True, exist_ok=True)

    hub_parser_paths = find_hub_parsers()

    # The actual path that needs to be added is the parent of the hub directory
    hub_parser_paths = [path.parent for path in hub_parser_paths]

    path_file = discovery_path.joinpath("biothings_cli.pth")
    with open(path_file, "w", encoding="utf-8") as path_handle:
        for parser_path in hub_parser_paths:
            logger.info("Adding %s -> %s", parser_path, path_file)
            path_handle.write(f"{parser_path}\n")


@cli_system_path
@operation_mode
def remove_system_paths() -> None:
    from biothings import config

    discovery_path = pathlib.Path(config.BIOTHINGS_CLI_PATH).resolve().absolute()
    path_file = discovery_path.joinpath("biothings_cli.pth")
    path_file.unlink(missing_ok=True)

    hub_parser_paths = find_hub_parsers()
    for parser_path in hub_parser_paths:
        try:
            sys.path.remove(str(parser_path))
        except ValueError:
            pass


def find_hub_parsers(upward_depth: int = 2) -> list[pathlib.Path]:
    """
    Attempts to locate any potential hub-based parsers that are use across different plugins
    within a shared hub instance

    Will attempt to traverse recursively by <upward_depth> levels (defaults to 2 levels) above the present working directory
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
    Either case we should be able to find the shared parsers within 2 upper levels
    """
    directory_pointer = pathlib.Path.cwd()

    traversal_counter = 0
    external_parser_paths = []

    # Match any path ending explicitly in hub. The bracket "[a]" matches the character literal
    # enclosed in the bracket, so [h][u][b] matches the literal hub
    match_expr = "**/[h][u][b]"
    while traversal_counter < upward_depth:
        directory_pointer = directory_pointer.parent
        for hub_path in directory_pointer.glob(match_expr):
            hub_dataload = hub_path.joinpath("dataload")
            if hub_dataload.exists():
                external_parser_paths.append(hub_path.resolve().absolute())
        traversal_counter += 1
    return external_parser_paths
