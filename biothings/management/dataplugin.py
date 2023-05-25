# flake8: noqa: B008
import os
import pathlib
from typing import Optional

import typer
from rich import print as rprint

# from biothings import config
from biothings.management.utils import get_logger

# logger = setup_default_log("dataplugin", config.LOG_FOLDER, "INFO")
logger = get_logger("dataplugin")

# To make sure biothings.config is initialized
from . import utils


def extra_help_msg():
    """print additional help msg here"""
    rprint("[magenta]:sparkles: Always run this command inside of your data plugin folder. [/magenta]")
    rprint(
        "[magenta]:sparkles: To override the default biothing.config, please define the config.py at the working directory. :rocket::boom:[/magenta]"
    )


short_help = (
    "[green]Test an individual data plugin locally and make simple queries to inspect your parsed data objects.[/green]"
)
long_help = (
    short_help
    + "\n\n[magenta]   :sparkles: Go to your existing data plugin folder.[/magenta]"
    + "\n[magenta]   :sparkles: Dumping, uploading and inspecting your data plugin.[/magenta]"
    + "\n[magenta]   :sparkles: Serving your data as a web service for making simple queries[/magenta]"
    + "\n\n[green]   :point_right: Always run this command inside of your data plugin folder.[/green]"
    + "\n[green]   :point_right: You can include a config.py at the working directly to override the default biothings.config settings.[/green]"
    + "\n   :rocket::boom::sparkling_heart:"
)

app = typer.Typer(
    help=long_help,
    short_help=short_help,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


@app.command(
    name="create",
    help="Create a new data plugin from the tempplate",
)
def create_data_plugin(
    name: Optional[str] = typer.Option(
        default="",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
    multi_uploaders: bool = typer.Option(
        False, "--multi-uploaders", help="Add this option if you want to create multiple uploaders"
    ),
    parallelizer: bool = typer.Option(False, "--parallelizer", help="Using parallelizer or not? Default: No"),
):
    utils.create_data_plugin_template(name, multi_uploaders, parallelizer, logger)


@app.command(
    name="dump",
    help="Download source data files to local",
)
def dump_data(verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging", show_default=True)):
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
    if not utils.is_valid_working_directory(working_dir, logger=logger):
        return exit(1)
    manifest = utils.get_manifest_content(working_dir)
    to_dumps = utils.get_todump_list(manifest.get("dumper"))
    for to_dump in to_dumps:
        utils.download(
            logger,
            to_dump["schema"],
            to_dump["remote_url"],
            to_dump["local_file"],
            to_dump["uncompress"],
        )
    plugin_name = working_dir.name
    data_folder = os.path.join(working_dir, ".biothings_hub", "data_folder")
    rprint("[green]Success![/green]")
    utils.show_dumped_files(data_folder, plugin_name)


@app.command(
    name="upload",
    help="Convert downloaded data from dump step into JSON documents and upload the to the source database",
)
def upload_source(
    batch_limit: Optional[int] = typer.Option(
        None,
        "--batch-limit",
        help="The maximum number of batches that should be uploaded. Batch size is 1000 docs",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),
):
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
    if not utils.is_valid_working_directory(working_dir, logger=logger):
        return exit(1)
    plugin_name = working_dir.name
    local_archive_dir = os.path.join(working_dir, ".biothings_hub")
    data_folder = os.path.join(working_dir, ".biothings_hub", "data_folder")
    os.makedirs(local_archive_dir, exist_ok=True)
    manifest = utils.get_manifest_content(working_dir)
    upload_sections = manifest.get("uploaders")
    if not upload_sections:
        upload_section = manifest.get("uploader")
        upload_sections = [upload_section]

    for section in upload_sections:
        utils.process_uploader(working_dir, data_folder, plugin_name, section, logger, batch_limit)
    rprint("[green]Success![/green]")
    utils.show_uploaded_sources(working_dir, plugin_name)


@app.command(
    name="list",
    help="Listing dumped files or uploaded sources",
)
def listing(
    dump: bool = typer.Option(False, "--dump", help="Listing dumped files"),
    upload: bool = typer.Option(False, "--upload", help="Listing uploaded sources"),
):
    working_dir = pathlib.Path().resolve()
    plugin_name = working_dir.name
    if not utils.is_valid_working_directory(working_dir, logger=logger):
        return exit(1)
    data_folder = os.path.join(working_dir, ".biothings_hub", "data_folder")
    if dump:
        utils.show_dumped_files(data_folder, plugin_name)
        return
    if upload:
        utils.show_uploaded_sources(working_dir, plugin_name)
        return
    utils.show_dumped_files(data_folder, plugin_name)
    utils.show_uploaded_sources(working_dir, plugin_name)


@app.command(
    name="inspect",
    help="Giving detailed information about the structure of documents coming from the parser",
)
def inspect_source(
    sub_source_name: Optional[str] = typer.Option(
        default="",
        help="Your sub source name",
    ),
    mode: Optional[str] = typer.Option(
        default="type,stats",
        help="""
            The inspect mode or list of modes (comma separated), e.g. "type,mapping".\n
            Possible values are:\n
            - "type": explore documents and report strict data structure\n
            - "mapping": same as type but also perform test on data so guess best mapping\n
               (eg. check if a string is splitable, etc...). Implies merge=True\n
            - "stats": explore documents and compute basic stats (count,min,max,sum)\n
            """,
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        "-l",
        help="Can limit the inspection to the x first docs (None = no limit, inspects all)",
    ),
    merge: Optional[bool] = typer.Option(
        False,
        "--merge",
        "-m",
        help="""Merge scalar into list when both exist (eg. {"val":..} and [{"val":...}])""",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),
    output: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="The local JSON file path for storing mapping info if you run with mode 'mapping' (absolute path or relative path)",
    ),
):
    """ """
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
    if not utils.is_valid_working_directory(working_dir, logger=logger):
        return exit(1)
    table_space = utils.get_uploaders(working_dir)
    if sub_source_name and sub_source_name not in table_space:
        rprint(f"[red]Your source name {sub_source_name} does not exits[/red]")
        return
    if sub_source_name:
        utils.process_inspect(sub_source_name, mode, limit, merge, logger, True, output)
    else:
        for source_name in table_space:
            utils.process_inspect(source_name, mode, limit, merge, logger, True, output)


@app.command(name="serve")
def serve(
    host: Optional[str] = typer.Option(
        default="localhost",
        help="API server ",
    ),
    port: Optional[int] = typer.Option(
        default=9999,
        help="API server port",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),
):
    """
    Run the simple API server for serving documents from the source database, \n
    Support pagination by using: start=&limit= \n
    Support filtering by document keys, for example:\n
    After run 'dump_and_upload', we have a source_name = "test"\n
    doc = {"_id": "123", "key": {"a":{"b": "1"},"x":[{"y": "3", "z": "4"}, "5"]}}.\n
    - You can see all available sources on the index page: http://host:port/
    - You can list all docs by:\n
    http://host:port/<your source name>/\n
    http://host:port/<your source name>/start=10&limit=10\n
    - You can filter out this doc by:\n
    http://host:port/<your source name>/?key.a.b=1 (find all docs that have nested dict keys a.b)\n
    http://host:port/<your source name>/?key.x.y=3 (find all docs that have mixed type dict-list)\n
    http://host:port/<your source name>/?key.x.z=4\n
    http://host:port/<your source name>/?key.x=5\n
    - Or you can retrieve this doc by: http://host:port/<your source name>/123/\n
    """
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
    if not utils.is_valid_working_directory(working_dir, logger=logger):
        return exit(1)
    table_space = utils.get_uploaders(working_dir)
    data_plugin_name = working_dir.name
    utils.serve(host, port, data_plugin_name, table_space)


@app.command(
    name="clean",
    help="Delete all dumped files and drop uploaded sources tables",
    no_args_is_help=True,
)
def clean_data(
    dump: bool = typer.Option(False, "--dump", help="Delete all dumped files"),
    upload: bool = typer.Option(False, "--upload", help="Drop uploaded sources tables"),
    clean_all: bool = typer.Option(
        False,
        "--all",
        help="Delete all dumped files and drop uploaded sources tables",
    ),
):
    working_dir = pathlib.Path().resolve()
    if not utils.is_valid_working_directory(working_dir, logger=logger):
        return exit(1)
    if dump:
        utils.do_clean_dumped_files(working_dir)
        return exit(0)
    if upload:
        utils.do_clean_uploaded_sources(working_dir)
        return exit(0)
    if clean_all:
        utils.do_clean_dumped_files(working_dir)
        utils.do_clean_uploaded_sources(working_dir)
        return exit(0)
    rprint("[red]Please provide at least one of following option: --dump, --upload, --all[/red]")
    return exit(1)
