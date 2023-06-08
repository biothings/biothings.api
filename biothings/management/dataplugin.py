import os
import pathlib
from typing import Optional

import typer
from rich import print as rprint
from typing_extensions import Annotated

from biothings.management import utils

logger = utils.get_logger("dataplugin")


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
    name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
    multi_uploaders: Annotated[
        Optional[bool],
        typer.Option("--multi-uploaders", help="If provided, the data plugin includes multiple uploaders"),
    ] = False,
    # parallelizer: bool = typer.Option(False, "--parallelizer", help="Using parallelizer or not? Default: No"),
    parallelizer: Annotated[
        Optional[bool],
        typer.Option("--parallelizer", help="If provided, the data plugin's upload step will run in parallel"),
    ] = False,
):
    utils.do_create(name, multi_uploaders, parallelizer, logger=logger)


@app.command(
    name="dump",
    help="Download source data files to local",
)
def dump_data(
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
):
    if verbose:
        logger.setLevel("DEBUG")

    mode = "v2"
    if mode == "v1":
        working_dir = pathlib.Path().resolve()
        if not utils.is_valid_working_directory(working_dir, logger=logger):
            return exit(1)
        plugin_name = working_dir.name
        data_folder = os.path.join(working_dir, ".biothings_hub", "data_folder")
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
        rprint("[green]Success![/green]")
        utils.show_dumped_files(data_folder, plugin_name)
    else:
        utils.do_dump(plugin_name=None, logger=logger)


@app.command(
    name="upload",
    help="Convert downloaded data from dump step into JSON documents and upload the to the source database",
)
def upload_source(
    batch_limit: Annotated[
        Optional[int],
        typer.Option(
            "--batch-limit",
            help="The maximum number of batches that should be uploaded. Batch size is 1000 docs",
        ),
    ] = None,
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
):
    if verbose:
        logger.setLevel("DEBUG")

    mode = "v2"
    if mode == "v1":
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
    else:
        utils.do_upload(plugin_name=None, logger=logger)


@app.command(
    "dump_and_upload",
    help="Download data source to local folder then convert to Json document and upload to the source database",
)
def dump_and_upload(
    # multi_uploaders: bool = typer.Option(
    #     False, "--multi-uploaders", help="Add this option if you want to create multiple uploaders"
    # ),
    # parallelizer: bool = typer.Option(
    #     False, "--parallelizer", help="Using parallelizer or not? Default: No"
    # ),
):
    utils.do_dump_and_upload(plugin_name=None, logger=logger)


@app.command(
    name="list",
    help="Listing dumped files or uploaded sources",
)
def listing(
    dump: Annotated[Optional[bool], typer.Option("--dump", help="Listing dumped files")] = False,
    upload: Annotated[Optional[bool], typer.Option("--upload", help="Listing uploaded sources")] = False,
    hubdb: Annotated[Optional[bool], typer.Option("--hubdb", help="Listing internal hubdb content")] = False,
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
):
    utils.do_list(plugin_name=None, dump=dump, upload=upload, hubdb=hubdb, logger=logger)


@app.command(
    name="inspect",
    help="Giving detailed information about the structure of documents coming from the parser",
)
def inspect_source(
    sub_source_name: Annotated[
        Optional[str], typer.Option("--sub-source-name", "-s", help="Your sub source name")
    ] = "",
    mode: Annotated[
        Optional[str],
        typer.Option(
            "--mode",
            "-m",
            help="""
            The inspect mode or list of modes (comma separated), e.g. "type,mapping".\n
            Possible values are:\n
            - "type": explore documents and report strict data structure\n
            - "mapping": same as type but also perform test on data so guess best mapping\n
               (eg. check if a string is splitable, etc...). Implies merge=True\n
            - "stats": explore documents and compute basic stats (count,min,max,sum)\n
            """,
        ),
    ] = "type,stats",
    limit: Annotated[
        Optional[int],
        typer.Option(
            "--limit",
            "-l",
            help="""
            can limit the inspection to the x first docs (None = no limit, inspects all)
            """,
        ),
    ] = None,
    merge: Annotated[
        Optional[bool],
        typer.Option(
            "--merge",
            "-m",
            help="""Merge scalar into list when both exist (eg. {"val":..} and [{"val":...}])""",
        ),
    ] = False,
    output: Annotated[
        Optional[str],
        typer.Option(
            "--output",
            "-o",
            help="The local JSON file path for storing mapping info if you run with mode 'mapping' (absolute path or relative path)",
        ),
    ] = None,
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
):
    """ """
    if verbose:
        logger.setLevel("DEBUG")
    utils.do_inspect(
        plugin_name=None,
        sub_source_name=sub_source_name,
        mode=mode,
        limit=limit,
        merge=merge,
        output=output,
        logger=logger,
    )


@app.command(name="serve")
def serve(
    host: Annotated[
        Optional[str],
        typer.Option(
            "--host",
            help="The host name to run the test API server",
        ),
    ] = "localhost",
    port: Annotated[
        Optional[int],
        typer.Option(
            "--port",
            "-p",
            help="The port number to tun the test API server",
        ),
    ] = 9999,
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
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
    utils.do_serve(plugin_name=None, host=host, port=port)


@app.command(
    name="clean",
    help="Delete all dumped files and drop uploaded sources tables",
    no_args_is_help=True,
)
def clean_data(
    dump: Annotated[Optional[bool], typer.Option("--dump", help="Delete all dumped files")] = False,
    upload: Annotated[Optional[bool], typer.Option("--upload", help="Drop uploaded sources tables")] = False,
    clean_all: Annotated[
        Optional[bool],
        typer.Option(
            "--all",
            help="Delete all dumped files and drop uploaded sources tables",
        ),
    ] = False,
):
    utils.do_clean(plugin_name=None, dump=dump, upload=upload, clean_all=clean_all, logger=logger)
