"""
Module for creating the cli interface for the dataplugin interface
"""

from typing import Optional
import asyncio

from typing_extensions import Annotated
import typer

from biothings.cli import operations


SHORT_HELP = "[green]CLI tool for locally evaluating a biothings dataplugin. Allows for simple querying and data inspection.[/green]"
FULL_HELP = (
    SHORT_HELP
    + "\n\n[magenta]   :sparkles: Run from an existing data plugin folder to evaluate a singular data plugin.[/magenta]"
    + "\n[magenta]   :sparkles: Run from a parent folder containing multiple data plugins to operate like a hub.[/magenta]"
    + "\n[green]   :point_right: Set [bold]BTCLI_RICH_TRACEBACK=1[/bold] ENV variable to enable full and pretty-formatted tracebacks, [/green]"
    + "\n[green]   :point_right: Set [bold]BTCLI_DEBUG=1[/bold] to enable even more debug logs for debugging purpose.[/green]"
    + "\n[green]   :point_right: Include a config.py at the working directory to override the default biothings.config settings.[/green]"
    + "\n   :rocket::boom::sparkling_heart:"
)

dataplugin_application = typer.Typer(
    help=FULL_HELP,
    short_help=SHORT_HELP,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


@dataplugin_application.command(name="create")
def create_data_plugin(
    name: Annotated[
        str,
        typer.Option("--plugin-name", "-n", help="Provide a data source plugin name"),
    ] = "",
    multi_uploaders: Annotated[
        Optional[bool],
        typer.Option("--multi-uploaders", help="If provided, the data plugin includes multiple uploaders"),
    ] = False,
    parallelizer: Annotated[
        Optional[bool],
        typer.Option("--parallelizer", help="If provided, the data plugin's upload step will run in parallel"),
    ] = False,
):
    """
    Create a new data plugin from a pre-defined template
    """
    operations.do_create(name, multi_uploaders, parallelizer)


@dataplugin_application.command(name="dump")
def dump_source(
    plugin_name: Annotated[str, typer.Option("--plugin-name", "-n", help="Provide a data source plugin name")] = None,
    show_dump: Annotated[
        Optional[bool],
        typer.Option("--show-dump", help="Displays the dump source result output after dump operation"),
    ] = True,
):
    """
    Download the source data files to the local file system
    """
    asyncio.run(operations.do_dump(plugin_name=plugin_name, show_dumped=show_dump))


@dataplugin_application.command(name="upload")
def upload_source(
    plugin_name: Annotated[str, typer.Option("--plugin-name", "-n", help="Data source plugin name")] = None,
    batch_limit: Annotated[
        Optional[int],
        typer.Option(
            "--batch-limit",
            help="The maximum number of documents per batch that should be uploaded",
        ),
    ] = 10000,
    parallel: Annotated[
        Optional[bool],
        typer.Option(
            "--parallel",
            help="Used for uploaders that leverage the ParallelizedUploader source for the plugin",
        ),
    ] = False,
    show_upload: Annotated[
        Optional[bool],
        typer.Option("--show-upload", help="Displays the uploader source result output after upload operation"),
    ] = True,
):
    """
    Parse the downloaded data files from the dump operation and upload to the source database

    Default database is sqlite3, but mongodb is supported if configured and an
    instance is setup

    [green]NOTE[/green]
    Only works correctly if the dump command has been run
    """
    upload_arguments = {"plugin_name": plugin_name, "batch_limit": batch_limit, "show_uploaded": show_upload}

    if parallel:
        asyncio.run(operations.do_parallel_upload(**upload_arguments))
    else:
        asyncio.run(operations.do_upload(**upload_arguments))


@dataplugin_application.command(name="dump_and_upload")
def dump_and_upload(
    plugin_name: Annotated[str, typer.Option("--plugin-name", "-n", help="Data source plugin name")] = None,
):
    """
    Sequentially execute the dump and upload commands

    Operation Order:
    1) downloads source data files to local file system
    2) converts them into JSON documents
    3) uploads those JSON documents to the source database.
    """
    asyncio.run(operations.do_dump_and_upload(plugin_name=plugin_name))


@dataplugin_application.command(name="list")
def listing(
    plugin_name: Annotated[str, typer.Option("--plugin-name", "-n", help="Provide a data source plugin name")] = None,
    dump: Annotated[Optional[bool], typer.Option("--dump", help="Listing dumped files")] = True,
    upload: Annotated[Optional[bool], typer.Option("--upload", help="Listing uploaded sources")] = True,
    hubdb: Annotated[Optional[bool], typer.Option("--hubdb", help="Listing internal hubdb content")] = False,
):
    """
    List dumped files, uploaded sources, or internal hubdb contents
    """
    asyncio.run(operations.do_list(plugin_name=plugin_name, dump=dump, upload=upload, hubdb=hubdb))


@dataplugin_application.command(name="inspect")
def inspect_source(
    plugin_name: Annotated[str, typer.Option("--plugin-name", "-n", help="Provide a data source plugin name")] = None,
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
):
    """
    Derive detailed information about the document data structure from the parsed documents

    [green]NOTE[/green]
    Only works correctly if the upload command has been run
    """
    asyncio.run(
        operations.do_inspect(
            plugin_name=plugin_name,
            sub_source_name=sub_source_name,
            mode=mode,
            limit=limit,
            merge=merge,
            output=output,
        )
    )


@dataplugin_application.command(name="serve")
def serve(
    plugin_name: Annotated[str, typer.Option("--plugin-name", "-n", help="Provide a data source plugin name")] = None,
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
):
    """
    Run a simple API server for serving documents from the source database

    For example, we have a source_name = "test" with the following document structure:
    doc = {
        "_id": "123",
        "key": {
            "a": {"b": "1"},
            "x": [
                {"y": "3", "z": "4"},
                "5"
            ]
        }
    }

    An API server will run at http://host:port/<your source name>/ (e.g http://localhost:9999/test/)

        - See all available sources on the index page: http://localhost:9999/
        - List all docs: http://localhost:9999/test/ (default is to return the first 10 docs)
        - Paginate doc list: http://localhost:9999/test/?start=10&limit=10
        - Retrieve a doc by id: http://localhost:9999/test/123
        - Filter out docs with one or multiple fielded terms:
            - http://localhost:9999/test/?q=key.a.b:1 (query by any field with dot notation like key.a.b=1)
            - http://localhost:9999/test/?q=key.a.b:1%20AND%20key.x.y=3 (find all docs that match two fields)
            - http://localhost:9999/test/?q=key.x.z:4*  (field value can contain wildcard * or ?)
            - http://localhost:9999/test/?q=key.x:5&start=10&limit=10 (pagination also works)
    """
    asyncio.run(operations.do_serve(plugin_name=plugin_name, host=host, port=port))


@dataplugin_application.command(name="clean", no_args_is_help=True)
def clean_data(
    plugin_name: Annotated[str, typer.Option("--plugin-name", "-n", help="Provide a data source plugin name")] = None,
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
    """
    Delete all dumped files and/or drop uploaded sources tables
    """
    asyncio.run(operations.do_clean(plugin_name=plugin_name, dump=dump, upload=upload, clean_all=clean_all))


@dataplugin_application.command(name="index")
def index_plugin(
    plugin_name: Annotated[str, typer.Option("--plugin", help="Data source plugin name")] = None,
):
    """
    [red][bold](experimental)[/bold][/red] Create an elaticsearch index from a data source database

    Our `quick-index` function that provides a way for quickly creating an elasticsearch
    index from a source backend

    We currently only support converting between MongoDB -> Elasticsearch for indexing

    [green]NOTE[/green]
    Only works correctly if the upload command has been run
    """
    asyncio.run(operations.do_index(plugin_name=plugin_name))


@dataplugin_application.command(name="validate")
def validate_manifest(
    plugin_name: Annotated[str, typer.Option("--plugin-name", help="Data source plugin name")] = None,
    manifest_file: Annotated[str, typer.Option("--manifest-file", "-m", help="Data source manifest file")] = None,
    show_schema: Annotated[bool, typer.Option("--show-schema", help="Display biothings manifest schema")] = None,
) -> None:
    """
    [red][bold](experimental)[/bold][/red] Validate a provided manifest file via JSONSchema

    Performs jsonschema validation against the manifest file.
    Will not perform validation against the potential loading of modules
    within the manifest

    if the --show-schema argument is applied, then display the biothings
    manifest schema

    The schema is located within the biothings repository at the following
    path relative to root:
    <biothings/hub/dataplugin/loaders/schema/manifest.json>

    For a reference about jsonschema itself, see the following:
    https://json-schema.org/
    """
    asyncio.run(operations.validate_manifest(plugin_name=plugin_name, manifest_file=manifest_file))
    if show_schema:
        asyncio.run(operations.display_schema())
