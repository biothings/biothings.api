from typing import Optional
import logging

import typer
from typing_extensions import Annotated

from biothings.cli import operations

logger = logging.getLogger("biothings-cli")

short_help = "[green]Test multiple data plugins in a local minimal hub without any external databases.[/green]"
long_help = (
    short_help
    + "\n\n[magenta]   :sparkles: Create your new data plugin in a sub-folder.[/magenta]"
    + "\n[magenta]   :sparkles: Dumping, uploading and inspecting your data plugin.[/magenta]"
    + "\n[magenta]   :sparkles: Serving your data as a web service for making simple queries[/magenta]"
    + "\n\n[green]   :point_right: Running this command outside of your data plugin[/green]"
    + "\n[green]   :point_right: That means your working directory can contains multiple data plugins[/green]"
    + "\n[green]   :point_right: Default traceback errors are kept minimal, but you can set [bold]BTCLI_RICH_TRACEBACK=1[/bold][/green]"
    + "\n[green]      ENV variable to enable full and pretty-formatted tracebacks, [/green]"
    + "\n[green]      or set [bold]BTCLI_DEBUG=1[/bold] to enable even more debug logs for debugging purpose.[/green]"
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
    """*create* command for creating a new data plugin from the template"""
    operations.do_create(name, multi_uploaders, parallelizer, logger=logger)


@app.command(
    name="dump",
    help="Download source data files to local",
)
def dump_data(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
):
    """*dump* command for downloading source data files to local"""
    operations.do_dump(plugin_name, logger=logger)


@app.command(
    name="upload",
    help="Convert downloaded data from dump step into JSON documents and upload the to the source database",
)
def upload_source(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
    batch_limit: Annotated[
        Optional[int],
        typer.Option(
            "--batch-limit",
            help="The maximum number of batches that should be uploaded. Batch size is 1000 docs",
        ),
    ] = None,
    # multi_uploaders: bool = typer.Option(
    #     False, "--multi-uploaders", help="Add this option if you want to create multiple uploaders"
    # ),
    # parallelizer: bool = typer.Option(
    #     False, "--parallelizer", help="Using parallelizer or not? Default: No"
    # ),
):
    """*upload* command for converting downloaded data from dump step into JSON documents and upload the to the source database.
    A local sqlite database used to store the uploaded data"""
    operations.do_upload(plugin_name, logger=logger)


@app.command(
    "dump_and_upload",
    help="Download data source to local folder then convert to Json document and upload to the source database",
)
def dump_and_upload(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
    # multi_uploaders: bool = typer.Option(
    #     False, "--multi-uploaders", help="Add this option if you want to create multiple uploaders"
    # ),
    # parallelizer: bool = typer.Option(
    #     False, "--parallelizer", help="Using parallelizer or not? Default: No"
    # ),
):
    """*dump_and_upload* command for downloading source data files to local, then converting them into JSON documents and uploading them to the source database.
    Two steps in one command."""
    operations.do_dump_and_upload(plugin_name, logger=logger)


@app.command(
    name="list",
    help="Listing dumped files or uploaded sources",
)
def listing(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
    dump: Annotated[Optional[bool], typer.Option("--dump", help="Listing dumped files")] = False,
    upload: Annotated[Optional[bool], typer.Option("--upload", help="Listing uploaded sources")] = False,
    hubdb: Annotated[Optional[bool], typer.Option("--hubdb", help="Listing internal hubdb content")] = False,
):
    """*list* command for listing dumped files and/or uploaded sources"""
    operations.do_list(plugin_name, dump, upload, hubdb, logger=logger)


@app.command(
    "inspect",
    help="Giving detailed information about the structure of documents coming from the parser",
)
def inspect_source(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
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
    # merge: Annotated[
    #     Optional[bool],
    #     typer.Option(
    #         "--merge",
    #         "-m",
    #         help="""Merge scalar into list when both exist (eg. {"val":..} and [{"val":...}])""",
    #     ),
    # ] = False,
    output: Annotated[
        Optional[str],
        typer.Option(
            "--output",
            "-o",
            help="The local JSON file path for storing mapping info if you run with mode 'mapping' (absolute path or relative path)",
        ),
    ] = None,
):
    """*inspect* command for giving detailed information about the structure of documents coming from the parser after the upload step"""
    operations.do_inspect(
        plugin_name=plugin_name,
        sub_source_name=sub_source_name,
        mode=mode,
        limit=limit,
        merge=False,
        output=output,
        logger=logger,
    )


@app.command("serve")
def serve(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
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
    *serve* command runs a simple API server for serving documents from the source database.

    For example, after run 'dump_and_upload', we have a source_name = "test" with a document structure
    like this:

    doc = {"_id": "123", "key": {"a":{"b": "1"},"x":[{"y": "3", "z": "4"}, "5"]}}.

    An API server will run at http://host:port/<your source name>/, like http://localhost:9999/test/:

        - You can see all available sources on the index page: http://localhost:9999/
        - You can list all docs: http://localhost:9999/test/ (default is to return the first 10 docs)
        - You can paginate doc list: http://localhost:9999/test/?start=10&limit=10
        - You can retrieve a doc by id: http://localhost:9999/test/123
        - You can filter out docs with one or multiple fielded terms:
            - http://localhost:9999/test/?q=key.a.b:1 (query by any field with dot notation like key.a.b=1)
            - http://localhost:9999/test/?q=key.a.b:1%20AND%20key.x.y=3 (find all docs that match two fields)
            - http://localhost:9999/test/?q=key.x.z:4*  (field value can contain wildcard * or ?)
            - http://localhost:9999/test/?q=key.x:5&start=10&limit=10 (pagination also works)
    """
    operations.do_serve(plugin_name=plugin_name, host=host, port=port, logger=logger)


@app.command(
    name="clean",
    help="Delete all dumped files and drop uploaded sources tables",
    no_args_is_help=True,
)
def clean_data(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
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
    """*clean* command for deleting all dumped files and/or drop uploaded sources tables"""
    operations.do_clean(plugin_name, dump=dump, upload=upload, clean_all=clean_all, logger=logger)
