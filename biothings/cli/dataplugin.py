from typing import Optional

import typer
from typing_extensions import Annotated

from biothings.cli import utils

logger = utils.get_logger("biothings-cli")


short_help = (
    "[green]Test an individual data plugin locally and make simple queries to inspect your parsed data objects.[/green]"
)
long_help = (
    short_help
    + "\n\n[magenta]   :sparkles: Go to your existing data plugin folder.[/magenta]"
    + "\n[magenta]   :sparkles: Dumping, uploading and inspecting your data plugin.[/magenta]"
    + "\n[magenta]   :sparkles: Serving your data as a web service for making simple queries[/magenta]"
    + "\n\n[green]   :point_right: Always run this command inside of your data plugin folder.[/green]"
    + "\n[green]   :point_right: Default traceback errors are kept minimal, but you can set [bold]BTCLI_RICH_TRACEBACK=1[/bold][/green]"
    + "\n[green]      ENV variable to enable full and pretty-formatted tracebacks, [/green]"
    + "\n[green]      or set [bold]BTCLI_DEBUG=1[/bold] to enable even more debug logs for debugging purpose.[/green]"
    + "\n[green]   :point_right: You can include a config.py at the working directly to override the default biothings.config settings.[/green]"
    + "\n   :rocket::boom::sparkling_heart:"
)

dataplugin_application = typer.Typer(
    help=long_help,
    short_help=short_help,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


@dataplugin_application.command(
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
    parallelizer: Annotated[
        Optional[bool],
        typer.Option("--parallelizer", help="If provided, the data plugin's upload step will run in parallel"),
    ] = False,
):
    """
    *create* command

    creates a new data plugin from a pre-defined template
    """
    utils.do_create(name, multi_uploaders, parallelizer, logger=logger)


@dataplugin_application.command(
    name="dump",
    help="Download source data files to local",
)
def dump_data():
    """
    *dump* command

    downloads source data files to the local file system
    """
    utils.do_dump(plugin_name=None, logger=logger)


@dataplugin_application.command(
    name="upload",
    help="Convert downloaded data from dump step into JSON documents and upload the to the source database",
)
def upload_source(
    batch_limit: Annotated[
        Optional[int],
        typer.Option(
            "--batch-limit",
            help="The maximum number of batches that should be uploaded. Default Batch size is 10000 docs",
        ),
    ] = None,
):
    """
    *upload* command

    ***NOTE***
    Only works correctly if the dump command has been run

    converts the data from the dump operation into JSON documents. Then uploads to a local
    source database. Default database is sqlite3, but mongodb is supported if configured and an
    instance is setup
    """
    utils.do_upload(plugin_name=None, logger=logger)


@dataplugin_application.command(
    "dump_and_upload",
    help="Download data source to local folder then convert to Json document and upload to the source database",
)
def dump_and_upload():
    """
    *dump_and_upload* command

    performs the dump and then upload commands sequentially
    1) downloads source data files to local file system
    2) converts them into JSON documents
    3) uploads those JSON documents to the source database.
    """
    utils.do_dump_and_upload(plugin_name=None, logger=logger)


@dataplugin_application.command(
    name="list",
    help="Listing dumped files or uploaded sources",
)
def listing(
    dump: Annotated[Optional[bool], typer.Option("--dump", help="Listing dumped files")] = False,
    upload: Annotated[Optional[bool], typer.Option("--upload", help="Listing uploaded sources")] = False,
    hubdb: Annotated[Optional[bool], typer.Option("--hubdb", help="Listing internal hubdb content")] = False,
):
    """
    *list* command

    lists dumped files and/or uploaded sources
    """
    utils.do_list(plugin_name=None, dump=dump, upload=upload, hubdb=hubdb, logger=logger)


@dataplugin_application.command(
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
):
    """
    *inspect* command

    gives detailed information about the structure of documents coming from the parser after the upload step
    """

    utils.do_inspect(
        plugin_name=None,
        sub_source_name=sub_source_name,
        mode=mode,
        limit=limit,
        merge=False,
        output=output,
        logger=logger,
    )


@dataplugin_application.command(name="serve")
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
    utils.do_serve(plugin_name=None, host=host, port=port, logger=logger)


@dataplugin_application.command(
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
    """
    *clean* command

    deletes all dumped files and/or drops uploaded sources tables
    """
    utils.do_clean(plugin_name=None, dump=dump, upload=upload, clean_all=clean_all, logger=logger)
