import os
import pathlib
from typing import Optional

import typer
from rich import print as rprint
from typing_extensions import Annotated

from biothings.management import utils

logger = utils.get_logger("dataplugin-hub")

short_help = "[green]Test multiple data plugins in a local minimal hub without any external databases.[/green]"
long_help = (
    short_help
    + "\n\n[magenta]   :sparkles: Create your new data plugin in a sub-folder.[/magenta]"
    + "\n[magenta]   :sparkles: Dumping, uploading and inspecting your data plugin.[/magenta]"
    + "\n[magenta]   :sparkles: Serving your data as a web service for making simple queries[/magenta]"
    + "\n\n[green]   :point_right: Running this command outside of your data plugin[/green]"
    + "\n[green]   :point_right: That means your working directory can contains multiple data plugins[/green]"
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
    utils.create_data_plugin_template(name, multi_uploaders, parallelizer, logger)


@app.command(
    name="dump",
    help="Download source data files to local",
)
def dump_data(
    plugin_name: Annotated[
        str,
        typer.Option("--name", "-n", help="Provide a data plugin name", prompt="What's your data plugin name?"),
    ] = "",
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
):
    from biothings.management.utils import do_dump

    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = utils.load_plugin(plugin_name)
    del uploader_manager
    dumper_class = dumper_manager[plugin_name][0]
    data_folder = do_dump(dumper_class, plugin_name)
    rprint("[green]Success![/green]")
    utils.show_dumped_files(data_folder, plugin_name)


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
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
):
    from biothings.management.utils import do_upload

    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = utils.load_plugin(plugin_name)
    del dumper_manager
    uploader_classes = uploader_manager[plugin_name]
    do_upload(uploader_classes)
    rprint("[green]Success![/green]")
    utils.show_uploaded_sources(pathlib.Path(f"{working_dir}/{plugin_name}"), plugin_name)


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
    from biothings.management.utils import do_dump, do_upload

    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = utils.load_plugin(plugin_name)
    dumper_class = dumper_manager[plugin_name][0]
    uploader_classes = uploader_manager[plugin_name]

    data_folder = do_dump(dumper_class, plugin_name)

    do_upload(uploader_classes)

    rprint("[green]Success![/green]")
    utils.show_dumped_files(data_folder, plugin_name)
    utils.show_uploaded_sources(pathlib.Path(f"{working_dir}/{plugin_name}"), plugin_name)


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
    verbose: Annotated[
        Optional[bool],
        typer.Option("--verbose", "-v", help="Verbose logging", show_default=True),
    ] = False,
):
    if dump is False and upload is False and hubdb is False:
        # if all set to False, we list both dump and upload as the default
        dump = upload = True

    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = utils.load_plugin(plugin_name)
    dumper_class = dumper_manager[plugin_name][0]
    dumper = dumper_class()
    dumper.prepare()
    utils.run_sync_or_async_job(dumper.create_todump_list, force=True)
    if dump:
        data_folder = dumper.current_data_folder
        if not data_folder:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.error('Data folder is not available. Please run "dump" first.')
            # Typically we should not need to use new_data_folder as the data_folder,
            # but we keep the code commented out below for future reference
            # utils.run_sync_or_async_job(dumper.create_todump_list, force=True)
            # data_folder = dumper.new_data_folder
        utils.show_dumped_files(dumper.new_data_folder, plugin_name)
    if upload:
        utils.show_uploaded_sources(pathlib.Path(f"{working_dir}/{plugin_name}"), plugin_name)
    if hubdb:
        utils.show_hubdb_content()


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
    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    if not limit:
        limit = None
    logger.info(f"Inspect Data plugin {plugin_name} with sub-source name: {sub_source_name} mode: {mode} limit {limit}")

    source_full_name = plugin_name
    if sub_source_name:
        source_full_name = f"{plugin_name}.{sub_source_name}"
    dumper_manager, uploader_manager = utils.load_plugin(plugin_name)
    if len(uploader_manager[source_full_name]) > 1 and not sub_source_name:
        rprint("[red]This is a multiple uploaders data plugin, so '--sub-source-name' must be provided![/red]")
        rprint(
            f"[red]Accepted values of --sub-source-name are: {', '.join(uploader.name for uploader in uploader_manager[source_full_name])}[/red]"
        )
        exit(1)
    table_space = utils.get_uploaders(pathlib.Path(f"{working_dir}/{plugin_name}"))
    if sub_source_name and sub_source_name not in table_space:
        rprint(f"[red]Your source name {sub_source_name} does not exits[/red]")
        exit(1)
    if sub_source_name:
        utils.process_inspect(sub_source_name, mode, limit, merge, logger, do_validate=True, output=output)
    else:
        for source_name in table_space:
            utils.process_inspect(source_name, mode, limit, merge, logger, do_validate=True, output=output)


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
    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = utils.load_plugin(plugin_name)
    uploader_cls = uploader_manager[plugin_name]
    if not isinstance(uploader_cls, list):
        uploader_cls = [uploader_cls]
    table_space = [item.name for item in uploader_cls]
    utils.serve(host, port, plugin_name, table_space)


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
    if clean_all:
        dump = upload = True
    if dump is False and upload is False:
        rprint("[red]Please provide at least one of following option: --dump, --upload, --all[/red]")
        return exit(1)

    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = utils.load_plugin(plugin_name)
    del uploader_manager
    dumper_class = dumper_manager[plugin_name][0]
    dumper = dumper_class()
    dumper.prepare()
    # dumper.create_todump_list(force=True)
    data_plugin_dir = pathlib.Path(f"{working_dir}/{plugin_name}")
    if not utils.is_valid_working_directory(data_plugin_dir, logger=logger):
        return exit(1)
    if dump:
        data_folder = dumper.current_data_folder
        if not data_folder:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.error('Data folder is not available. Please run "dump" first.')
        utils.do_clean_dumped_files(data_folder, plugin_name)
    if upload:
        utils.do_clean_uploaded_sources(data_plugin_dir, plugin_name)
