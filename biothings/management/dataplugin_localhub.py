# flake8: noqa: B008
import os
import pathlib
from shutil import copytree
from typing import Optional

import tornado.template
import typer
from rich import print as rprint

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


def load_plugin(plugin_name):
    from biothings.hub.dataload.dumper import DumperManager
    from biothings.hub.dataload.uploader import UploaderManager
    from biothings.hub.dataplugin.assistant import LocalAssistant
    from biothings.hub.dataplugin.manager import DataPluginManager
    from biothings.utils.hub_db import get_data_plugin

    plugin_manager = DataPluginManager(job_manager=None)
    dmanager = DumperManager(job_manager=None)
    upload_manager = UploaderManager(job_manager=None)

    LocalAssistant.data_plugin_manager = plugin_manager
    LocalAssistant.dumper_manager = dmanager
    LocalAssistant.uploader_manager = upload_manager

    assistant = LocalAssistant(f"local://{plugin_name}")
    dp = get_data_plugin()
    dp.remove({"_id": assistant.plugin_name})
    dp.insert_one(
        {
            "_id": assistant.plugin_name,
            "plugin": {
                "url": f"local://{plugin_name}",
                "type": assistant.plugin_type,
                "active": True,
            },
            "download": {
                # "data_folder": "/data/biothings_studio/plugins/pharmgkb", # tmp fake
                "data_folder": f"./{plugin_name}",  # tmp path to your data plugin
            },
        }
    )
    p_loader = assistant.loader
    p_loader.load_plugin()

    return p_loader.__class__.dumper_manager, assistant.__class__.uploader_manager


@app.command(
    "dump_and_upload",
    help="Download data source to local folder then convert to Json document and upload to the source database",
)
def dump_and_upload(
    plugin_name: Optional[str] = typer.Option(
        "",
        "--name",
        "-n",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    )
    # multi_uploaders: bool = typer.Option(
    #     False, "--multi-uploaders", help="Add this option if you want to create multiple uploaders"
    # ),
    # parallelizer: bool = typer.Option(
    #     False, "--parallelizer", help="Using parallelizer or not? Default: No"
    # ),
):
    from biothings.hub.dataload.uploader import upload_worker
    from biothings.utils.hub_db import get_data_plugin

    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = load_plugin(plugin_name)
    dumper_class = dumper_manager[plugin_name][0]
    uploader_classes = uploader_manager[plugin_name]
    dumper = dumper_class()
    dumper.prepare()
    dumper.create_todump_list(force=True)
    for item in dumper.to_dump:
        dumper.download(item["remote"], item["local"])
    dumper.steps = ["post"]
    dumper.post_dump()
    dumper.register_status("success")
    dumper.release_client()

    for uploader_cls in uploader_classes:
        uploader = uploader_cls.create(db_conn_info="")
        uploader.make_temp_collection()
        uploader.prepare()
        upload_worker(
            uploader.fullname,
            uploader.__class__.storage_class,
            uploader.load_data,
            uploader.temp_collection_name,
            10000,
            1,
            uploader.data_folder,
            db=uploader.db,
        )
        uploader.switch_collection()

    # cleanup
    dumper.src_dump.remove({"_id": dumper.src_name})
    dp = get_data_plugin()
    dp.remove({"_id": plugin_name})
    rprint("[green]Success![/green]")
    utils.show_dumped_files(dumper.new_data_folder, plugin_name)
    utils.show_uploaded_sources(pathlib.Path(f"{working_dir}/{plugin_name}"), plugin_name)


@app.command(
    "inspect",
    help="Giving detailed information about the structure of documents coming from the parser",
)
def inspect(
    plugin_name: Optional[str] = typer.Option(
        "",
        "--name",
        "-n",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
    sub_source_name: Optional[str] = typer.Option(
        "",
        "--sub-source-name",
        "-s",
        help="Your sub source name",
    ),
    mode: Optional[str] = typer.Option(
        "type,stats",
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
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        "-l",
        help="""
        can limit the inspection to the x first docs (None = no limit, inspects all)
        """,
    ),
    merge: Optional[bool] = typer.Option(
        False,
        "--merge",
        "-m",
        help="""Merge scalar into list when both exist (eg. {"val":..} and [{"val":...}])""",
    ),
    output: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="The local JSON file path for storing mapping info if you run with mode 'mapping' (absolute path or relative path)",
    ),
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
    dumper_manager, uploader_manager = load_plugin(plugin_name)
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


@app.command(
    name="clean",
    help="Delete all dumped files and drop uploaded sources tables",
    no_args_is_help=True,
)
def clean_data(
    plugin_name: Optional[str] = typer.Option(
        "",
        "--name",
        "-n",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
    dump: bool = typer.Option(False, "--dump", help="Delete all dumped files"),
    upload: bool = typer.Option(False, "--upload", help="Drop uploaded sources tables"),
    clean_all: bool = typer.Option(
        False,
        "--all",
        help="Delete all dumped files and drop uploaded sources tables",
    ),
):
    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = load_plugin(plugin_name)
    dumper_class = dumper_manager[plugin_name][0]
    dumper = dumper_class()
    dumper.prepare()
    dumper.create_todump_list(force=True)
    data_plugin_dir = pathlib.Path(f"{working_dir}/{plugin_name}")
    if not utils.is_valid_working_directory(data_plugin_dir, logger=logger):
        return exit(1)
    if dump:
        utils.do_clean_dumped_files(pathlib.Path(dumper.new_data_folder), from_hub=True)
        return exit(0)
    if upload:
        utils.do_clean_uploaded_sources(data_plugin_dir)
        return exit(0)
    if clean_all:
        utils.do_clean_dumped_files(pathlib.Path(dumper.new_data_folder), from_hub=True)
        utils.do_clean_uploaded_sources(data_plugin_dir)
        return exit(0)
    rprint("[red]Please provide at least one of following option: --dump, --upload, --all[/red]")
    return exit(1)


@app.command(
    name="list",
    help="Listing dumped files or uploaded sources",
)
def listing(
    plugin_name: Optional[str] = typer.Option(
        "",
        "--name",
        "-n",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
    dump: bool = typer.Option(False, "--dump", help="Listing dumped files"),
    upload: bool = typer.Option(False, "--upload", help="Listing uploaded sources"),
):
    working_dir = pathlib.Path().resolve()
    valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
    if not plugin_name or plugin_name not in valid_names:
        rprint("[red]Please provide your data plugin name! [/red]")
        rprint("Choose from:\n    " + "\n    ".join(valid_names))
        return exit(1)
    dumper_manager, uploader_manager = load_plugin(plugin_name)
    dumper_class = dumper_manager[plugin_name][0]
    dumper = dumper_class()
    dumper.prepare()
    dumper.create_todump_list(force=True)
    if dump:
        utils.show_dumped_files(dumper.new_data_folder, plugin_name)
        return
    if upload:
        utils.show_uploaded_sources(pathlib.Path(f"{working_dir}/{plugin_name}"), plugin_name)
        return
    utils.show_dumped_files(dumper.new_data_folder, plugin_name)
    utils.show_uploaded_sources(pathlib.Path(f"{working_dir}/{plugin_name}"), plugin_name)


@app.command("serve")
def serve(
    plugin_name: Optional[str] = typer.Option(
        "",
        "--name",
        "-n",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
    host: Optional[str] = typer.Option(
        "localhost",
        "--host",
        help="API server ",
    ),
    port: Optional[int] = typer.Option(
        9999,
        "--port",
        "-p",
        help="API server port",
    ),
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
    dumper_manager, uploader_manager = load_plugin(plugin_name)
    uploader_cls = uploader_manager[plugin_name]
    if not isinstance(uploader_cls, list):
        uploader_cls = [uploader_cls]
    table_space = [item.name for item in uploader_cls]
    utils.serve(host, port, plugin_name, table_space)
