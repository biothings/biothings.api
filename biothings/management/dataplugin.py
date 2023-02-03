import os
import pathlib
from typing import Optional

import typer

from biothings import config
from biothings.utils.loggers import setup_default_log
from biothings.utils.sqlite3 import get_src_db

logger = setup_default_log("dataplugin", config.LOG_FOLDER, "INFO")

# To make sure biothings.config is initialized
from . import utils

app = typer.Typer(
    help="Direct testing under a data plugin folder, no hub needed, simple dump, upload and inspection. data can be stored in a sqlite db"
)


@app.command("list", help="Listing dumped files or uploaded sources")
def listing(
    dump: bool = typer.Option(False, "--dump", help="Listing dumped files"),  # NOQA: B008
    upload: bool = typer.Option(False, "--upload", help="Listing uploaded sources"),  # NOQA: B008
):
    working_dir = pathlib.Path().resolve()
    plugin_name = working_dir.name
    data_folder = os.path.join(working_dir, ".biothings_hub", "data_folder")
    if dump:
        print(f"There are dumped files by {plugin_name}:\n")
        if not os.listdir(data_folder):
            print("Empty file!")
        print("\n".join(os.listdir(data_folder)))
    if upload:
        uploaders = utils.get_uploaders(working_dir)
        src_db = get_src_db()
        print(f"There are uploaded sources by {plugin_name}:\n")
        uploaded_sources = [item for item in src_db.collection_names() if item in uploaders]
        if not uploaded_sources:
            print("Empty source!")
        else:
            print("\n".join(uploaded_sources))


@app.command("clean", help="Remove selected files from .biothings_hub folder")
def clean_data(
    dump: bool = typer.Option(False, "--dump", help="Clean dumped files"),  # NOQA: B008
    upload: bool = typer.Option(False, "--upload", help="Clean uploaded sources"),  # NOQA: B008
    clean_all: bool = typer.Option(False, "--all", help="Clean all"),  # NOQA: B008
):
    working_dir = pathlib.Path().resolve()
    if dump:
        utils.do_clean_dumped_files(working_dir)
    if upload:
        utils.do_clean_uploaded_sources(working_dir)
    if clean_all:
        utils.do_clean_dumped_files(working_dir)
        utils.do_clean_uploaded_sources(working_dir)


@app.command("dump", help="Download source data to local")
def dump_data(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging")  # NOQA: B008
):
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
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


@app.command(
    "upload",
    help="Convert downloaded data from dump step into JSON documents and upload the to the source database",
)
def upload_source(
    limit: Optional[int] = typer.Option(  # NOQA: B008
        None,
        "--limit",
        help="Can limit the upload to the first limit * 1000 docs (None = no limit, upload all)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),  # NOQA: B008
):
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
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
        utils.process_uploader(working_dir, data_folder, plugin_name, section, logger, limit)


@app.command(
    "inspect",
    help="Giving detailed information about the structure of documents coming from the parser",
)
def inspect(
    sub_source_name: Optional[str] = typer.Option(  # NOQA: B008
        default="",
        help="Your sub source name",
    ),
    mode: Optional[str] = typer.Option(  # NOQA: B008
        default="type,stats",
        help="""
            The inspect mode or list of modes (comma separated) eg. "type,mapping".\n
            Possible values are:\n
            - "type": explore documents and report strict data structure\n
            - "mapping": same as type but also perform test on data so guess best mapping\n
               (eg. check if a string is splitable, etc...). Implies merge=True\n
            - "stats": explore documents and compute basic stats (count,min,max,sum)\n
            - "deepstats": same as stats but record values and also compute mean,stdev,median (memory intensive...)\n
            - "jsonschema", same as "type" but returned a json-schema formatted result\n""",
    ),
    limit: Optional[int] = typer.Option(  # NOQA: B008
        None,
        "--limit",
        help="Can limit the inspection to the x first docs (None = no limit, inspects all)",
    ),
    merge: Optional[bool] = typer.Option(  # NOQA: B008
        False,
        "--merge",
        help="""Merge scalar into list when both exist (eg. {"val":..} and [{"val":...}]""",
    ),
    validate: Optional[bool] = typer.Option(  # NOQA: B008
        True,
        "--validate",
        help="""Validate data""",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),  # NOQA: B008
):
    """ """
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
    plugin_name = working_dir.name
    working_dir = pathlib.Path().resolve()
    manifest = utils.get_manifest_content(working_dir)
    upload_section = manifest.get("uploader")
    source_name = plugin_name
    if not upload_section:
        upload_sections = manifest.get("uploaders")
        table_space = [item["name"] for item in upload_sections]
        source_name = sub_source_name
        if sub_source_name not in table_space:
            logger.error(f"Your source name {sub_source_name} does not exits")
            return
    utils.process_inspect(source_name, mode, limit, merge, logger, validate)


@app.command("serve")
def serve(
    host: Optional[str] = typer.Option(  # NOQA: B008
        default="localhost",
        help="API server ",
    ),
    port: Optional[int] = typer.Option(  # NOQA: B008
        default=9999,
        help="API server port",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),  # NOQA: B008
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
    http://host:port/<your source name>/?key.x.[].y=3 (find all docs that have mixed type dict-list)\n
    http://host:port/<your source name>/?key.x.[].z=4\n
    http://host:port/<your source name>/?key.x.[]=5\n
    - Or you can retrieve this doc by: http://host:port/<your source name>/123/\n
    """
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
    table_space = utils.get_uploaders(working_dir)
    data_plugin_name = working_dir.name
    utils.serve(host, port, data_plugin_name, table_space)
