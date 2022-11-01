import os
import pathlib
from types import SimpleNamespace
from typing import Optional

import typer

import biothings

_config = SimpleNamespace()
_config.HUB_DB_BACKEND = {
    "module": "biothings.utils.sqlite3",
    "sqlite_db_folder": ".biothings_hub",
}
_config.DATA_SRC_DATABASE = ".data_src_database"
_config.LOG_FOLDER = ".biothings_hub/logs"
biothings.config = _config
from biothings.utils.loggers import setup_default_log

logger = setup_default_log("standalone", _config.LOG_FOLDER, "INFO")

# To make sure biothings.config is initialized
from . import utils

app = typer.Typer(
    help="Direct testing under a data plugin folder, no hub needed, simple dump, upload and inspection. data can be stored in a sqlite db"
)


@app.command("dump", help="Download source data to local")
def dump(
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
def upload(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging")  # NOQA: B008
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
        utils.process_uploader(working_dir, data_folder, plugin_name, section, logger)


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
        default="mapping,type,stats",
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
    utils.process_inspect(source_name, mode, limit, merge, logger)


@app.command("serve")
def serve(
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
    - You can list all docs by:\n
    http://localhost:9999/tests/\n
    http://localhost:9999/tests/start=10&limit=10\n
    - You can filter out this doc by:\n
    http://localhost:9999/tests/?key.a.b=1 (find all docs that have nested dict keys a.b)\n
    http://localhost:9999/tests/?key.x.[].y=3 (find all docs that have mixed type dict-list)\n
    http://localhost:9999/tests/?key.x.[].z=4\n
    http://localhost:9999/tests/?key.x.[]=5\n
    - Or you can retrieve this doc by: http://localhost:9999/tests/123/\n
    """
    if verbose:
        logger.setLevel("DEBUG")
    working_dir = pathlib.Path().resolve()
    data_plugin_name = working_dir.name
    manifest = utils.get_manifest_content(working_dir)
    upload_section = manifest.get("uploader")
    table_space = [data_plugin_name]
    if not upload_section:
        upload_sections = manifest.get("uploaders")
        table_space = [item["name"] for item in upload_sections]
    utils.serve(port, data_plugin_name, table_space)
