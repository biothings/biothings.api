import logging
import os
import pathlib
from types import SimpleNamespace
from typing import Optional

import typer

app = typer.Typer()

logger = logging.getLogger(__file__)
logging.basicConfig(level="INFO")

import biothings

_config = SimpleNamespace()
_config.HUB_DB_BACKEND = {
    "module": "biothings.utils.sqlite3",
    "sqlite_db_folder": ".biothings_hub",
}
_config.DATA_HUB_DB_DATABASE = ".hubdb"
_config.DATA_SRC_DATABASE = ".data_src_database"
_config.LOG_FOLDER = ".biothings_hub/biothings_hub_logs"
_config.logger = logger

biothings.config = _config

# To make sure biothings.config is initialized
from . import utils


@app.command("dump")
def dump():
    workspace_dir = pathlib.Path().resolve()
    manifest = utils.get_manifest_content(workspace_dir)
    to_dumps = utils.get_todump_list(manifest.get("dumper"))
    for to_dump in to_dumps:
        utils.download(**to_dump)


@app.command("upload")
def upload():
    workspace_dir = pathlib.Path().resolve()
    plugin_name = workspace_dir.name
    local_archive_dir = os.path.join(workspace_dir, ".biothings_hub")
    data_folder = os.path.join(workspace_dir, ".biothings_hub", "data_folder")
    os.makedirs(local_archive_dir, exist_ok=True)
    manifest = utils.get_manifest_content(workspace_dir)
    upload_sections = manifest.get("uploaders")
    if not upload_sections:
        upload_section = manifest.get("uploader")
        upload_sections = [upload_section]

    for section in upload_sections:
        utils.process_uploader(workspace_dir, data_folder, plugin_name, section)


@app.command("inspect")
def inspect(
    sub_source_name: Optional[str] = typer.Option(  # NOQA: B008
        default="",
        help="Your sub source name",
    ),
    mode: Optional[str] = typer.Option(  # NOQA: B008
        default="mapping,type,stats",
        help="""
            mode: the inspect mode or list of modes (comma separated) eg. "type,mapping".
            Possible values are:
            - "type": (default) explore documents and report strict data structure
            - "mapping": same as type but also perform test on data so guess best mapping
                       (eg. check if a string is splitable, etc...). Implies merge=True
            - "stats": explore documents and compute basic stats (count,min,max,sum)
            - "deepstats": same as stats but record values and also compute mean,stdev,median
                         (memory intensive...)
            - "jsonschema", same as "type" but returned a json-schema formatted result
            """,
    ),
    limit: Optional[int] = typer.Option(  # NOQA: B008
        None,
        "--limit",
        help="""
        can limit the inspection to the x first docs (None = no limit, inspects all)
        """,
    ),
    merge: Optional[bool] = typer.Option(  # NOQA: B008
        False,
        "--merge",
        help="""
        merge scalar into list when both exist (eg. {"val":..} and [{"val":...}]
        """,
    ),
):
    workspace_dir = pathlib.Path().resolve()
    plugin_name = workspace_dir.name
    workspace_dir = pathlib.Path().resolve()
    manifest = utils.get_manifest_content(workspace_dir)
    upload_section = manifest.get("uploader")
    source_name = plugin_name
    if not upload_section:
        upload_sections = manifest.get("uploaders")
        table_space = [item["name"] for item in upload_sections]
        source_name = sub_source_name
        if sub_source_name not in table_space:
            logger.error(f"Your source name {sub_source_name} does not exits")
            return
    utils.process_inspect(source_name, mode, limit, merge)


@app.command("serve")
def serve(
    port: Optional[int] = typer.Option(  # NOQA: B008
        default=9999,
        help="API server port",
    ),
):
    workspace_dir = pathlib.Path().resolve()
    data_plugin_name = workspace_dir.name
    manifest = utils.get_manifest_content(workspace_dir)
    upload_section = manifest.get("uploader")
    table_space = [data_plugin_name]
    if not upload_section:
        upload_sections = manifest.get("uploaders")
        table_space = [item["name"] for item in upload_sections]
    utils.serve(port, data_plugin_name, table_space)
