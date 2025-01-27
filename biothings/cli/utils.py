"""
Utility functions for the biothings-cli tool

These are semantically separated from the operations
in that these functions aide in helping the operations
perform a task. Usually anything releated to plugin metadata,
job handling, and data manipulation should logically exist here
"""

import asyncio
import logging
import math
import os
import pathlib
import shutil
import time
from pprint import pformat
from typing import Callable, Union

import rich
import typer
import yaml
from rich import box, print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from biothings.cli.manager import CLIJobManager
from biothings.utils import es
from biothings.utils.common import timesofar
from biothings.utils.dataload import dict_traverse
from biothings.utils.serializer import load_json, to_json
from biothings.cli.structure import TEMPLATE_DIRECTORY


logger = logging.getLogger(name="biothings-cli")


def run_sync_or_async_job(job_manager: CLIJobManager, func: Callable, *args, **kwargs):
    """
    When func is defined as either normal or async function/method, we will call this function properly and return the results.
    For an async function/method, we will use CLIJobManager to run it.
    """
    if asyncio.iscoroutinefunction(func):
        kwargs["job_manager"] = job_manager
        return job_manager.loop.run_until_complete(func(*args, **kwargs))

    return func(*args, **kwargs)


def get_plugin_name(plugin_name=None, with_working_dir=True):
    """return a valid plugin name (the folder name contains a data plugin)
    When plugin_name is provided as None, it use the current working folder.
    when with_working_dir is True, returns (plugin_name, working_dir) tuple
    """
    working_dir = pathlib.Path().resolve()
    if plugin_name is None:
        plugin_name = working_dir.name
    else:
        valid_names = [f.name for f in os.scandir(working_dir) if f.is_dir() and not f.name.startswith(".")]
        if not plugin_name or plugin_name not in valid_names:
            rprint("[red]Please provide your data plugin name! [/red]")
            rprint("Choose from:\n    " + "\n    ".join(valid_names))
            raise typer.Exit(code=1)
    return plugin_name, working_dir if with_working_dir else plugin_name


def show_dumped_files(data_folder: Union[str, pathlib.Path], plugin_name: str) -> None:
    """
    A helper function to show the dumped files in the data folder
    """
    try:
        data_folder = pathlib.Path(data_folder).resolve().absolute()
        list_indent = "\n    - "
        file_collection_repr = list_indent + list_indent.join(pathobj.name for pathobj in data_folder.iterdir())
        message = (
            f"[green]Source:[/green][bold] {plugin_name}[/bold]\n"
            f"[green]Data Folder:[/green][bold] {data_folder}:[/bold]\n"
            f"[green]Data Folder Contents:[/green][bold]{file_collection_repr}"
        )
    except OSError:
        message = (
            f"[green]Source:[/green][bold] {plugin_name}[/bold]\n"
            f"[green]Data Folder:[/green][bold] {data_folder}:[/bold]\n"
            "Empty directory"
        )

    console = Console()
    console.print(
        Panel(
            message,
            title="[bold]Dump[/bold]",
            title_align="left",
        )
    )


def get_uploaded_collections(src_db, uploaders):
    """
    A helper function to get the uploaded collections in the source database
    """
    uploaded_sources = []
    archived_sources = []
    temp_sources = []
    for item in src_db.collection_names():
        if item in uploaders:
            uploaded_sources.append(item)
        for uploader_name in uploaders:
            if item.startswith(f"{uploader_name}_archive_"):
                archived_sources.append(item)
            if item.startswith(f"{uploader_name}_temp_"):
                temp_sources.append(item)
    return uploaded_sources, archived_sources, temp_sources


def show_hubdb_content():
    """
    Output hubdb content in a pretty format.
    """
    from biothings import config
    from biothings.utils import hub_db

    console = Console()
    hub_db.setup(config)
    coll_list = [
        hub_db.get_data_plugin(),
        hub_db.get_src_dump(),
        hub_db.get_src_master(),
        hub_db.get_hub_config(),
        hub_db.get_event(),
    ]
    hub_db_content = []
    for collection in coll_list:
        content = collection.find()
        if content:
            hub_db_content.append(f"[green]Collection:[/green] [bold]{collection.name}[/bold]\n{pformat(content)}")
    hub_db_content = "\n".join(hub_db_content)
    console.print(
        Panel(
            hub_db_content,
            title="[bold]Hubdb[/bold]",
            title_align="left",
        )
    )


def process_inspect(source_name, mode, limit, merge, logger, do_validate, output=None):
    """
    Perform inspect for the given source. It's used in do_inspect function below
    """
    from biothings.utils import hub_db
    from biothings.hub.datainspect.document_inspect import (
        inspect_docs,
        get_converters,
        merge_record,
        compute_metadata,
        stringify_inspect_doc,
        flatten_and_validate,
        run_converters,
    )

    VALID_INSPECT_MODES = ["jsonschema", "type", "mapping", "stats"]
    mode = mode.split(",")
    if "jsonschema" in mode:
        mode = ["jsonschema", "type"]
    for m in mode:
        if m not in VALID_INSPECT_MODES:
            logger.error('"%s" is not a valid inspect mode', m)
            raise typer.Exit(1)
    if not limit:
        limit = None
    sample = None
    clean = True

    t0 = time.time()
    data_provider = ("src", source_name)

    src_db = hub_db.get_src_db()
    pre_mapping = "mapping" in mode
    src_cols = src_db[source_name]
    inspected = {}
    converters, mode = get_converters(mode)
    for m in mode:
        inspected.setdefault(m, {})
    cur = src_cols.find()
    res = inspect_docs(
        cur,
        mode=mode,
        clean=clean,
        merge=merge,
        logger=logger,
        pre_mapping=pre_mapping,
        limit=limit,
        sample=sample,
        metadata=False,
        auto_convert=False,
    )

    for m in mode:
        inspected[m] = merge_record(inspected[m], res[m], m)
    for m in mode:
        if m == "mapping":
            try:
                inspected["mapping"] = es.generate_es_mapping(inspected["mapping"])
                # metadata for mapping only once generated
                inspected = compute_metadata(inspected, m)
            except es.MappingError as e:
                inspected["mapping"] = {"pre-mapping": inspected["mapping"], "errors": e.args[1]}
        else:
            inspected = compute_metadata(inspected, m)
    run_converters(inspected, converters)

    res = stringify_inspect_doc(inspected)
    _map = {"results": res, "data_provider": repr(data_provider), "duration": timesofar(t0)}

    # _map["started_at"] = started_at

    def clean_big_nums(k, v):
        # TODO: same with float/double? seems mongo handles more there ?
        if isinstance(v, int) and v > 2**64:
            return k, math.nan
        else:
            return k, v

    dict_traverse(_map, clean_big_nums)
    mapping = _map["results"].get("mapping", {})
    type_and_stats = {
        source_name: {
            _mode: flatten_and_validate(_map["results"].get(_mode, {}), do_validate) for _mode in ["type", "stats"]
        }
    }
    mapping_table = None
    if "mapping" in mode and mapping:
        if mapping.get("errors"):
            mapping_table = f"[red]{mapping.get('errors')}[/red]"
        else:
            mapping_table = Table(
                title="[bold green]MAPPING[/bold green]",
                box=box.SQUARE_DOUBLE_HEAD,
                expand=False,
                show_lines=True,
            )
            mapping_table.add_column(f"Sub source name: [bold]{source_name}[/bold]", justify="left", style="cyan")
            mapping_table.add_row(to_json(mapping, indent=True, sort_keys=True))
    report = []
    problem_summary = []
    if "stats" in mode:
        report = type_and_stats[source_name]["stats"]
    elif "type" in mode:
        for item in type_and_stats[source_name]["type"]:
            item.pop("stats", None)
            report.append(item)
    console = Console()
    if report:
        panel = Panel(
            f"This is the field type and stats for datasource: [bold green]{source_name}[/bold green]\n"
            f"It provides a summary of the data structure, including: a map of all types involved in the data;\n"
            f"basic statistics, showing how volumetry fits over data structure.\n"
            f"The basic statistics include these fields:\n"
            f"* [italic]_count[/italic]: Total records\n"
            f"* [italic]_max[/italic]: Maximum value\n"
            f"* [italic]_min[/italic]: Minimum value\n"
            f"* [italic]_none[/italic]: number of records have no value",
            title=f"[bold green]{source_name}[/bold green]",
            box=box.HORIZONTALS,
        )
        for field in report:
            warnings = field.pop("warnings", [])
            field["warning"] = ""
            for warning in warnings:
                field_name = field.get("field_name")
                if field_name == "__root__":
                    continue
                warning_mgs = f"* [red]{warning['code']}[/red]: {warning['message']}"
                problem_summary.append(warning_mgs)
            if warnings:
                field["warning"] = ",".join(item["code"] for item in warnings)

        table = Table(
            title="[bold green]TYPE & STATS[/bold green]",
            box=box.SQUARE_DOUBLE_HEAD,
            show_lines=True,
        )
        table.add_column("Field name", justify="left", style="cyan")
        table.add_column("Field type", justify="center", style="magenta")
        table.add_column("Stats._count", justify="right", style="green")
        table.add_column("Stats._max", justify="right", style="green")
        table.add_column("Stats._min", justify="right", style="green")
        table.add_column("Stats._none", justify="right", style="green")
        table.add_column("Warning", justify="right", style="red")
        for item in report:
            if item["field_name"] == "__root__":
                continue
            field_name = f'[bold]{item["field_name"]}[/bold]'
            if item["warning"] != "":
                field_name += "[red] \u26a0[/red]" * len(item["warning"].split(","))
            table.add_row(
                field_name,
                str(item["field_type"]),
                str(item.get("stats", {}).get("_count", "")),
                str(item.get("stats", {}).get("_max", "")),
                str(item.get("stats", {}).get("_min", "")),
                str(item.get("stats", {}).get("_none", "")),
                item["warning"],
            )
        problem_panel = None
        if problem_summary:
            problem_panel = Panel("[yellow]Warnings:[/yellow]\n" + "\n".join(problem_summary))
        console.print(panel)
        if mapping_table:
            console.print(mapping_table)
        console.print(table)
        if problem_panel:
            console.print(problem_panel)
    elif mapping_table:
        console.print(mapping_table)

    if "mapping" in mode and mapping and output:
        # TODO: the following block is commented out because we don't need to append the mapping info to the existing. Delete this block if we verify it's not needed.
        # with open(output, "w+") as fp:
        #     current_content = fp.read()
        #     if current_content:
        #         current_content = load_json(current_content)
        #     else:
        #         current_content = {}
        #     current_content.update(mapping)
        #     fp.write(to_json(current_content, indent=True, sort_keys=True))
        with open(output, "w", encoding="utf-8") as fp:
            fp.write(to_json(mapping, indent=True, sort_keys=True))
            rprint(f"[green]Successfully wrote the mapping info to the JSON file: [bold]{output}[/bold][/green]")


def get_manifest_content(working_dir: Union[str, pathlib.Path]) -> dict:
    """
    return the manifest content of the data plugin in the working directory
    """
    working_dir = pathlib.Path(working_dir).resolve().absolute()
    manifest_file_yml = working_dir.joinpath("manifest.yaml")
    manifest_file_json = working_dir.joinpath("manifest.json")

    manifest = {}
    if manifest_file_yml.exists():
        with open(manifest_file_yml, "r", encoding="utf-8") as yaml_handle:
            manifest = yaml.safe_load(yaml_handle)
    elif manifest_file_json.exists():
        with open(manifest_file_json, "r", encoding="utf-8") as json_handle:
            manifest = load_json(json_handle.read())
    else:
        logger.info("No manifest file discovered")
    return manifest


def get_uploaders(working_dir: pathlib.Path) -> list[str]:
    """
    A helper function to get the uploaders from the manifest file in the working directory
    used in show_uploaded_sources function below
    """
    data_plugin_name = working_dir.name

    manifest = get_manifest_content(working_dir)
    uploader_section = manifest.get("uploader", None)
    uploaders_section = manifest.get("uploaders", None)
    table_space = [data_plugin_name]
    if uploader_section is None and uploaders_section is not None:
        table_space = [item["name"] for item in uploaders_section]
    return table_space


def show_uploaded_sources(working_dir, plugin_name):
    """
    A helper function to show the uploaded sources from given plugin.
    """
    from biothings import config
    from biothings.utils import hub_db

    console = Console()
    uploaders = get_uploaders(working_dir)
    src_db = hub_db.get_src_db()
    uploaded_sources, archived_sources, temp_sources = get_uploaded_collections(src_db, uploaders)
    if not uploaded_sources:
        console.print(
            Panel(
                f"[green]Source:[/green] [bold]{plugin_name}[/bold]\n"
                + f"[green]DB path:[/green] [bold]{working_dir}/{src_db.dbfile}[/bold]\n"
                + f"[green]- Database:[/green] [bold]{src_db.name}[/bold]\n"
                + "Empty source!",
                title="[bold]Upload[/bold]",
                title_align="left",
            )
        )
    else:
        console.print(
            Panel(
                f"[green]Source:[/green] [bold]{plugin_name}[/bold]\n"
                + f"[green]DB path:[/green] [bold]{working_dir}/{src_db.dbfile}[/bold]\n"
                + f"[green]- Database:[/green] [bold]{src_db.name}[/bold]\n"
                + "    -[green] Collections:[/green] [bold]\n        "
                + "\n        ".join(uploaded_sources)
                + "[/bold] \n"
                + "    -[green] Archived collections:[/green][bold]\n        "
                + "\n        ".join(archived_sources)
                + "[/bold] \n"
                + "    -[green] Temporary collections:[/green][bold]\n        "
                + "\n        ".join(temp_sources),
                title="[bold]Upload[/bold]",
                title_align="left",
            )
        )


def remove_files_in_folder(folder_path):
    """
    Remove all files in a folder.
    """
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            rprint("[red]Failed to delete %s. Reason: %s [/red]" % (file_path, e))
    shutil.rmtree(folder_path)


def clean_dumped_files(data_folder: Union[str, pathlib.Path], plugin_name: str):
    """
    Remove all dumped files by a data plugin in the data folder.
    """
    if not os.path.isdir(data_folder):
        rich.print(f"[red]Data folder {data_folder} not found! Nothing has been dumped yet[/red]")
        return
    if not os.listdir(data_folder):
        rich.print("[red]Empty folder![/red]")
    else:
        rich.print(f"[green]There are all files dumped by [bold]{plugin_name}[/bold]:[/green]")
        print("\n".join(os.listdir(data_folder)))
        delete = typer.confirm("Do you want to delete them?")
        if not delete:
            raise typer.Abort()
        remove_files_in_folder(data_folder)
        rich.print("[green]Deleted![/green]")


def clean_uploaded_sources(working_dir, plugin_name):
    """
    Remove all uploaded sources by a data plugin in the working directory.
    """
    from biothings import config
    from biothings.utils import hub_db

    uploaders = get_uploaders(working_dir)
    src_db = hub_db.get_src_db()
    uploaded_sources = []
    for item in src_db.collection_names():
        if item in uploaders:
            uploaded_sources.append(item)
        for uploader_name in uploaders:
            if item.startswith(f"{uploader_name}_archive_") or item.startswith(f"{uploader_name}_temp_"):
                uploaded_sources.append(item)
    if not uploaded_sources:
        rich.print("[red]No source has been uploaded yet! [/red]")
    else:
        rich.print(f"[green]There are all sources uploaded by [bold]{plugin_name}[/bold]:[/green]")
        print("\n".join(uploaded_sources))
        delete = typer.confirm("Do you want to drop them?")
        if not delete:
            raise typer.Abort()
        for source in uploaded_sources:
            src_db[source].drop()
        rich.print("[green]All collections are dropped![/green]")
