import asyncio
import importlib
import json
import math
import os
import pathlib
import shutil
import sys
import time
from ftplib import FTP
from functools import partial
from urllib import parse as urlparse

import requests
import typer
import yaml
from orjson import orjson
from rich import box, print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

import biothings.utils.inspect as btinspect
from biothings.utils import es, storage
from biothings.utils.common import get_random_string, get_timestamp, timesofar, uncompressall
from biothings.utils.dataload import dict_traverse
from biothings.utils.sqlite3 import get_src_db
from biothings.utils.workers import upload_worker


def get_todump_list(dumper_section):
    working_dir = pathlib.Path().resolve()
    data_folder = os.path.join(working_dir, ".biothings_hub", "data_folder")
    remote_urls = dumper_section.get("data_url")
    uncompress = dumper_section.get("uncompress")
    to_dumps = []
    for remote_url in remote_urls:
        filename = os.path.basename(remote_url)
        local_file = os.path.join(data_folder, filename)
        if "ftp" in remote_url:
            to_dumps.append(
                {
                    "schema": "ftp",
                    "remote_url": remote_url,
                    "local_file": local_file,
                    "uncompress": uncompress,
                }
            )
        elif "http" in remote_url:
            to_dumps.append(
                {
                    "schema": "http",
                    "remote_url": remote_url,
                    "local_file": local_file,
                    "uncompress": uncompress,
                }
            )
        elif "https" in remote_url:
            to_dumps.append(
                {
                    "schema": "https",
                    "remote_url": remote_url,
                    "local_file": local_file,
                    "uncompress": uncompress,
                }
            )
        else:
            raise Exception("Not supported schema")
    return to_dumps


def _get_optimal_buffer_size(ftp_host):
    known_optimal_sizes = {
        "ftp.ncbi.nlm.nih.gov": 33554432,
        # see https://ftp.ncbi.nlm.nih.gov/README.ftp for reason
        # add new ones above
        "DEFAULT": 8192,
    }
    normalized_host = ftp_host.lower()
    if normalized_host in known_optimal_sizes:
        return known_optimal_sizes[normalized_host]
    else:
        return known_optimal_sizes["DEFAULT"]


def download(logger, schema, remote_url, local_file, uncompress=True):
    logger.debug(f"Start download {remote_url}")
    local_dir = os.path.dirname(local_file)
    os.makedirs(local_dir, exist_ok=True)
    if schema in ["http", "https"]:
        client = requests.Session()
        res = client.get(remote_url, stream=True, headers={})
        if not res.status_code == 200:
            raise Exception(
                "Error while downloading '%s' (status: %s, reason: %s)"
                % (remote_url, res.status_code, res.reason)
            )
        logger.info("Downloading '%s' as '%s'" % (remote_url, local_file))
        fout = open(local_file, "wb")
        for chunk in res.iter_content(chunk_size=512 * 1024):
            if chunk:
                fout.write(chunk)
        fout.close()
        logger.info(f"Successful download {remote_url}")
    if schema == "ftp":
        split = urlparse.urlsplit(remote_url)
        ftp_host = split.hostname
        ftp_timeout = 10 * 60.0
        ftp_user = split.username or ""
        ftp_passwd = split.password or ""
        cwd_dir = "/".join(split.path.split("/")[:-1])
        remotefile = split.path.split("/")[-1]
        client = FTP(ftp_host, timeout=ftp_timeout)
        client.login(ftp_user, ftp_passwd)
        if cwd_dir:
            client.cwd(cwd_dir)
        try:
            with open(local_file, "wb") as out_f:
                client.retrbinary(
                    cmd="RETR %s" % remotefile,
                    callback=out_f.write,
                    blocksize=_get_optimal_buffer_size(ftp_host),
                )
            # set the mtime to match remote ftp server
            response = client.sendcmd("MDTM " + remotefile)
            code, lastmodified = response.split()
            logger.info(f"Successful download {remote_url}")
        except Exception as e:
            logger.error("Error while downloading %s: %s" % (remotefile, e))
            client.close()
            raise
        finally:
            client.close()
    if uncompress:
        uncompressall(local_dir)
    return os.listdir(local_dir)


def make_temp_collection(uploader_name):
    return f"{uploader_name}_temp_{get_random_string()}"


def switch_collection(db, temp_collection_name, collection_name, logger):
    if temp_collection_name and db[temp_collection_name].count() > 0:
        if collection_name in db.collection_names():
            # renaming existing collections
            new_name = "_".join([collection_name, "archive", get_timestamp(), get_random_string()])
            logger.info(
                f"Renaming collection {collection_name} to {new_name} for archiving purpose."
            )
            db[collection_name].rename(new_name, dropTarget=True)
        logger.info(f"Renaming collection {temp_collection_name} to {collection_name}")
        db[temp_collection_name].rename(collection_name)
    else:
        raise Exception("No temp collection (or it's empty)")


def load_module_locally(module_name, working_dir):
    file_path = os.path.join(working_dir, f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def get_load_data_func(working_dir, parser, **kwargs):
    module_name, func = parser.split(":")
    module = load_module_locally(module_name, working_dir)
    func = getattr(module, func)
    return partial(func, **kwargs)


def get_custom_mapping_func(working_dir, mapping):
    module_name, func = mapping.split(":")
    module = load_module_locally(module_name, working_dir)
    func = getattr(module, func)
    return func


def process_uploader(working_dir, data_folder, main_source, upload_section, logger, limit):
    parser = upload_section.get("parser")
    parser_kwargs = upload_section.get("parser_kwargs")
    parser_kwargs_serialized = {}
    if parser_kwargs:
        parser_kwargs_serialized = orjson.loads(parser_kwargs)
    # mapping = upload_section.get("mapping")
    name = upload_section.get("name")
    ondups = upload_section.get("on_duplicates")

    if name:
        uploader_fullname = name
    else:
        uploader_fullname = main_source
    temp_collection_name = make_temp_collection(uploader_fullname)
    src_db = get_src_db()
    storage_class_name = storage.get_storage_class(ondups)
    storage_mod, class_name = storage_class_name.rsplit(".", 1)
    storage_mod = importlib.import_module(storage_mod)
    storage_class = getattr(storage_mod, class_name)
    load_data_func = get_load_data_func(working_dir, parser, **parser_kwargs_serialized)
    # TODO
    # if mapping:
    #     mapping_func = get_custom_mapping_func(working_dir, mapping)
    upload_worker(
        uploader_fullname,
        storage_class,
        load_data_func,
        temp_collection_name,
        1000,
        1,
        data_folder,
        db=src_db,
        max_batch_num=limit,
    )
    switch_collection(
        src_db,
        temp_collection_name=temp_collection_name,
        collection_name=uploader_fullname,
        logger=logger,
    )


def process_inspect(source_name, mode, limit, merge, logger, do_validate):
    mode = mode.split(",")
    if "jsonschema" in mode:
        mode = ["jsonschema", "type"]
    if not limit:
        limit = None
    sample = None
    clean = True
    logger.info(f"Inspecting source name: {source_name} mode: {mode} limit {limit} merge {merge}")

    t0 = time.time()
    data_provider = ("src", source_name)

    src_db = get_src_db()
    pre_mapping = "mapping" in mode
    src_cols = src_db[source_name]
    inspected = {}
    converters, mode = btinspect.get_converters(mode)
    for m in mode:
        inspected.setdefault(m, {})
    cur = src_cols.find()
    res = btinspect.inspect_docs(
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
        inspected[m] = btinspect.merge_record(inspected[m], res[m], m)
    for m in mode:
        if m == "mapping":
            try:
                inspected["mapping"] = es.generate_es_mapping(inspected["mapping"])
                # metadata for mapping only once generated
                inspected = btinspect.compute_metadata(inspected, m)
            except es.MappingError as e:
                inspected["mapping"] = {"pre-mapping": inspected["mapping"], "errors": e.args[1]}
        else:
            inspected = btinspect.compute_metadata(inspected, m)
    btinspect.run_converters(inspected, converters)

    res = btinspect.stringify_inspect_doc(inspected)
    _map = {"results": res, "data_provider": repr(data_provider), "duration": timesofar(t0)}

    # _map["started_at"] = started_at

    def clean_big_nums(k, v):
        # TODO: same with float/double? seems mongo handles more there ?
        if isinstance(v, int) and v > 2**64:
            return k, math.nan
        else:
            return k, v

    dict_traverse(_map, clean_big_nums)
    mapping = _map["results"].get("mapping", {}).get(source_name.lower(), {}).get("properties")
    type_and_stats = {
        source_name: {
            _mode: btinspect.flatten_and_validate(_map["results"].get(_mode, {}), do_validate)
            for _mode in ["type", "stats"]
        }
    }
    mapping_table = None
    if "mapping" in mode and mapping:
        mapping_table = Table(
            title="[bold green]MAPPING[/bold green]",
            box=box.SQUARE_DOUBLE_HEAD,
            expand=False,
            show_lines=True,
        )
        mapping_table.add_column("Field name", justify="left", style="cyan")
        mapping_table.add_column("Info", justify="left", style="green")
        for key, value in mapping.items():
            mapping_table.add_row(str(key), json.dumps(value))
    report = []
    problem_summary = []
    if "stats" in mode:
        report = type_and_stats[source_name]["stats"]
    elif "type" in mode:
        for item in type_and_stats[source_name]["type"]:
            item.pop("stats", None)
            report.append(item)
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
                str(item["stats"]["_count"]),
                str(item["stats"]["_max"]),
                str(item["stats"]["_min"]),
                str(item["stats"]["_none"]),
                item["warning"],
            )
        problem_panel = None
        if problem_summary:
            problem_panel = Panel("[yellow]Warnings:[/yellow]\n" + "\n".join(problem_summary))
        console = Console()
        console.print(panel)
        if mapping_table:
            console.print(mapping_table)
        console.print(table)
        if problem_panel:
            console.print(problem_panel)


def get_manifest_content(working_dir):
    manifest_file_yml = os.path.join(working_dir, "manifest.yaml")
    manifest_file_json = os.path.join(working_dir, "manifest.json")
    if os.path.isfile(manifest_file_yml):
        manifest = yaml.safe_load(open(manifest_file_yml))
        return manifest
    elif os.path.isfile(manifest_file_json):
        manifest = json.load(open(manifest_file_json))
        return manifest
    else:
        raise FileNotFoundError("manifest file does not exits in current working directory")


def serve(host, port, plugin_name, table_space):
    from .web_app import main

    src_db = get_src_db()
    print(f"Serving data plugin source: {plugin_name}")
    asyncio.run(main(host=host, port=port, db=src_db, table_space=table_space))


def get_uploaders(working_dir: pathlib.Path):
    data_plugin_name = working_dir.name
    manifest = get_manifest_content(working_dir)
    upload_section = manifest.get("uploader")
    table_space = [data_plugin_name]
    if not upload_section:
        upload_sections = manifest.get("uploaders")
        table_space = [item["name"] for item in upload_sections]
    return table_space


def remove_files_in_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            rprint("[red]Failed to delete %s. Reason: %s [/red]" % (file_path, e))


def do_clean_dumped_files(working_dir):
    plugin_name = working_dir.name
    data_folder = os.path.join(working_dir, ".biothings_hub", "data_folder")
    if not os.listdir(data_folder):
        print("Empty folder!")
    else:
        rprint(f"[green]There are all files dumped by [bold]{plugin_name}[/bold]:[/green]")
        print("\n".join(os.listdir(data_folder)))
        delete = typer.confirm("Do you want to delete them?")
        if not delete:
            raise typer.Abort()
        remove_files_in_folder(data_folder)
        rprint("[green]Deleted![/green]")


def do_clean_uploaded_sources(working_dir):
    plugin_name = working_dir.name
    uploaders = get_uploaders(working_dir)
    src_db = get_src_db()
    uploaded_sources = [item for item in src_db.collection_names() if item in uploaders]
    if not uploaded_sources:
        print("Empty sources!")
    else:
        rprint(f"[green]There are all sources uploaded by [bold]{plugin_name}[/bold]:[/green]")
        print("\n".join(uploaded_sources))
        delete = typer.confirm("Do you want to drop them?")
        if not delete:
            raise typer.Abort()
        for source in uploaded_sources:
            src_db[source].drop()
        rprint("[green]All sources are dropped![/green]")


def show_dumped_files(data_folder, plugin_name):
    console = Console()
    if not os.path.isdir(data_folder) or not os.listdir(data_folder):
        console.print(Panel("Empty file!"))
    else:
        console.print(
            Panel(
                "\n".join(os.listdir(data_folder)),
                title=f"[green]There are all files dumped by [bold]{plugin_name}[/bold][/green] at {data_folder}",
                title_align="left",
            )
        )


def show_uploaded_sources(working_dir, plugin_name):
    console = Console()
    uploaders = get_uploaders(working_dir)
    src_db = get_src_db()
    uploaded_sources = [item for item in src_db.collection_names() if item in uploaders]
    if not uploaded_sources:
        console.print(Panel("Empty source!"))
    else:
        console.print(
            Panel(
                f'{" " * 5}[bold]DB path:[/bold] {src_db.dbfile}"\n{" " * 5}'
                f'[bold]Database:[/bold] {src_db.name}"\n{" " * 5}[bold]Collections:[/bold] '
                + ", ".join(uploaded_sources),
                title=f"[green]There are all sources uploaded by [bold]{plugin_name}[/bold][/green]",
                title_align="left",
            )
        )
