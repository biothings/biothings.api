import asyncio
import importlib
import json
import logging
import math
import os
import pathlib
import shutil
import sys
import time
from ftplib import FTP
from functools import partial
from types import SimpleNamespace
from urllib import parse as urlparse

import requests
import tornado.template
import typer
import yaml
from orjson import orjson
from rich import box, print as rprint
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table

import biothings.utils.inspect as btinspect
from biothings.utils import es, storage
from biothings.utils.common import get_random_string, get_timestamp, timesofar, uncompressall
from biothings.utils.dataload import dict_traverse
from biothings.utils.sqlite3 import get_src_db
from biothings.utils.workers import upload_worker


def get_logger(name=None):
    logging.basicConfig(
        level="INFO",
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
    )
    logger = logging.getLogger(name=None)
    return logger


def run_sync_or_async_job(func, *args, **kwargs):
    """When func is defined as either normal or async function/method, we will call this function properly and return the results.
    For an async function/method, we will use CLIJobManager to run it.
    """
    if asyncio.iscoroutinefunction(func):
        from biothings.utils.manager import CLIJobManager

        job_manager = CLIJobManager()
        kwargs["job_manager"] = job_manager
        return job_manager.loop.run_until_complete(func(*args, **kwargs))
    else:
        # otherwise just run it as normal
        return func(*args, **kwargs)


def load_plugin_managers(plugin_path, plugin_name=None, data_folder=None):
    from biothings import config as btconfig
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

    _plugin_path = pathlib.Path(plugin_path).resolve()
    btconfig.DATA_PLUGIN_FOLDER = _plugin_path.parent.as_posix()
    plugin_name = plugin_name or _plugin_path.name
    data_folder = data_folder or f"./{plugin_name}"
    assistant = LocalAssistant(f"local://{plugin_name}")
    # print(assistant.plugin_name, plugin_name, _plugin_path.as_posix(), btconfig.DATA_PLUGIN_FOLDER)
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
                "data_folder": data_folder,  # tmp path to your data plugin
            },
        }
    )
    p_loader = assistant.loader
    p_loader.load_plugin()

    return p_loader.__class__.dumper_manager, assistant.__class__.uploader_manager


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


def load_plugin(plugin_name=None, dumper=True, uploader=True):
    _plugin_name, working_dir = get_plugin_name(plugin_name, with_working_dir=True)
    if plugin_name is None:
        # current working_dir has the data plugin
        dumper_manager, uploader_manager = load_plugin_managers(working_dir, data_folder=".")
        data_plugin_dir = working_dir
    else:
        dumper_manager, uploader_manager = load_plugin_managers(_plugin_name)
        data_plugin_dir = pathlib.Path(f"{working_dir}/{plugin_name}")
    current_plugin = SimpleNamespace(
        plugin_name=_plugin_name,
        data_plugin_dir=data_plugin_dir,
        in_plugin_dir=plugin_name is None,
    )
    if dumper:
        dumper_class = dumper_manager[_plugin_name][0]
        _dumper = dumper_class()
        _dumper.prepare()
        current_plugin.dumper = _dumper
    if uploader:
        uploader_classes = uploader_manager[_plugin_name]
        if not isinstance(uploader_classes, list):
            uploader_classes = [uploader_classes]
        current_plugin.uploader_classes = uploader_classes
    return current_plugin


########################
# for create command   #
########################


def do_create(name, multi_uploaders=False, parallelizer=False, logger=None):
    """Create a new data plugin from the template"""
    logger = logger or get_logger(__name__)
    working_dir = pathlib.Path().resolve()
    biothing_source_dir = pathlib.Path(__file__).parent.parent.resolve()
    template_dir = os.path.join(biothing_source_dir, "hub", "dataplugin", "templates")
    plugin_dir = os.path.join(working_dir, name)
    if os.path.isdir(plugin_dir):
        logger.error("Data plugin with the same name is already exists, please remove it before create")
        return exit(1)
    shutil.copytree(template_dir, plugin_dir)
    # create manifest file
    loader = tornado.template.Loader(plugin_dir)
    parsed_template = (
        loader.load("manifest.yaml.tpl").generate(multi_uploaders=multi_uploaders, parallelizer=parallelizer).decode()
    )
    manifest_file_path = os.path.join(working_dir, name, "manifest.yaml")
    with open(manifest_file_path, "w") as fh:
        fh.write(parsed_template)

    # remove manifest template
    os.unlink(f"{plugin_dir}/manifest.yaml.tpl")
    if not parallelizer:
        os.unlink(f"{plugin_dir}/parallelizer.py")
    logger.info(f"Successful create data plugin template at: \n {plugin_dir}")


def get_todump_list(dumper_section):
    # Deprecated!
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
    # deprecated!
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
    # deprecated!
    logger.debug(f"Start download {remote_url}")
    local_dir = os.path.dirname(local_file)
    os.makedirs(local_dir, exist_ok=True)
    if schema in ["http", "https"]:
        client = requests.Session()
        res = client.get(remote_url, stream=True, headers={})
        if not res.status_code == 200:
            raise Exception(
                "Error while downloading '%s' (status: %s, reason: %s)" % (remote_url, res.status_code, res.reason)
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


###############################
# for dump & upload command   #
###############################


def do_dump(plugin_name=None, show_dumped=True, logger=None):
    """Perform dump for the given plugin"""
    from biothings import config
    from biothings.utils import hub_db

    hub_db.setup(config)
    logger = logger or get_logger(__name__)
    _plugin = load_plugin(plugin_name, dumper=True, uploader=False)
    dumper = _plugin.dumper
    dumper.prepare()
    run_sync_or_async_job(dumper.create_todump_list, force=True)
    for item in dumper.to_dump:
        logger.info('Downloading remote data from "%s"...', item["remote"])
        dumper.download(item["remote"], item["local"])
        logger.info('Downloaded locally as "%s"', item["local"])
    dumper.steps = ["post"]
    dumper.post_dump()
    dumper.register_status("success")
    dumper.release_client()
    # cleanup
    # Commented out this line below. we should keep the dump info in src_dump collection for other cmds, e.g. upload, list etc
    # dumper.src_dump.remove({"_id": dumper.src_name})
    dp = hub_db.get_data_plugin()
    dp.remove({"_id": _plugin.plugin_name})
    data_folder = dumper.new_data_folder
    if show_dumped:
        logger.info("[green]Success![/green] :rocket:", extra={"markup": True})
        show_dumped_files(data_folder, _plugin.plugin_name)
    return _plugin


def do_upload(plugin_name=None, show_uploaded=True, logger=None):
    """Perform upload for the given list of uploader_classes"""
    from biothings.hub.dataload.uploader import upload_worker

    logger = logger or get_logger(__name__)

    _plugin = load_plugin(plugin_name, dumper=False, uploader=True)
    for uploader_cls in _plugin.uploader_classes:
        uploader = uploader_cls.create(db_conn_info="")
        uploader.make_temp_collection()
        uploader.prepare()
        if not uploader.data_folder or not pathlib.Path(uploader.data_folder).exists():
            logger.error(
                'Data folder "%s" for "%s" is empty or does not exist yet. Have you run "dump" yet?',
                uploader.data_folder,
                uploader.fullname,
            )
            raise typer.Exit(1)
        else:
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
            uploader.keep_archive = 3  # keep 3 archived collections, that's probably good enough for CLI, default is 10
            uploader.clean_archived_collections()
    if show_uploaded:
        logger.info("[green]Success![/green] :rocket:", extra={"markup": True})
        show_uploaded_sources(pathlib.Path(_plugin.data_plugin_dir), _plugin.plugin_name)
    return _plugin


def do_dump_and_upload(plugin_name, logger=None):
    logger = logger or get_logger(__name__)
    _plugin = do_dump(plugin_name, show_dumped=False, logger=logger)
    do_upload(plugin_name, show_uploaded=False, logger=logger)
    logger.info("[green]Success![/green] :rocket:", extra={"markup": True})
    show_dumped_files(_plugin.dumper.new_data_folder, _plugin.plugin_name)
    show_uploaded_sources(pathlib.Path(_plugin.data_plugin_dir), _plugin.plugin_name)


def make_temp_collection(uploader_name):
    # Deprecated!
    return f"{uploader_name}_temp_{get_random_string()}"


def switch_collection(db, temp_collection_name, collection_name, logger):
    # Deprecated!
    if temp_collection_name and db[temp_collection_name].count() > 0:
        if collection_name in db.collection_names():
            # renaming existing collections
            new_name = "_".join([collection_name, "archive", get_timestamp(), get_random_string()])
            logger.info(f"Renaming collection {collection_name} to {new_name} for archiving purpose.")
            db[collection_name].rename(new_name, dropTarget=True)
        logger.info(f"Renaming collection {temp_collection_name} to {collection_name}")
        db[temp_collection_name].rename(collection_name)
    else:
        raise Exception("No temp collection (or it's empty)")


def process_uploader(working_dir, data_folder, main_source, upload_section, logger, batch_limit):
    # Deprecated!
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
        max_batch_num=batch_limit,
    )
    switch_collection(
        src_db,
        temp_collection_name=temp_collection_name,
        collection_name=uploader_fullname,
        logger=logger,
    )


def load_module_locally(module_name, working_dir):
    # Deprecated!
    file_path = os.path.join(working_dir, f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def get_load_data_func(working_dir, parser, **kwargs):
    # Deprecated!
    module_name, func = parser.split(":")
    module = load_module_locally(module_name, working_dir)
    func = getattr(module, func)
    return partial(func, **kwargs)


def get_custom_mapping_func(working_dir, mapping):
    # Deprecated!
    module_name, func = mapping.split(":")
    module = load_module_locally(module_name, working_dir)
    func = getattr(module, func)
    return func


########################
# for inspect command  #
########################


def process_inspect(source_name, mode, limit, merge, logger, do_validate, output=None):
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
    mapping = _map["results"].get("mapping", {})
    type_and_stats = {
        source_name: {
            _mode: btinspect.flatten_and_validate(_map["results"].get(_mode, {}), do_validate)
            for _mode in ["type", "stats"]
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
            mapping_table.add_row(json.dumps(mapping, indent=4, separators=(",", ": "), sort_keys=True))
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
        with open(output, "w+") as fp:
            current_content = fp.read()
            if current_content:
                current_content = json.load(current_content)
            else:
                current_content = {}
            current_content.update(mapping)
            fp.write(json.dumps(current_content, indent=4, separators=(",", ": "), sort_keys=True))
            rprint(f"[green]Successful writing the mapping info to the JSON file: [bold]{output}[/bold][/green]")


def do_inspect(
    plugin_name=None, sub_source_name=None, mode="type,stats", limit=None, merge=False, output=None, logger=None
):
    logger = logger or get_logger(__name__)
    if not limit:
        limit = None
    logger.info(f"Inspect Data plugin {plugin_name} with sub-source name: {sub_source_name} mode: {mode} limit {limit}")

    _plugin = load_plugin(plugin_name)
    # source_full_name = _plugin.plugin_name if sub_source_name else f"{_plugin.plugin_name}.{sub_source_name}"
    if len(_plugin.uploader_classes) > 1 and not sub_source_name:
        rprint("[red]This is a multiple uploaders data plugin, so '--sub-source-name' must be provided![/red]")
        rprint(
            f"[red]Accepted values of --sub-source-name are: {', '.join(uploader.name for uploader in _plugin.uploader_classes)}[/red]"
        )
        raise typer.Exit(code=1)
    # table_space = get_uploaders(pathlib.Path(f"{working_dir}/{plugin_name}"))
    table_space = [item.name for item in _plugin.uploader_classes]
    if sub_source_name and sub_source_name not in table_space:
        rprint(f"[red]Your source name {sub_source_name} does not exits[/red]")
        raise typer.Exit(code=1)
    if sub_source_name:
        process_inspect(sub_source_name, mode, limit, merge, logger, do_validate=True, output=output)
    else:
        for source_name in table_space:
            process_inspect(source_name, mode, limit, merge, logger, do_validate=True, output=output)


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


########################
# for list command     #
########################


def get_uploaders(working_dir: pathlib.Path):
    data_plugin_name = working_dir.name
    manifest = get_manifest_content(working_dir)
    upload_section = manifest.get("uploader")
    table_space = [data_plugin_name]
    if not upload_section:
        upload_sections = manifest.get("uploaders")
        table_space = [item["name"] for item in upload_sections]
    return table_space


def show_dumped_files(data_folder, plugin_name):
    console = Console()
    if not os.path.isdir(data_folder) or not os.listdir(data_folder):
        console.print(
            Panel(
                f"[green]Source:[/green][bold] {plugin_name}[/bold]\n"
                + f"[green]Data Folder:[/green][bold] {data_folder}:[/bold]\n"
                + "Empty file!",
                title="[bold]Dump[/bold]",
                title_align="left",
            )
        )
    else:
        console.print(
            Panel(
                f"[green]Source:[/green][bold] {plugin_name}[/bold]\n"
                + f"[green]Data Folder:[/green][bold] {data_folder}:[/bold]\n    - "
                + "\n    - ".join(os.listdir(data_folder)),
                title="[bold]Dump[/bold]",
                title_align="left",
            )
        )


def get_uploaded_collections(src_db, uploaders):
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


def show_uploaded_sources(working_dir, plugin_name):
    console = Console()
    uploaders = get_uploaders(working_dir)
    src_db = get_src_db()
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


def show_hubdb_content():
    from biothings import config
    from biothings.utils import hub_db

    console = Console()
    hub_db.setup(config)
    coll_list = [hub_db.get_data_plugin(), hub_db.get_src_dump(), hub_db.get_src_master()]
    hub_db_content = "\n".join(
        [
            f"[green]Collection:[/green] [bold]{collection.name}[/bold]\n{json.dumps(collection.find(), indent=4)}"
            for collection in coll_list
        ]
    )
    console.print(
        Panel(
            hub_db_content,
            title="[bold]Hubdb[/bold]",
            title_align="left",
        )
    )


def do_list(plugin_name=None, dump=False, upload=False, hubdb=False, logger=None):
    logger = logger or get_logger(__name__)
    if dump is False and upload is False and hubdb is False:
        # if all set to False, we list both dump and upload as the default
        dump = upload = True

    _plugin = load_plugin(plugin_name, dumper=True, uploader=False)
    if dump:
        data_folder = _plugin.dumper.current_data_folder
        if not data_folder:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.error('Data folder is not available. Please run "dump" first.')
            # Typically we should not need to use new_data_folder as the data_folder,
            # but we keep the code commented out below for future reference
            # utils.run_sync_or_async_job(dumper.create_todump_list, force=True)
            # data_folder = dumper.new_data_folder
        show_dumped_files(data_folder, _plugin.plugin_name)
    if upload:
        show_uploaded_sources(pathlib.Path(_plugin.data_plugin_dir), _plugin.plugin_name)
    if hubdb:
        show_hubdb_content()


def is_valid_working_directory(working_dir, logger=None):
    logger = logger or get_logger(__name__)
    if not os.path.isfile(f"{working_dir}/manifest.yaml") and not os.path.isfile(f"{working_dir}/manifest.json"):
        err = "This command must be run inside a data plugin folder. Please go to a data plugin folder and try again!"
        logger.error(err, extra={"markup": True})
        return False
    return True


########################
# for serve command    #
########################


def serve(host, port, plugin_name, table_space):
    from .web_app import main

    src_db = get_src_db()
    rprint(f"[green]Serving data plugin source: {plugin_name}[/green]")
    asyncio.run(main(host=host, port=port, db=src_db, table_space=table_space))


def do_serve(plugin_name=None, host="localhost", port=9999):
    _plugin = load_plugin(plugin_name)
    uploader_classes = _plugin.uploader_classes
    table_space = [item.name for item in uploader_classes]
    serve(host=host, port=port, plugin_name=_plugin.plugin_name, table_space=table_space)


########################
# for clean command    #
########################


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
    shutil.rmtree(folder_path)


def do_clean_dumped_files(data_folder, plugin_name):
    if not os.path.isdir(data_folder):
        rprint(f"[red]Data folder {data_folder} not found! Nothing has been dumped yet[/red]")
        return
    if not os.listdir(data_folder):
        rprint("[red]Empty folder![/red]")
    else:
        rprint(f"[green]There are all files dumped by [bold]{plugin_name}[/bold]:[/green]")
        print("\n".join(os.listdir(data_folder)))
        delete = typer.confirm("Do you want to delete them?")
        if not delete:
            raise typer.Abort()
        remove_files_in_folder(data_folder)
        rprint("[green]Deleted![/green]")


def do_clean_uploaded_sources(working_dir, plugin_name):
    uploaders = get_uploaders(working_dir)
    src_db = get_src_db()
    uploaded_sources = []
    for item in src_db.collection_names():
        if item in uploaders:
            uploaded_sources.append(item)
        for uploader_name in uploaders:
            if item.startswith(f"{uploader_name}_archive_") or item.startswith(f"{uploader_name}_temp_"):
                uploaded_sources.append(item)
    if not uploaded_sources:
        rprint("[red]No source has been uploaded yet! [/red]")
    else:
        rprint(f"[green]There are all sources uploaded by [bold]{plugin_name}[/bold]:[/green]")
        print("\n".join(uploaded_sources))
        delete = typer.confirm("Do you want to drop them?")
        if not delete:
            raise typer.Abort()
        for source in uploaded_sources:
            src_db[source].drop()
        rprint("[green]All collections are dropped![/green]")


def do_clean(plugin_name=None, dump=False, upload=False, clean_all=False, logger=None):
    logger = logger or get_logger(__name__)
    if clean_all:
        dump = upload = True
    if dump is False and upload is False:
        logger.error("Please provide at least one of following option: --dump, --upload, --all")
        raise typer.Exit(1)

    _plugin = load_plugin(plugin_name, dumper=True, uploader=False)

    if not is_valid_working_directory(_plugin.data_plugin_dir, logger=logger):
        raise typer.Exit(1)
    if dump:
        data_folder = _plugin.dumper.current_data_folder
        if not data_folder:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.error('Data folder is not available. Please run "dump" first.')
        do_clean_dumped_files(data_folder, _plugin._plugin_name)
    if upload:
        do_clean_uploaded_sources(_plugin.data_plugin_dir, _plugin.plugin_name)
