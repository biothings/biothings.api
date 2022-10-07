import json
import math
import os
import pathlib
import time
from shutil import copytree
from typing import Optional

import tornado.template
import typer

import biothings.utils.inspect as btinspect
from biothings.hub.dataload.dumper import DumperManager
from biothings.hub.dataload.uploader import UploaderManager, upload_worker
from biothings.hub.dataplugin.assistant import LocalAssistant
from biothings.hub.dataplugin.manager import DataPluginManager
from biothings.utils import es
from biothings.utils.common import timesofar
from biothings.utils.dataload import dict_traverse
from biothings.utils.hub_db import get_data_plugin, get_src_db
from biothings.utils.loggers import get_logger

app = typer.Typer()


@app.command("create")
def create_data_plugin(
    name: Optional[str] = typer.Option(  # NOQA: B008
        default="",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
    multi_uploaders: bool = typer.Option(  # NOQA: B008
        False, "--multi-uploaders", help="Add this option if you want to create multiple uploaders"
    ),
    parallelizer: bool = typer.Option(  # NOQA: B008
        False, "--parallelizer", help="Using parallelizer or not? Default: No"
    ),
):
    workspace_dir = pathlib.Path().resolve()
    biothing_source_dir = pathlib.Path(__file__).parent.parent.resolve()
    template_dir = os.path.join(biothing_source_dir, "hub", "dataplugin", "templates")
    plugin_dir = os.path.join(workspace_dir, name)
    if os.path.isdir(plugin_dir):
        print("Data plugin with the same name is already exists, please remove it before create")
        return exit(1)
    copytree(template_dir, plugin_dir)
    # create manifest file
    loader = tornado.template.Loader(plugin_dir)
    parsed_template = (
        loader.load("manifest.yaml.tpl")
        .generate(multi_uploaders=multi_uploaders, parallelizer=parallelizer)
        .decode()
    )
    manifest_file_path = os.path.join(workspace_dir, name, "manifest.yaml")
    with open(manifest_file_path, "w") as fh:
        fh.write(parsed_template)

    # remove manifest template
    os.unlink(f"{plugin_dir}/manifest.yaml.tpl")
    if not parallelizer:
        os.unlink(f"{plugin_dir}/parallelizer.py")
    print(f"Successful create data plugin template at: \n {plugin_dir}")


def load_plugin(plugin_name):
    plugin_manager = DataPluginManager(job_manager=None)
    dmanager = DumperManager(job_manager=None)
    upload_manager = UploaderManager(job_manager=None)

    LocalAssistant.data_plugin_manager = plugin_manager
    LocalAssistant.dumper_manager = dmanager
    LocalAssistant.uploader_manager = upload_manager

    # load pharmgkb data plug,
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


@app.command("test_dump_and_upload")
def test_dump_and_upload(
    plugin_name: Optional[str] = typer.Option(  # NOQA: B008
        default="",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    )
    # multi_uploaders: bool = typer.Option(  # NOQA: B008
    #     False, "--multi-uploaders", help="Add this option if you want to create multiple uploaders"
    # ),
    # parallelizer: bool = typer.Option(  # NOQA: B008
    #     False, "--parallelizer", help="Using parallelizer or not? Default: No"
    # ),
):
    # plugin_name = "pharmgkb"
    # prepare dumper
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

    for uploader_cls in uploader_classes:
        uploader = uploader_cls.create(db_conn_info="")
        uploader.make_temp_collection()
        uploader.prepare()
        upload_worker(
            uploader.fullname,
            uploader.__class__.storage_class,
            uploader.load_data,
            uploader.temp_collection_name,
            1,
            1,
            uploader.data_folder,
            db=uploader.db,
        )
        uploader.switch_collection()

    # cleanup
    dumper.src_dump.remove({"_id": dumper.src_name})
    dp = get_data_plugin()
    dp.remove({"_id": plugin_name})


@app.command("test_inspect")
def test_inspect(
    plugin_name: Optional[str] = typer.Option(  # NOQA: B008
        default="",
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
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
    logger, logfile = get_logger("inspect")
    mode = mode.split(",")
    if "jsonschema" in mode:
        mode = ["jsonschema", "type"]
    if not limit:
        limit = None
    sample = None
    clean = True
    logger.info(
        f"Inspect Data plugin {plugin_name} with sub-source name: {sub_source_name} mode: {mode} limit {limit}"
    )

    t0 = time.time()
    data_provider = ("src", plugin_name)
    source_full_name = source_table_name = plugin_name
    if sub_source_name:
        data_provider = ("src", sub_source_name)
        source_full_name = f"{plugin_name}.{sub_source_name}"
        source_table_name = sub_source_name

    src_db = get_src_db()
    dumper_manager, uploader_manager = load_plugin(plugin_name)
    uploader_cls = uploader_manager[source_full_name][0]
    registerer_obj = uploader_cls.create(db_conn_info="")
    registerer_obj.prepare()

    pre_mapping = "mapping" in mode
    src_cols = src_db[source_table_name]
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
    print(json.dumps(_map, indent=2))
