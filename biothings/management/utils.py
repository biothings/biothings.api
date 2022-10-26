import asyncio
import importlib
import json
import math
import os
import pathlib
import sys
import time
from ftplib import FTP
from functools import partial
from urllib import parse as urlparse

import requests
import typer
from orjson import orjson

import biothings.utils.inspect as btinspect
from biothings import config
from biothings.utils import es, storage
from biothings.utils.common import get_random_string, get_timestamp, timesofar, uncompressall
from biothings.utils.dataload import dict_traverse
from biothings.utils.sqlite3 import get_src_db
from biothings.utils.workers import upload_worker

app = typer.Typer()

logger = config.logger


def get_todump_list(dumper_section):
    workspace_dir = pathlib.Path().resolve()
    data_folder = os.path.join(workspace_dir, ".biothings_hub", "data_folder")
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


def download(schema, remote_url, local_file, uncompress=True):
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
        logger.debug("Downloading '%s' as '%s'" % (remote_url, local_file))
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


#
# def prepare(*args, **kwargs):
#     conn = get_src_conn()
#     db = conn[self.__class__.__database__]
#     self._state["collection"] = self._state["db"][self.collection_name]
#     self._state["src_dump"] = self.prepare_src_dump()
#     self._state["src_master"] = get_src_master()
#     self._state["logger"], self.logfile = self.setup_log()
#     self.data_folder = self.src_doc.get("download", {}).get(
#         "data_folder") or self.src_doc.get(
#         "data_folder"
#     )


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


def get_load_data_func(workspace_dir, parser, **kwargs):
    sys.path.insert(1, workspace_dir)
    mod_name, func = parser.split(":")
    mod = importlib.import_module(mod_name)
    parser_func = getattr(mod, func)
    return partial(parser_func, **kwargs)


def get_custom_mapping_func(workspace_dir, mapping):
    sys.path.insert(1, workspace_dir)
    mod_name, func = mapping.split(":")
    mod = importlib.import_module(mod_name)
    mapping_func = getattr(mod, func)
    return mapping_func


def process_uploader(workspace_dir, data_folder, main_source, upload_section):
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
    load_data_func = get_load_data_func(workspace_dir, parser, **parser_kwargs_serialized)
    # TODO
    # if mapping:
    #     mapping_func = get_custom_mapping_func(workspace_dir, mapping)
    upload_worker(
        uploader_fullname,
        storage_class,
        load_data_func,
        temp_collection_name,
        1,
        1,
        data_folder,
        db=src_db,
    )
    switch_collection(
        src_db,
        temp_collection_name=temp_collection_name,
        collection_name=uploader_fullname,
        logger=logger,
    )


def process_inspect(plugin_name, sub_source_name, mode, limit, merge):
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
    source_table_name = plugin_name
    if sub_source_name:
        data_provider = ("src", sub_source_name)
        source_table_name = sub_source_name

    src_db = get_src_db()
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


def serve(port, plugin_name, table_space):
    from .app import main

    src_db = get_src_db()
    print(f"Serving data plugin source {plugin_name} on port http://127.0.0.1:{port}")
    asyncio.run(main(port=port, db=src_db, table_space=table_space))
