"""
Operations supported by the cli tooling

Grouped into the following categories

------------------------------------------------------------------------------------
> plugin template creation

> def do_create(name, multi_uploaders=False, parallelizer=False, logger=None):
------------------------------------------------------------------------------------

------------------------------------------------------------------------------------
> data download / upload

> def do_dump(plugin_name=None, show_dumped=True, logger=None):
> def do_upload(plugin_name=None, show_uploaded=True, logger=None):
> def do_dump_and_upload(plugin_name, logger=None):
> def do_list(plugin_name=None, dump=False, upload=False, hubdb=False, logger=None):
------------------------------------------------------------------------------------

------------------------------------------------------------------------------------
> data inspection

> def do_inspect(
      plugin_name=None,
      sub_source_name=None,
      mode="type,stats",
      limit=None,
      merge=False,
      output=None,
      logger=None
  ):
------------------------------------------------------------------------------------

------------------------------------------------------------------------------------
> mock web server 

> def do_serve(plugin_name=None, host="localhost", port=9999, logger=None):
------------------------------------------------------------------------------------

------------------------------------------------------------------------------------
> data cleaning

> def do_clean_dumped_files(data_folder, plugin_name):
> def do_clean_uploaded_sources(working_dir, plugin_name):
> def do_clean(plugin_name=None, dump=False, upload=False, clean_all=False, logger=None):
------------------------------------------------------------------------------------
"""

import asyncio
import logging
import math
import os
import pathlib
import shutil
import sys
import time
from pprint import pformat
from types import SimpleNamespace
from typing import Union

import tornado.template
import typer
import yaml
from rich import box, print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from biothings.utils import es
from biothings.utils.common import timesofar
from biothings.utils.dataload import dict_traverse
from biothings.utils.serializer import load_json, to_json
from biothings.utils.workers import upload_worker
import biothings.utils.inspect as btinspect
from biothings.cli.utils import (
    get_uploaders,
    load_plugin,
    process_inspect,
    remove_files_in_folder,
    run_sync_or_async_job,
    serve,
    show_dumped_files,
    show_hubdb_content,
    show_uploaded_sources,
)


logger = logging.getLogger(name="biothings-cli")


########################
# for create command   #
########################
def do_create(name, multi_uploaders=False, parallelizer=False, logger=None):
    """
    Create a new data plugin from the template
    """
    working_dir = pathlib.Path().resolve()
    biothing_source_dir = pathlib.Path(__file__).parent.parent.resolve()
    template_dir = os.path.join(biothing_source_dir, "hub", "dataplugin", "templates")
    plugin_dir = os.path.join(working_dir, name)
    if os.path.isdir(plugin_dir):
        logger.error("Data plugin with the same name is already exists, please remove it before create")
        sys.exit(1)
    shutil.copytree(template_dir, plugin_dir)

    # create manifest file
    loader = tornado.template.Loader(plugin_dir)
    parsed_template = (
        loader.load("manifest.yaml.tpl").generate(multi_uploaders=multi_uploaders, parallelizer=parallelizer).decode()
    )
    manifest_file_path = os.path.join(working_dir, name, "manifest.yaml")
    with open(manifest_file_path, "w", encoding="utf-8") as fh:
        fh.write(parsed_template)

    # remove manifest template
    os.unlink(f"{plugin_dir}/manifest.yaml.tpl")
    if not parallelizer:
        os.unlink(f"{plugin_dir}/parallelizer.py")
    logger.info(f"Successfully created data plugin template at: \n {plugin_dir}")


###############################
# for dump & upload command   #
###############################


def do_dump(plugin_name=None, show_dumped=True, logger=None):
    """
    Perform dump for the given plugin
    """
    from biothings import config
    from biothings.utils import hub_db

    hub_db.setup(config)
    _plugin = load_plugin(plugin_name, dumper=True, uploader=False, logger=logger)
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
    """
    Perform upload for the given list of uploader_classes
    """
    _plugin = load_plugin(plugin_name, dumper=False, uploader=True, logger=logger)
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
    """
    Perform both dump and upload for the given plugin
    """
    _plugin = do_dump(plugin_name, show_dumped=False, logger=logger)
    do_upload(plugin_name, show_uploaded=False, logger=logger)
    logger.info("[green]Success![/green] :rocket:", extra={"markup": True})
    show_dumped_files(_plugin.dumper.new_data_folder, _plugin.plugin_name)
    show_uploaded_sources(pathlib.Path(_plugin.data_plugin_dir), _plugin.plugin_name)


def do_list(plugin_name=None, dump=True, upload=True, hubdb=False, logger=None):
    """
    List the dumped files, uploaded sources, or hubdb content.
    """
    _plugin = load_plugin(plugin_name, dumper=True, uploader=False, logger=logger)
    if dump:
        data_folder = _plugin.dumper.current_data_folder
        if not data_folder:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.error('Data folder is not available. Please run "dump" first.')
            # Typically we should not need to use new_data_folder as the data_folder,
            # but we keep the code commented out below for future reference
            # utils.run_sync_or_async_job(dumper.create_todump_list, force=True)
            # data_folder = dumper.new_data_folder
        else:
            show_dumped_files(data_folder, _plugin.plugin_name)
    if upload:
        show_uploaded_sources(pathlib.Path(_plugin.data_plugin_dir), _plugin.plugin_name)
    if hubdb:
        show_hubdb_content()


########################
# for inspect command  #
########################
def do_inspect(
    plugin_name=None, sub_source_name=None, mode="type,stats", limit=None, merge=False, output=None, logger=None
):
    """
    Perform inspection on a data plugin.
    """
    if not limit:
        limit = None

    _plugin = load_plugin(plugin_name, logger=logger)
    # source_full_name = _plugin.plugin_name if sub_source_name else f"{_plugin.plugin_name}.{sub_source_name}"
    if len(_plugin.uploader_classes) > 1:
        if not sub_source_name:
            logger.error('This is a multiple uploaders data plugin, so "--sub-source-name" must be provided!')
            logger.error(
                'Accepted values of "--sub-source-name" are: %s',
                ", ".join(uploader.name for uploader in _plugin.uploader_classes),
            )
            raise typer.Exit(code=1)
        logger.info(
            'Inspecting data plugin "%s" (sub_source_name="%s", mode="%s", limit=%s)',
            _plugin.plugin_name,
            sub_source_name,
            mode,
            limit,
        )
    else:
        logger.info('Inspecting data plugin "%s" (mode="%s", limit=%s)', _plugin.plugin_name, mode, limit)
    # table_space = get_uploaders(pathlib.Path(f"{working_dir}/{plugin_name}"))
    table_space = [item.name for item in _plugin.uploader_classes]
    if sub_source_name and sub_source_name not in table_space:
        logger.error('Your source name "%s" does not exits', sub_source_name)
        raise typer.Exit(code=1)
    if sub_source_name:
        process_inspect(sub_source_name, mode, limit, merge, logger=logger, do_validate=True, output=output)
    else:
        for source_name in table_space:
            process_inspect(source_name, mode, limit, merge, logger=logger, do_validate=True, output=output)


########################
# for serve command    #
########################


def do_serve(plugin_name=None, host="localhost", port=9999, logger=None):
    _plugin = load_plugin(plugin_name, dumper=False, uploader=True, logger=logger)
    uploader_classes = _plugin.uploader_classes
    table_space = [item.name for item in uploader_classes]
    serve(host=host, port=port, plugin_name=_plugin.plugin_name, table_space=table_space)


########################
# for clean command    #
########################


def do_clean_dumped_files(data_folder, plugin_name):
    """
    Remove all dumped files by a data plugin in the data folder.
    """
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
    """
    Clean the dumped files, uploaded sources, or both.
    """
    if clean_all:
        dump = upload = True
    if dump is False and upload is False:
        logger.error("Please provide at least one of following option: --dump, --upload, --all")
        raise typer.Exit(1)

    _plugin = load_plugin(plugin_name, dumper=True, uploader=False, logger=logger)

    if dump:
        data_folder = _plugin.dumper.current_data_folder
        if not data_folder:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.error('Data folder is not available. Please run "dump" first.')
        do_clean_dumped_files(data_folder, _plugin.plugin_name)
    if upload:
        do_clean_uploaded_sources(_plugin.data_plugin_dir, _plugin.plugin_name)
