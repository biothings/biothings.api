"""
Operations supported by the cli tooling

Grouped into the following categories

--------------------------------------------------------------------------------
### plugin template creation ###
--------------------------------------------------------------------------------
> def do_create(name, multi_uploaders=False, parallelizer=False):


--------------------------------------------------------------------------------
### data download / upload ###
--------------------------------------------------------------------------------
> async def do_dump(plugin_name=None, show_dumped=True):
> async def do_upload(plugin_name=None, show_uploaded=True):
> async def do_dump_and_upload(plugin_name):
> async def do_list(plugin_name=None, dump=False, upload=False, hubdb=False):
> async def do_index(plugin_name: str):


------------------------------------------------------------------------------------
### data inspection ###
------------------------------------------------------------------------------------

> async def do_inspect(
      plugin_name=None,
      sub_source_name=None,
      mode="type,stats",
      limit=None,
      merge=False,
      output=None,
  ):

--------------------------------------------------------------------------------
### mock web server ###
--------------------------------------------------------------------------------
> async def do_serve(plugin_name=None, host="localhost", port=9999):


--------------------------------------------------------------------------------
### data cleaning ###
--------------------------------------------------------------------------------
> async def do_clean(plugin_name=None, dump=False, upload=False, clean_all=False):


--------------------------------------------------------------------------------
### manifest actions ###
--------------------------------------------------------------------------------
> async def validate_manifest(manifest_file: Union[str, pathlib.Path]):
> async def display_schema():


------------------------------------------------------------------------------------
"""

from typing import Union
import json
import logging
import os
import pathlib
import shutil
import sys
import uuid

from rich import box
from rich.console import Console
from rich.panel import Panel
import jsonschema
import rich
import tornado.template
import typer

from biothings.cli.assistant import CLIAssistant
from biothings.cli.utils import (
    process_inspect,
    run_sync_or_async_job,
    show_dumped_files,
    show_hubdb_content,
    show_uploaded_sources,
    clean_dumped_files,
    clean_uploaded_sources,
)
from biothings.cli.structure import TEMPLATE_DIRECTORY
from biothings.utils.workers import upload_worker


logger = logging.getLogger(name="biothings-cli")


def do_create(name: str, multi_uploaders: bool = False, parallelizer: bool = False):
    """
    Create a new data plugin from the template
    """
    working_directory = pathlib.Path().cwd()
    new_plugin_directory = working_directory.joinpath(name)
    if new_plugin_directory.is_dir():
        logger.error(
            "Data plugin with the same name is already exists. Please remove {new_plugin_directory) before proceeding"
        )
        sys.exit(1)

    shutil.copytree(TEMPLATE_DIRECTORY, new_plugin_directory)

    # create manifest file
    loader = tornado.template.Loader(new_plugin_directory)
    parsed_template = (
        loader.load("manifest.yaml.tpl").generate(multi_uploaders=multi_uploaders, parallelizer=parallelizer).decode()
    )
    manifest_file_path = os.path.join(working_directory, name, "manifest.yaml")
    with open(manifest_file_path, "w", encoding="utf-8") as fh:
        fh.write(parsed_template)

    # remove manifest template
    os.unlink(f"{new_plugin_directory}/manifest.yaml.tpl")
    if not parallelizer:
        os.unlink(f"{new_plugin_directory}/parallelizer.py")
    logger.info(f"Successfully created data plugin template at: \n {new_plugin_directory}")


async def do_dump(plugin_name: str = None, show_dumped: bool = True) -> CLIAssistant:
    """
    Perform dump for the given plugin
    """
    from biothings import config
    from biothings.utils import hub_db

    hub_db.setup(config)
    assistant_instance = CLIAssistant(plugin_name)
    dumper = assistant_instance.get_dumper_class()
    job_manager = assistant_instance.dumper_manager.job_manager
    run_sync_or_async_job(job_manager, dumper.create_todump_list, force=True)

    for item in dumper.to_dump:
        logger.info('Downloading remote data from "%s"...', item["remote"])
        dumper.download(item["remote"], item["local"])
        logger.info('Downloaded locally as "%s"', item["local"])

    dumper.steps = ["post"]
    dumper.post_dump()
    dumper.register_status("success")
    dumper.release_client()

    dp = hub_db.get_data_plugin()
    dp.remove({"_id": assistant_instance.plugin_name})
    data_folder = dumper.new_data_folder

    if show_dumped:
        logger.info("[green]Success![/green] :rocket:", extra={"markup": True})
        show_dumped_files(data_folder, assistant_instance.plugin_name)
    return assistant_instance


async def do_upload(plugin_name: str = None, show_uploaded: bool = True):
    """
    Perform upload for the given list of uploader_classes
    """
    assistant_instance = CLIAssistant(plugin_name)
    uploader_classes = assistant_instance.get_uploader_class()
    for uploader_class in uploader_classes:
        uploader = uploader_class.create(db_conn_info="")
        uploader.make_temp_collection()
        uploader.prepare()
        uploader.update_master()

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
        show_uploaded_sources(pathlib.Path(assistant_instance.plugin_directory), assistant_instance.plugin_name)
    return assistant_instance


async def do_dump_and_upload(plugin_name: str) -> None:
    """
    Perform both dump and upload for the given plugin
    """
    await do_dump(plugin_name, show_dumped=True)
    await do_upload(plugin_name, show_uploaded=True)
    logger.info("[green]Success![/green] :rocket:", extra={"markup": True})


async def do_index(plugin_name: str):
    """
    Creats an elasticsearch data-index for the plugin

    Handles the index configuration generation, source merging
    from the source to the target, and then index generation

    Modified version of the quick_index function call found here:
    biothings/hub/__init__.py
    """
    from biothings import config

    assistant_instance = CLIAssistant(plugin_name)
    assistant_instance.build_manager.configure()
    assistant_instance.build_manager.poll()

    plugin_identifier = uuid.uuid4()
    build_configuration_name = f"{plugin_name}-{plugin_identifier}-configuration"
    build_name = f"{plugin_name}-{plugin_identifier}"
    index_name = build_name.lower()

    build_config_params = {"num_shards": 1, "num_replicas": 0}

    try:
        builder_class = "biothings.hub.databuild.builder.LinkDataBuilder"
        sources = [plugin_name]
        document_type = "temporary"
        assistant_instance.build_manager.create_build_configuration(
            build_configuration_name,
            doc_type=document_type,
            sources=sources,
            builder_class=builder_class,
            params=build_config_params,
        )

        # create a temporary build
        await assistant_instance.build_manager.merge(
            build_name=build_configuration_name,
            target_name=build_name,
            force=True,
            steps=("merge", "metadata"),
        )

        indexer_env = "localhub"
        assistant_instance.index_manager.configure(config.INDEX_CONFIG)
        await assistant_instance.index_manager.index(indexer_env, build_name=build_name, index_name=index_name)
    except Exception as gen_exp:
        raise gen_exp


async def do_list(plugin_name: str = None, dump: bool = True, upload: bool = True, hubdb: bool = False) -> CLIAssistant:
    """
    List the dumped files, uploaded sources, or hubdb content.
    """
    assistant_instance = CLIAssistant(plugin_name)
    if dump:
        dumper_instance = assistant_instance.get_dumper_class()
        data_folder = dumper_instance.current_data_folder
        if not data_folder:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.error('Data folder is not available. Please run "dump" first.')
            # Typically we should not need to use new_data_folder as the data_folder,
            # but we keep the code commented out below for future reference
            # utils.run_sync_or_async_job(dumper.create_todump_list, force=True)
            # data_folder = dumper.new_data_folder
        else:
            show_dumped_files(data_folder, assistant_instance.plugin_name)

    if upload:
        show_uploaded_sources(pathlib.Path(assistant_instance.plugin_directory), assistant_instance.plugin_name)

    if hubdb:
        show_hubdb_content()
    return assistant_instance


async def do_inspect(
    plugin_name: str = None, sub_source_name=None, mode="type,stats", limit=None, merge=False, output=None
):
    """
    Perform inspection on a data plugin.
    """
    if not limit:
        limit = None

    assistant_instance = CLIAssistant(plugin_name)
    uploader_classes = assistant_instance.get_uploader_class()
    if len(uploader_classes) > 1:
        if not sub_source_name:
            logger.error(
                (
                    'This is a multiple uploaders data plugin, so "--sub-source-name" must be provided! '
                    'Accepted values of "--sub-source-name" are: %s',
                    ", ".join(uploader.name for uploader in uploader_classes),
                )
            )
            raise typer.Exit(code=1)
        logger.info(
            'Inspecting data plugin "%s" (sub_source_name="%s", mode="%s", limit=%s)',
            assistant_instance.plugin_name,
            sub_source_name,
            mode,
            limit,
        )
    else:
        logger.info('Inspecting data plugin "%s" (mode="%s", limit=%s)', assistant_instance.plugin_name, mode, limit)

    table_space = [item.name for item in uploader_classes]
    if sub_source_name and sub_source_name not in table_space:
        logger.error('Your source name "%s" does not exits', sub_source_name)
        raise typer.Exit(code=1)
    if sub_source_name:
        process_inspect(sub_source_name, mode, limit, merge, logger=logger, do_validate=True, output=output)
    else:
        for source_name in table_space:
            process_inspect(source_name, mode, limit, merge, logger=logger, do_validate=True, output=output)


async def do_serve(plugin_name: str = None, host: str = "localhost", port: int = 9999):
    """
    Handles creation of a basic web server for hosting files using for a dataplugin
    """
    from biothings import config
    from biothings.utils import hub_db
    from biothings.cli.web_app import main

    assistant_instance = CLIAssistant(plugin_name)
    uploader_classes = assistant_instance.get_uploader_class()
    table_space = [item.name for item in uploader_classes]

    src_db = hub_db.get_src_db()
    rich.print(f"[green]Serving data plugin source: {plugin_name}[/green]")
    await main(host=host, port=port, db=src_db, table_space=table_space)


async def do_clean(plugin_name: str = None, dump: bool = False, upload: bool = False, clean_all: bool = False):
    """
    Clean the dumped files, uploaded sources, or both.
    """
    if clean_all:
        dump = True
        upload = True

    if not dump and not upload:
        logger.error("Please provide at least one of following option: --dump, --upload, --all")
        raise typer.Exit(1)

    assistant_instance = CLIAssistant(plugin_name)

    if dump:
        dumper = assistant_instance.get_dumper_class()
        data_folder = dumper.current_data_folder
        if data_folder:
            clean_dumped_files(data_folder, assistant_instance.plugin_name)
        else:
            # data_folder should be saved in hubdb already, if dump has been done successfully first
            logger.warning('Data folder is not available. Please run "dump" first.')

    if upload:
        clean_uploaded_sources(assistant_instance.plugin_directory, assistant_instance.plugin_name)


async def display_schema():
    """
    Loads the jsonschema definition file and displays it to the
    console
    """
    from biothings.hub.dataplugin.loaders.schema import load_manifest_schema

    manifest_schema = load_manifest_schema()
    schema_validator = jsonschema.validators.validator_for(manifest_schema)
    valid_schema = False
    try:
        schema_validator.check_schema(manifest_schema)
        valid_schema = True
    except jsonschema.exceptions.SchemaError as schema_error:
        logger.exception(schema_error)

    schema_repr = json.dumps(manifest_schema, indent=2)

    console = Console()
    panel = Panel(
        f"* [bold green]Valid Schema[/bold green]: {valid_schema}\n"
        f"* [bold green]Schema Contents[/bold green]:\n{schema_repr}",
        title="[bold green]Biothings JSONSchema Information[/bold green]",
        subtitle="[bold green]Biothings JSONSchema Information[/bold green]",
        box=box.ASCII,
    )
    console.print(panel)


async def validate_manifest(manifest_file: Union[str, pathlib.Path]):
    """
    Loads the manifest file and validates it against the schema file
    If an error exists it will display the error to the enduser
    """
    from biothings.hub.dataplugin.loaders.loader import ManifestBasedPluginLoader

    manifest_file = pathlib.Path(manifest_file).resolve().absolute()
    plugin_name = manifest_file.parent.name
    manifest_loader = ManifestBasedPluginLoader(plugin_name=plugin_name)

    manifest_state = {"path": manifest_file, "valid": False, "repr": None, "error": None}

    try:
        with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
            manifest = json.load(manifest_handle)
    except json.JSONDecodeError as decode_error:
        logger.exception(decode_error)
        manifest_state["error"] = f"{manifest_file} is not valid JSON"

    manifest_state["repr"] = json.dumps(manifest, indent=2)

    try:
        manifest_loader.validate_manifest(manifest)
    except Exception as gen_exc:
        logger.exception(gen_exc)
        manifest_state["error"] = f"{manifest_file} doesn't conform to the schema"
    else:
        manifest_state["valid"] = True

    console = Console()
    panel_message = (
        f"* [bold green]Plugin Name[/bold green]: {plugin_name}\n"
        f"* [bold green]Manifest Path[/bold green]: {manifest_state['path']}\n"
        f"* [bold green]Valid Manifest[/bold green]: {manifest_state['valid']}\n"
    )

    if manifest_state["error"] is not None:
        panel_message += f"* [bold green]Manifest Error[/bold green]: {manifest_state['error']}\n"
    elif manifest_state["error"] is None and manifest_state["repr"] is not None:
        panel_message += f"* [bold green]Manifest Contents[/bold green]:\n{manifest_state['repr']}\n"

    panel = Panel(
        renderable=panel_message,
        title="[bold green]Biothings Manifest Validation[/bold green]",
        subtitle="[bold green]Biothings Manifest Validation[/bold green]",
        box=box.ASCII,
    )
    console.print(panel)
