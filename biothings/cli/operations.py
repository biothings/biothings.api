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
> async def do_dump
> async def do_upload
> async def do_parallel_upload
> async def do_dump_and_upload
> async def do_list

--------------------------------------------------------------------------------
### data build / index ###
--------------------------------------------------------------------------------
> async def do_index

------------------------------------------------------------------------------------
### data inspection ###
------------------------------------------------------------------------------------
> async def do_inspect

--------------------------------------------------------------------------------
### mock web server ###
--------------------------------------------------------------------------------
> async def do_serve

--------------------------------------------------------------------------------
### data cleaning ###
--------------------------------------------------------------------------------
> async def do_clean

--------------------------------------------------------------------------------
### manifest actions ###
--------------------------------------------------------------------------------
> async def validate_manifest
> async def display_schema

------------------------------------------------------------------------------------
"""

import asyncio
import functools
import logging
import multiprocessing
import os
import pathlib
import platform
import random
import shutil
import sys
import uuid
from typing import Callable, Optional, Union

import jsonschema
import rich
import tornado.template
import typer
from rich import box
from rich.console import Console
from rich.panel import Panel

from biothings.cli.exceptions import MissingPluginName, UnknownUploaderSource
from biothings.cli.structure import TEMPLATE_DIRECTORY
from biothings.cli.utils import (
    clean_dumped_files,
    clean_uploaded_sources,
    display_inspection_table,
    process_inspect,
    show_dumped_files,
    show_hubdb_content,
    show_source_build,
    show_source_index,
    show_uploaded_sources,
    write_mapping_to_file,
)
from biothings.utils.serializer import JSONDecodeError, load_json, to_json
from biothings.utils.workers import upload_worker

logger = logging.getLogger(name="biothings-cli")


def operation_mode(operation_method: Callable):
    """
    Based off the directory structure for where the biothings-cli
    was invoked we set the "mode" to one of two states:

    0) singular
    The current working directory contains a singular data-plugin

    In this case we don't require a plugin_name argument to be passed
    at the command-line

    1) hub
    The current working directory contains N directories operating as a
    "hub" or collection of data-plugins under one umbrella

    In this case we do require a plugin_name argument to be passed
    at the command-line. Otherwise we have no idea which data-plugin to
    refer to

    We attempt to load the plugin from this working directory. If we sucessfully load
    either a manifest or advanced plugin, then we can safely say this is a singular
    dataplugin

    If we cannot load either a manifest or advanced plugin then we default assume that
    the mode is hub
    """

    @functools.wraps(operation_method)
    def determine_operation_mode(*args, **kwargs):
        working_directory = pathlib.Path.cwd()
        working_directory_files = {file.name for file in working_directory.iterdir()}

        mode = None
        if "manifest.json" in working_directory_files or "manifest.yaml" in working_directory_files:
            logger.debug("Inferring singular manifest plugin from directory structure")
            mode = "SINGULAR"
        elif "__init__.py" in working_directory_files:
            logger.debug("Inferring singular advanced plugin from directory structure")
            mode = "SINGULAR"
        else:
            logger.debug("Inferring multiple plugins from directory structure")
            mode = "HUB"

        if mode == "SINGULAR":
            if kwargs.get("plugin_name", None) is not None:
                kwargs["plugin_name"] = None
        elif mode == "HUB":
            if kwargs.get("plugin_name", None) is None:
                raise MissingPluginName(working_directory)

        operation_result = operation_method(*args, **kwargs)
        return operation_result

    return determine_operation_mode


# do not apply operation_mode decorator since this operation means to create a new plugin
# regardless what the current working directory has
# @operation_mode
def do_create(plugin_name: str, multi_uploaders: bool = False, parallelizer: bool = False):
    """
    Create a new data plugin from the template
    """
    working_directory = pathlib.Path().cwd()
    new_plugin_directory = working_directory.joinpath(plugin_name)
    if new_plugin_directory.is_dir():
        logger.error(
            (
                "Data plugin with the same name is already exists. "
                f"Please remove {new_plugin_directory} before proceeding"
            )
        )
        sys.exit(1)

    shutil.copytree(TEMPLATE_DIRECTORY, new_plugin_directory)

    # create manifest file
    loader = tornado.template.Loader(new_plugin_directory)
    manifest_template = loader.load("manifest.yaml.tpl")
    populated_manifest = manifest_template.generate(multi_uploaders=multi_uploaders, parallelizer=parallelizer).decode()
    manifest_file_path = os.path.join(working_directory, plugin_name, "manifest.yaml")
    with open(manifest_file_path, "w", encoding="utf-8") as fh:
        fh.write(populated_manifest)

    # remove manifest template
    os.unlink(f"{new_plugin_directory}/manifest.yaml.tpl")
    if not parallelizer:
        os.unlink(f"{new_plugin_directory}/parallelizer.py")
    logger.info("Successfully created data plugin template at: %s\n", new_plugin_directory)


@operation_mode
async def do_dump(plugin_name: Optional[str] = None, show_dumped: bool = True) -> None:
    """
    Perform dump for the given plugin
    """
    from biothings import config
    from biothings.cli.assistant import CLIAssistant
    from biothings.utils import hub_db

    hub_db.setup(config)
    assistant_instance = CLIAssistant(plugin_name)
    dumper_class = assistant_instance.dumper_manager[assistant_instance.plugin_name][0]
    dumper_instance = assistant_instance.dumper_manager.create_instance(dumper_class)
    dumper_instance.__class__.AUTO_UPLOAD = False

    if dumper_instance.need_prepare():
        dumper_instance.prepare()

    try:
        dumper_instance.set_release()
    except AttributeError:
        attribute_warning = (
            "Unable to call `set_release` for dumper instance [%s]. "
            "This is likely because the dumper instance does not support this method",
            dumper_instance,
        )
        logger.warning(attribute_warning)

    dump_job = dumper_instance.dump(
        job_manager=assistant_instance.job_manager,
        force=False,
    )
    await asyncio.gather(dump_job)

    dp = hub_db.get_data_plugin()
    dp.remove({"_id": assistant_instance.plugin_name})

    dumper_instance.register_status(status="success")
    logger.info("[green]Success![/green] :rocket:", extra={"markup": True})

    if show_dumped:
        data_folder = dumper_instance.current_data_folder
        show_dumped_files(data_folder, assistant_instance.plugin_name)


@operation_mode
async def do_upload(plugin_name: Optional[str] = None, batch_limit: int = 10000, show_uploaded: bool = True) -> None:
    """
    Perform upload for the given list of uploader_classes

    The callback in the hub leverages the `upload_src` method

    >>> if self.managers.get("upload_manager"):
    >>>     self.commands["upload"] = self.managers["upload_manager"].upload_src
    >>>     self.commands["upload_all"] = self.managers["upload_manager"].upload_all
    >>>     self.commands["update_source_meta"] = self.managers["upload_manager"].update_source_meta
    """
    from biothings.cli.assistant import CLIAssistant

    assistant_instance = CLIAssistant(plugin_name)
    uploader_classes = assistant_instance.get_uploader_class()
    for uploader_class in uploader_classes:
        uploader = uploader_class.create(db_conn_info="")
        uploader.register_status("uploading")
        uploader.make_temp_collection()
        uploader.prepare_src_dump()
        uploader.prepare()
        uploader.update_master()

        if not uploader.data_folder or not pathlib.Path(uploader.data_folder).exists():
            uploader_error_message = (
                "Data folder '%s' for '%s' is empty or does not exist yet. "
                "Please ensure you have run `biothings-cli dataplugin dump`"
            )
            logger.error(uploader_error_message, uploader.data_folder, uploader.fullname)
            raise typer.Exit(1)

        upload_worker(
            uploader.fullname,
            uploader.__class__.storage_class,
            uploader.load_data,
            uploader.temp_collection_name,
            batch_limit,
            1,
            uploader.data_folder,
            db=uploader.db,
        )
        uploader.switch_collection()
        uploader.keep_archive = 3  # keep 3 archived collections, that's probably good enough for CLI, default is 10
        uploader.clean_archived_collections()

        document_count = uploader.db[uploader.collection_name].count()
        uploader.register_status("success", count=document_count, err=None, tb=None)

    if show_uploaded:
        logger.info("[green]Success![/green] :rocket:", extra={"markup": True, "notify": True})
        show_uploaded_sources(pathlib.Path(assistant_instance.plugin_directory), assistant_instance.plugin_name)


@operation_mode
async def do_parallel_upload(
    plugin_name: Optional[str] = None, batch_limit: int = 10000, show_uploaded: bool = True
) -> None:
    """
    Perform upload for the given list of uploader_classes

    The callback in the hub leverages the `upload_src` method

    >>> if self.managers.get("upload_manager"):
    >>>     self.commands["upload"] = self.managers["upload_manager"].upload_src
    >>>     self.commands["upload_all"] = self.managers["upload_manager"].upload_all
    >>>     self.commands["update_source_meta"] = self.managers["upload_manager"].update_source_meta

    This is a modified version of the ParallelUploader `update_data` source call
    """
    from biothings.cli.assistant import CLIAssistant

    assistant_instance = CLIAssistant(plugin_name)
    uploader_classes = assistant_instance.get_uploader_class()
    for uploader_class in uploader_classes:
        uploader = uploader_class.create(db_conn_info="")
        uploader.make_temp_collection()
        uploader.prepare()
        uploader.update_master()

        if not uploader.data_folder or not pathlib.Path(uploader.data_folder).exists():
            uploader_error_message = (
                "Data folder '%s' for '%s' is empty or does not exist yet. "
                "Please ensure you have run `biothings-cli dataplugin dump`"
            )
            logger.error(uploader_error_message, uploader.data_folder, uploader.fullname)
            raise typer.Exit(1)

        job_parameters = uploader.jobs()
        jobs = []
        job_manager = assistant_instance.dumper_manager.job_manager
        uploader.unprepare()
        for batch_number, data_load_arguments in enumerate(job_parameters):
            pinfo = uploader.get_pinfo()
            pinfo["step"] = "update_data"
            pinfo["description"] = f"{data_load_arguments}"
            job = await job_manager.defer_to_process(
                pinfo,
                upload_worker,
                uploader.fullname,  # worker name
                uploader.storage_class,  # storage class
                uploader.load_data,  # loading function
                uploader.temp_collection_name,  # destination collection name
                batch_limit,  # batch size
                batch_number,  # batch number
                *data_load_arguments,  # loading function arguments
                # db=uploader.db,
            )
            jobs.append(job)
        await asyncio.gather(*jobs)
        uploader.switch_collection()

        # keep 3 archived collections, good enough for CLI, default is 10
        uploader.keep_archive = 3
        uploader.clean_archived_collections()

    if show_uploaded:
        logger.info("[green]Success![/green] :rocket:", extra={"markup": True})
        show_uploaded_sources(pathlib.Path(assistant_instance.plugin_directory), assistant_instance.plugin_name)


@operation_mode
async def do_dump_and_upload(plugin_name: str) -> None:
    """
    Perform both dump and upload for the given plugin
    """
    await do_dump(plugin_name=plugin_name, show_dumped=True)
    await do_upload(plugin_name=plugin_name, show_uploaded=True)
    logger.info("[green]Success![/green] :rocket:", extra={"markup": True})


@operation_mode
async def do_index(plugin_name: Optional[str] = None, sub_source_name: Optional[str] = None) -> None:
    """
    Creats an elasticsearch data-index for the plugin

    Handles the index configuration generation, source merging
    from the source to the target, and then index generation

    Modified version of the quick_index function call found here:
    biothings/hub/__init__.py


    Control Flow:
    This was originally a port of the quick-index function provided in the biothings-hub,
    but we've added additional checks for increased user functionality

    0) We create a build configuration. This constitutes a call to the hubdb.get_src_build_config
    and then update/inserting it into our database.

    For sqlite3, this table is located in the biothings_hubdb file under the src_build_config
    table

    For MongoDB
    │ - Database: data_src_database     │
    │     - Collections:                │
    │         occurrences               │
    │         druglabels                │
    │         annotations               │

    The record stored will look like the following:
    tutorials-9bb210ef-8f96-4dc2-9e1c-f42e53ea23b9-configuration |
    {
        "_id": "tutorials-9bb210ef-8f96-4dc2-9e1c-f42e53ea23b9-configuration",
        "name": "tutorials-9bb210ef-8f96-4dc2-9e1c-f42e53ea23b9-configuration",
        "doc_type": "temporary",
        "sources": ["tutorials"],
        "root": [], "builder_class":
        "biothings.hub.databuild.builder.LinkDataBuilder",
        "num_shards": 1,
        "num_replicas": 0
    }

    1) After creating our build configuration we have to verify that the provided source exists
    in the source_backend master documents. For single source data plugins, this will likely match
    the plugin_name and isn't an issue. But for multiple datasources in one plugin, we have to be
    explicit about the source we want to index. So we verify that either the plugin_name or
    subsource name is found in the source backend master documents before proceeding

    2) We then attempt to load the elasticsearch mapping. There are a split of dataplugins
    that provide the elasticsearch mapping and a split that do not. If a hardcoded mapping
    is not provided, then we attempt to provide a mapping dynamically through our inspection
    operation

    3) We then create a build through the merge operation. Due to limitations and the fact that the
    command-line we meant to examine a singular source at a time, it's limited to a LinkDataBuilder
    as we aren't merging multiple data sources together and have a singular datasource in the build

    4) Lastly is actually creating the new elasticsearch index from the build. This does require
    elasticsearch to be running at the specified address in the settings.py file or configuration.
    The default location is localhost:9200. If successful a couple frames detailing the build and
    index information will be displayed to the enduser
    """
    from biothings import config
    from biothings.cli.assistant import CLIAssistant
    from biothings.hub.databuild.builder import BuilderException
    from biothings.utils.manager import JobManager

    if platform.system() == "Windows":
        logger.warning("The `biothings-cli dataplugin index` command isn't supported on windows")
        raise typer.Exit(code=2)

    if config.HUB_DB_BACKEND["module"] == "biothings.utils.sqlite3":
        logger.warning(
            "The `biothings-cli dataplugin index` command only supports MongoDB as the HUB_DB_BACKEND. "
            "Please setup MongoDB locally and change the configuration to use the following to continue: \n%s",
            to_json({"module": "biothings.utils.mongo", "uri": "mongodb://localhost:27017"}, indent=True),
        )
        raise typer.Exit(code=2)

    logger.debug('Forcing the multiprocessing start method to "fork"')
    multiprocessing.set_start_method("fork")

    index_job_manager = JobManager(
        loop=asyncio.get_running_loop(),
        process_queue=None,
        thread_queue=None,
        max_memory_usage=None,
        num_workers=os.cpu_count(),
        num_threads=16,
        auto_recycle=True,
    )

    assistant_instance = CLIAssistant(plugin_name, job_manager=index_job_manager)
    assistant_instance.build_manager.configure()
    uploader_manager = assistant_instance.uploader_manager

    valid_sources = [uploader.name for uploader in uploader_manager[assistant_instance.plugin_name]]
    if len(valid_sources) > 1 and sub_source_name is None or sub_source_name not in valid_sources:
        logger.error(
            (
                "Multiple uploaders exist for %s data-plugin, so --sub-source-name (-s) "
                "must be provided to specify which source you want to index.\n"
                "\t>>> Supported values for --sub-source-name: %s\n"
                "\t>>> Example: `biothings-cli dataplugin index --plugin-name %s --sub-source-name %s"
            ),
            assistant_instance.plugin_name,
            valid_sources,
            assistant_instance.plugin_name,
            random.choice(valid_sources),
        )
        raise typer.Exit(code=2)

    plugin_identifier = uuid.uuid4()
    build_configuration_name = f"{assistant_instance.plugin_name}-{plugin_identifier}-configuration"
    build_name = f"{assistant_instance.plugin_name}-{plugin_identifier}"
    index_name = build_name.lower()

    build_config_params = {"num_shards": 1, "num_replicas": 0}

    builder_class = "biothings.hub.databuild.builder.LinkDataBuilder"
    sources = [assistant_instance.plugin_name]
    if sub_source_name is not None:
        sources = [sub_source_name]

    document_type = "temporary"
    assistant_instance.build_manager.create_build_configuration(
        build_configuration_name,
        doc_type=document_type,
        sources=sources,
        builder_class=builder_class,
        params=build_config_params,
    )

    # validate available sources before continuing
    data_builder = assistant_instance.build_manager[build_configuration_name]
    elasticsearch_mapping = None
    try:
        elasticsearch_mapping = data_builder.get_mapping(sources)
    except BuilderException as build_exc:
        logger.info("No registered mapping found. Auto-generating mapping for source(s) %s", sources)
        generated_mapping = process_inspect(
            source_name=sources[0],
            mode="mapping",
            limit=None,
            merge=False,
        )
        elasticsearch_mapping = generated_mapping["results"]["mapping"]

        discovered_source_uploader = False
        for uploader_class in uploader_manager[assistant_instance.plugin_name]:
            if uploader_class.name == sources[0]:
                plugin_uploader = uploader_class(db_conn_info="")
                plugin_uploader.prepare()
                plugin_uploader.update_master()
                master_document = plugin_uploader.generate_doc_src_master()
                master_document["mapping"] = elasticsearch_mapping
                plugin_uploader.save_doc_src_master(master_document)
                discovered_source_uploader = True
                break

        if not discovered_source_uploader:
            missing_source_uploader_message = (
                f"Unable to find the source uploader corresponding to {sources[0]}.\n"
                f"\t>>> Discovered the following uploaders from plugin manager: {valid_sources}"
            )
            raise UnknownUploaderSource(missing_source_uploader_message) from build_exc

    # create a temporary build
    merge_job = assistant_instance.build_manager.merge(
        build_name=build_configuration_name,
        target_name=build_name,
        sources=sources,
        force=True,
        steps=("merge", "metadata"),
    )
    await asyncio.gather(merge_job)

    indexer_env = "commandhub"
    assistant_instance.index_manager.configure(config.INDEX_CONFIG)
    index_job = assistant_instance.index_manager.index(indexer_env, build_name=build_name, index_name=index_name)
    await asyncio.gather(index_job)

    show_source_build(data_builder, build_configuration_name)
    await show_source_index(index_name, assistant_instance.index_manager, elasticsearch_mapping)


@operation_mode
async def do_list(
    plugin_name: Optional[str] = None, dump: bool = True, upload: bool = True, hubdb: bool = False
) -> None:
    """
    List the dumped files, uploaded sources, or hubdb content.
    """
    from biothings.cli.assistant import CLIAssistant

    assistant_instance = CLIAssistant(plugin_name)
    if dump:
        dumper_instance = assistant_instance.get_dumper_class()
        data_folder = dumper_instance.current_data_folder
        if not data_folder:
            missing_data_folder_message = (
                "Unable to list the dumped data files as the data folder is not available from the dumper instance. "
                "It may have already been deleted or the command `biothings-cli dataplugin dump` was never run. "
            )
            logger.warning(missing_data_folder_message)
        else:
            show_dumped_files(data_folder, assistant_instance.plugin_name)

    if upload:
        show_uploaded_sources(pathlib.Path(assistant_instance.plugin_directory), assistant_instance.plugin_name)

    if hubdb:
        show_hubdb_content()


@operation_mode
async def do_inspect(
    plugin_name: Optional[str] = None,
    sub_source_name: Optional[str] = None,
    mode: str = "type,stats",
    limit: Optional[int] = None,
    merge: bool = False,
    output: Optional[Union[str, pathlib.Path]] = None,
):
    """
    Perform inspection on a data plugin.
    """
    from biothings.cli.assistant import CLIAssistant

    assistant_instance = CLIAssistant(plugin_name)
    uploader_classes = assistant_instance.get_uploader_class()

    valid_sources = [uploader.name for uploader in uploader_classes]
    if len(valid_sources) > 1 and sub_source_name is None or sub_source_name not in valid_sources:
        logger.error(
            (
                "Multiple uploaders exist for %s data-plugin, so --sub-source-name (-s) "
                "must be provided to specify which source you want to inspect.\n"
                "\t>>> Supported values for --sub-source-name: %s\n"
                "\t>>> Example: `biothings-cli dataplugin inspect --plugin-name %s --sub-source-name %s"
            ),
            assistant_instance.plugin_name,
            valid_sources,
            assistant_instance.plugin_name,
            random.choice(valid_sources),
        )
        raise typer.Exit(code=2)

    logger.info(
        'Inspecting data plugin "%s" (sub_source_name="%s", mode="%s", limit=%s)',
        assistant_instance.plugin_name,
        sub_source_name,
        mode,
        limit,
    )

    table_space = [item.name for item in uploader_classes]
    if sub_source_name and sub_source_name not in table_space:
        logger.error('Your source name "%s" does not exits', sub_source_name)
        raise typer.Exit(code=1)

    if sub_source_name:
        inspection_mapping = process_inspect(sub_source_name, mode, limit, merge)
        display_inspection_table(
            source_name=sub_source_name, mode=mode, inspection_mapping=inspection_mapping, validate=True
        )
        if output is not None:
            write_mapping_to_file(output, inspection_mapping)
    else:
        for source_index, source_name in enumerate(table_space):
            inspection_mapping = process_inspect(source_name, mode, limit, merge)
            display_inspection_table(
                source_name=source_name, mode=mode, inspection_mapping=inspection_mapping, validate=True
            )
            if output is not None:
                sub_output = f"{output}{source_index}"
                write_mapping_to_file(sub_output, inspection_mapping)


@operation_mode
async def do_serve(plugin_name: Optional[str] = None, host: str = "localhost", port: int = 9999):
    """
    Handles creation of a basic web server for hosting files using for a dataplugin
    """
    from biothings.cli.assistant import CLIAssistant
    from biothings.cli.web_app import main
    from biothings.utils import hub_db

    assistant_instance = CLIAssistant(plugin_name)
    uploader_classes = assistant_instance.get_uploader_class()
    table_space = [item.name for item in uploader_classes]

    src_db = hub_db.get_src_db()
    rich.print(f"[green]Serving data plugin source: {assistant_instance.plugin_name}[/green]")
    await main(host=host, port=port, db=src_db, table_space=table_space)


@operation_mode
async def do_clean(
    plugin_name: Optional[str] = None, dump: bool = False, upload: bool = False, clean_all: bool = False
):
    """
    Clean the dumped files, uploaded sources, or both.
    """
    from biothings.cli.assistant import CLIAssistant

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
    except jsonschema.SchemaError as schema_error:
        logger.exception(schema_error)

    schema_repr = to_json(manifest_schema, indent=True)

    console = Console()
    panel = Panel(
        f"* [bold green]Valid Schema[/bold green]: {valid_schema}\n"
        f"* [bold green]Schema Contents[/bold green]:\n{schema_repr}",
        title="[white]Biothings JSONSchema Information[/white]",
        title_align="left",
        subtitle="[white]Biothings JSONSchema Information[/white]",
        subtitle_align="left",
        box=box.ROUNDED,
    )
    console.print(panel)


@operation_mode
async def validate_manifest(plugin_name: Optional[str] = None):
    """
    Loads the manifest file and validates it against the schema file
    If an error exists it will display the error to the enduser
    """
    from biothings.hub.dataplugin.loaders.loader import ManifestBasedPluginLoader

    if plugin_name is None:
        plugin_directory = pathlib.Path.cwd().resolve().absolute()
        plugin_name = plugin_directory.name
        manifest_file = plugin_directory.joinpath("manifest.json")
    else:
        calling_directory = pathlib.Path.cwd().resolve().absolute()
        plugin_directory = calling_directory.joinpath(plugin_name)
        manifest_file = plugin_directory.joinpath("manifest.json")

    manifest_loader = ManifestBasedPluginLoader(plugin_name=plugin_name)
    manifest_state = {"path": manifest_file, "valid": False, "repr": None, "error": None}

    try:
        with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
            manifest = load_json(manifest_handle.read())
    except JSONDecodeError as decode_error:
        logger.exception(decode_error)
        manifest_state["error"] = f"{manifest_file} is not valid JSON"
        return

    manifest_state["repr"] = to_json(manifest, indent=True)

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
        title="[white]Biothings Manifest Validation[/white]",
        title_align="left",
        subtitle="[white]Biothings Manifest Validation[/white]",
        subtitle_align="left",
        box=box.ROUNDED,
    )
    console.print(panel)
