"""
Custom assistant representation for the biothings-cli

Intended to handle our creation of different managers
and dataplugin loading.

Supported plugin types
> manifest
> advanced

Supported plugin locations
> local

"""

import asyncio
import copy
import logging
import os
import pathlib
import sys

import rich
import typer

from biothings.utils.common import get_plugin_name_from_local_manifest
from biothings.hub.dataplugin.assistant import BaseAssistant
from biothings.utils.manager import JobManager

logger = logging.getLogger(name="biothings-cli")


class CLIAssistant(BaseAssistant):
    """
    Assistant instance used for interfacing with the various
    action (dump, upload, build, etc ...) managers
    """

    plugin_type = "CLI"

    def __init__(self, plugin_name: str = None):
        from biothings import config

        from biothings.hub.databuild.builder import BuilderManager
        from biothings.hub.dataindex.indexer import IndexManager
        from biothings.hub.dataload.dumper import DumperManager
        from biothings.hub.dataload.uploader import UploaderManager
        from biothings.hub.dataplugin.manager import DataPluginManager

        src_folder = None
        if plugin_name is None:
            self.plugin_directory = pathlib.Path().cwd()
            plugin_name = self.plugin_directory.name

            src_folder = pathlib.Path().cwd()
            sys.path.append(str(src_folder.parent))
            self.data_directory = pathlib.Path().cwd()
        else:
            self.plugin_directory = pathlib.Path().cwd().joinpath(plugin_name)

            src_folder = copy.copy(self.plugin_directory)
            sys.path.append(str(src_folder))
            self.data_directory = copy.copy(self.plugin_directory)

        url = f"local://{plugin_name}"
        super().__init__(url, plugin_name, src_folder)

        self.job_manager = JobManager(
            loop=asyncio.get_running_loop(),
            process_queue=None,
            thread_queue=None,
            max_memory_usage=None,
            num_workers=os.cpu_count(),
            num_threads=16,
            auto_recycle=True,
        )
        self.build_manager = BuilderManager(job_manager=self.job_manager, datasource_path=self.data_directory)
        self.data_plugin_manager = DataPluginManager(job_manager=self.job_manager, datasource_path=self.data_directory)
        self.dumper_manager = DumperManager(job_manager=self.job_manager, datasource_path=self.data_directory)
        self.index_manager = IndexManager(job_manager=self.job_manager, datasource_path=self.data_directory)
        self.uploader_manager = UploaderManager(job_manager=self.job_manager, datasource_path=self.data_directory)

        config.DATA_PLUGIN_FOLDER = self._src_folder
        self.load_plugin()

    @property
    def loader(self):
        """
        Return loader object able to interpret plugin's folder content

        Iterate over known loaders, the first one which can interpret plugin content is kept
        """
        if not self._loader:
            for loader_class in self.loaders.values():
                loader_class.dumper_manager = self.dumper_manager
                loader_class.uploader_manager = self.uploader_manager
                loader_class.data_plugin_manager = self.data_plugin_manager
                loader_class.keylookup = self.keylookup
                loader = loader_class(self.plugin_name)
                if loader.can_load_plugin():
                    self._loader = loader
                    self.logger.debug(
                        'For plugin "%s", selecting loader class "%s"',
                        self.plugin_name,
                        self._loader.__class__.__name__,
                    )
                    self.register_loader()
                    break
        return self._loader

    @property
    def plugin_name(self):
        """
        Attempts to determine the plugin_name if it hasn't been assigned yet
        The actual property for storing the plugin_name is via `_plugin_name`.
        User access is via the `plugin_name` property

        Attempts to extract the plugin name from the manifest file
        """
        if not self._plugin_name:
            try:
                self._plugin_name = get_plugin_name_from_local_manifest(self.plugin_directory)
                if self._plugin_name is None:
                    self._plugin_name = self.plugin_directory.name
            except Exception as gen_exc:
                self.logger.exception(gen_exc)
                raise gen_exc
        return self._plugin_name

    def can_handle(self) -> bool:
        return True

    def load_plugin(self):
        """
        Return a plugin object for the given plugin_name.
        If dumper is True, include a dumper instance in the plugin object.
        If uploader is True, include uploader_classes in the plugin object.

        If <plugin_name> is not valid, raise the proper error and exit.
        """
        from biothings import config
        from biothings.utils.hub_db import get_data_plugin

        assistant_debug_info = (
            f"[green]Assistant Plugin Name:[/green][bold] "
            f"[lightsalmon1]{self.plugin_name}[/lightsalmon1]\n"
            f"[green]Assistant Plugin Path:[/green][bold] "
            f"[lightsalmon1]{self._src_folder.as_posix()}[/lightsalmon1]\n"
            f"[green]Data Plugin Folder:[/green][bold] "
            f"[lightsalmon1]{config.DATA_PLUGIN_FOLDER}[/lightsalmon1]"
        )
        logger.debug(assistant_debug_info, extra={"markup": True})

        plugin_entry = {
            "_id": self.plugin_name,
            "plugin": {
                "url": self.url,
                "type": self.plugin_type,
                "active": True,
            },
            "download": {
                "data_folder": str(self.data_directory),  # tmp path to your data plugin
            },
        }

        data_plugin = get_data_plugin()
        data_plugin.remove({"_id": self.plugin_name})
        data_plugin.insert_one(plugin_entry)
        self.loader.load_plugin()

    def validate_plugin_name(self, plugin_name: str, working_directory: pathlib.Path) -> None:
        """
        We validate the name based off the subdirectories in the working directory

        Raises a typer.Exit exception with code = 1 if the name is invalid
        """
        subdirectory_names = {f.name for f in working_directory.iterdir() if f.is_dir() and not f.name.startswith(".")}
        if not plugin_name or plugin_name not in subdirectory_names:
            rich.print("[red]Please provide your data plugin name! [/red]")
            rich.print("Choose from:\n    " + "\n    ".join(subdirectory_names))
            raise typer.Exit(code=1)

    def get_dumper_class(self):
        """
        Retrieves the associated dumper class stored from the dumper manager
        object stored with the assistant instance. Then builds the dumper class
        and prepares it before returning it
        """
        try:
            dumper_class = self.dumper_manager[self.plugin_name][0]
            dumper_instance = dumper_class()
            dumper_instance.prepare()
            return dumper_instance
        except Exception as gen_exc:
            logger.exception(gen_exc)
            raise gen_exc

    def get_uploader_class(self):
        """
        Retrieves the associated uploader class(s) stored from the uploader manager
        object stored with the assistant instance. Then builds the uploader class
        and prepares it before returning it
        """
        try:
            uploader_classes = self.uploader_manager[self.plugin_name]
            if not isinstance(uploader_classes, list):
                uploader_classes = [uploader_classes]
            return uploader_classes
        except Exception as gen_exc:
            logger.exception(gen_exc)
            raise gen_exc
