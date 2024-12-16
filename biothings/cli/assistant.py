import logging
import pathlib
import sys

import rich
import typer

from biothings.hub.dataplugin.assistant import BaseAssistant

logger = logging.getLogger(name="biothings-cli")


class CLIAssistant(BaseAssistant):
    """
    Assistant instance used for interfacing with the various
    action (dump, upload, build, etc ...) managers
    """

    from biothings.hub.databuild.builder import BuilderManager
    from biothings.hub.dataindex.indexer import IndexManager
    from biothings.hub.dataload.dumper import DumperManager
    from biothings.hub.dataload.uploader import UploaderManager
    from biothings.hub.dataplugin.manager import DataPluginManager

    build_manager = BuilderManager(job_manager=None)
    data_plugin_manager = DataPluginManager(job_manager=None)
    dumper_manager = DumperManager(job_manager=None)
    index_manager = IndexManager(job_manager=None)
    upload_manager = UploaderManager(job_manager=None)

    plugin_type = "CLI"

    def __init__(self, url: str, plugin_name: str = None):
        super().__init__(url)

        from biothings import config

        working_directory = pathlib.Path().cwd()
        if plugin_name is None:
            # assume that the current working directory has the data plugin
            self.plugin_name = working_directory.name
            self.plugin_directory = working_directory
            self.data_directory = working_directory
            self.validate_plugin_name(plugin_name, working_directory)
        else:
            self.plugin_name = plugin_name
            self.plugin_directory = working_directory.joinpath(plugin_name)
            self.data_directory = working_directory.joinpath(plugin_name)

        sys.path.append(str(self.plugin_directory.parent))
        config.DATA_PLUGIN_FOLDER = self.plugin_directory
        self.load_plugin()

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
            f"[lightsalmon1]{self.plugin_directory.as_posix()}[/lightsalmon1]\n"
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
                "data_folder": str(self.data_folder),  # tmp path to your data plugin
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
        if plugin_name not in subdirectory_names:
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
