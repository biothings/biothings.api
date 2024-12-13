import asyncio
import logging
import math
import os
import pathlib
import shutil
import sys
import time
import uuid
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
from biothings.utils.manager import CLIJobManager
from biothings.utils.dataload import dict_traverse
from biothings.utils.serializer import load_json, to_json
from biothings.utils.workers import upload_worker
import biothings.utils.inspect as btinspect
from biothings.hub.dataplugin.assistant import BaseAssistant


class CLIAssistant(BaseAssistant):
    """
    Assistant instance used for interfacing with the various
    action (dump, upload, build, etc ...) managers
    """

    plugin_type = None
    data_plugin_manager = None
    dumper_manager = None
    uploader_manager = None
    keylookup = None

    def __init__(self, url: str):
        super().__init__(url)
        self.url = url
        self._plugin_name = None
        self._src_folder = None
        self._loader = None
        self.logfile = None
        self.logger = None
        self.setup_log()
        self._initialize_managers()
        # self.load_plugin_managers()

    def _initialize_managers(self) -> None:
        """
        Initializes all the action managers in one location
        """
        from biothings.hub.dataload.dumper import DumperManager
        from biothings.hub.dataload.uploader import UploaderManager
        from biothings.hub.databuild.builder import BuilderManager
        from biothings.hub.dataplugin.manager import DataPluginManager

        self.data_plugin_manager = DataPluginManager(job_manager=None)
        self.dumper_manager = DumperManager(job_manager=None)
        self.upload_manager = UploaderManager(job_manager=None)
        self.build_manager = BuilderManager(job_manager=None)

    def load_plugin_managers(
        self,
        plugin_path: Union[str, pathlib.Path],
        plugin_name: str = None,
        data_folder: Union[str, pathlib.Path] = None,
    ):
        """
        Load a data plugin from <plugin_path>, and return a tuple of (dumper_manager, upload_manager)
        """
        if plugin_name is None:
            plugin_name = _plugin_path.name

        if data_folder is None:
            data_folder = pathlib.Path(f"./{plugin_name}")
        data_folder = pathlib.Path(data_folder).resolve().absolute()

        from biothings import config
        from biothings.utils.hub_db import get_data_plugin

        _plugin_path = pathlib.Path(plugin_path).resolve()
        config.DATA_PLUGIN_FOLDER = _plugin_path.parent.as_posix()
        sys.path.append(str(_plugin_path.parent))

        logger.debug(self.plugin_name, plugin_name, _plugin_path.as_posix(), config.DATA_PLUGIN_FOLDER)

        data_plugin = get_data_plugin()
        data_plugin.remove({"_id": self.plugin_name})
        data_plugin.insert_one(
            {
                "_id": self.plugin_name,
                "plugin": {
                    "url": f"local://{plugin_name}",
                    "type": self.plugin_type,
                    "active": True,
                },
                "download": {
                    "data_folder": str(data_folder),  # tmp path to your data plugin
                },
            }
        )
        self.loader.load_plugin()

        return plugin_loader.__class__.dumper_manager, assistant.__class__.uploader_manager

    def get_plugin_name(plugin_name=None, with_working_dir=True):
        """
        return a valid plugin name (the folder name contains a data plugin)
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

    def load_plugin(plugin_name: str = None, dumper: bool = True, uploader: bool = True, logger: logging.Logger = None):
        """
        Return a plugin object for the given plugin_name.
        If dumper is True, include a dumper instance in the plugin object.
        If uploader is True, include uploader_classes in the plugin object.

        If <plugin_name> is not valid, raise the proper error and exit.
        """
        logger = logger or get_logger(__name__)

        _plugin_name, working_dir = get_plugin_name(plugin_name, with_working_dir=True)
        if plugin_name is None:
            # current working_dir has the data plugin
            data_plugin_dir = pathlib.Path(working_dir)
            data_folder = pathlib.Path(".").resolve().absolute()
            plugin_args = {"plugin_path": working_dir, "plugin_name": None, "data_folder": data_folder}
        else:
            data_plugin_dir = pathlib.Path(working_dir, _plugin_name)
            plugin_args = {"plugin_path": _plugin_name, "plugin_name": None, "data_folder": None}
        try:
            dumper_manager, uploader_manager = load_plugin_managers(**plugin_args)
        except Exception as gen_exc:
            logger.exception(gen_exc)
            if plugin_name is None:
                plugin_loading_error_message = "This command must be run inside a data plugin folder. Please go to a data plugin folder and try again!"
            else:
                plugin_loading_error_message = (
                    f'The data plugin folder "{data_plugin_dir}" is not a valid data plugin folder. Please try another.'
                )
            logger.error(plugin_loading_error_message, extra={"markup": True})
            raise typer.Exit(1)

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
