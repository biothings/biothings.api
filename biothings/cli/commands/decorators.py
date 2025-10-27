"""
Collection of decorators for usage within the biothings-cli

These are often method we want associated with many of the plugin methods we
use, but don't directly impact the logic of the actual operation. Typically things
related to paths and configurations that apply to large swaths of the cli
would make sense as a decorator
"""

import functools
import inspect
import logging
import pathlib
import sys
from typing import Callable

from biothings.cli.exceptions import MissingPluginName


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
    async def determine_operation_mode(*args, **kwargs):
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

        if inspect.iscoroutinefunction(operation_method):
            operation_result = await operation_method(*args, **kwargs)
        else:
            operation_result = operation_method(*args, **kwargs)
        return operation_result

    return determine_operation_mode


def cli_system_path(func: Callable):  # pylint: disable=unused-argument
    """
    Used for ensuring that if we've appended files to biothings-cli
    path file (stored under config.BIOTHINGS_CLI_PATH), then we need to update
    the system path so we can discover the modules at runtime
    """

    @functools.wraps(func)
    async def update_system_path(*args, **kwargs):
        from biothings import config

        discovery_path = pathlib.Path(config.BIOTHINGS_CLI_PATH).resolve().absolute()
        path_file = discovery_path.joinpath(".biothings_cli.pth")

        if path_file.exists():
            with open(path_file, "r", encoding="utf-8") as handle:
                path_entries = handle.readlines()
                path_entries = [entry.strip("\n") for entry in path_entries]
                sys.path.extend(path_entries)
                for path in path_entries:
                    logger.debug("Adding %s to system path", path)

        if inspect.iscoroutinefunction(func):
            func_result = await func(*args, **kwargs)
        else:
            func_result = func(*args, **kwargs)
        return func_result

    return update_system_path
