"""
Exception definitions for the biothings-cli
"""

from pathlib import Path
from typing import Callable, Union


class MissingPluginName(Exception):
    def __init__(self, working_directory: Union[str, Path]):
        message = (
            "Attempting to utilize the biothings-cli tooling in HUB mode "
            "without specifying the plugin-name. "
            f"The current working directory [{working_directory}] "
            "does not contain a plugin. "
            "Either specify the plugin-name via the --plugin-name option, "
            "or change your directory upon execution"
        )
        super().__init__(message)
