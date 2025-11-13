"""
Exception definitions for the biothings-cli
"""

from pathlib import Path
from typing import Union


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


class UnknownUploaderSource(Exception):
    """
    Used in the elasticsearch indexing

    If no elasticsearch mapping is found for a plugin and multiple
    data sources have been uploaded, we have to specify via --sub-source-name
    what specific uploaded data source we wish to index. If that
    subsource name doesn't match any of the uploaded sources discovered
    by the uploader manager, then we raise this exception
    """
