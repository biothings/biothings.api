"""
dataplugin schema module for validating our manifest files
defining our various plugins
"""

import json
import pathlib


def load_manifest_schema() -> dict:
    """
    Loads the schema structure from the schema module
    using the implicit directory structure of both
    the `__init__.py` and `manifest.json` being in
    the same directory
    """
    schema_directory = pathlib.Path(__file__).resolve().absolute().parent
    manifest_file = schema_directory.joinpath("manifest.json")
    with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
        schema_repr = json.load(manifest_handle)
        return schema_repr
