"""
Small module for universally loading the manifest schema definition
and storing the associated manifest errors for helping users
debug their manifest files
"""

import json
import pathlib

SCHEMA_DIRECTORY = pathlib.Path(__file__).resolve().absolute().parent


def load_manifest_schema() -> dict:
    """
    Loads the schema structure from the schema module
    using the implicit directory structure of both
    the `__init__.py` and `manifest.json` being in
    the same directory
    """
    manifest_schema_file = SCHEMA_DIRECTORY.joinpath("manifest.json")
    with open(manifest_schema_file, "r", encoding="utf-8") as schema_handle:
        manifest_schema = json.load(schema_handle)
        return manifest_schema
