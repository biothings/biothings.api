"""
Small module for universally loading the manifest schema definition
"""

import json

import pathlib


LOADER_DIRECTORY = pathlib.Path(__file__).resolve().absolute().parent
SCHEMA_DIRECTORY = LOADER_DIRECTORY.joinpath("schema")


def load_manifest_schema() -> dict:
    """
    Loads the manifest schema definition from the `schema` directory
    """
    manifest_schema_file = SCHEMA_DIRECTORY.joinpath("manifest.json")
    with open(manifest_schema_file, "r", encoding="utf-8") as schema_handle:
        manifest_schema = json.load(schema_handle)
        return manifest_schema
