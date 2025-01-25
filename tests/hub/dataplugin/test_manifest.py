"""
Tests the plugin manifest functionality
"""

import logging
import json
import pathlib

import jsonschema
import pytest

from biothings.hub.dataplugin.loaders.loader import ManifestBasedPluginLoader
from biothings.hub.dataplugin.loaders.schema.exceptions import (
    ManifestTypeException,
    ManifestMissingPropertyException,
    ManifestMutuallyExclusivePropertyException,
    ManifestAdditionalPropertyException,
    ManifestMinimumRequiredItemsException,
    ManifestIncorrectEnumException,
)


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "manifest",
    [
        "mock_manifest.json",
    ],
)
def test_valid_manifest_validation(manifest: str, temporary_data_storage: pathlib.Path):
    """
    Explores the manifest validation procedure to ensure that different
    types of valid manifests are correctly loaded
    """
    plugin_name = "turbographicsfx"

    manifest_directory = temporary_data_storage.joinpath("manifests")
    manifest_file = manifest_directory.joinpath(manifest)
    assert manifest_file.exists()

    manifest_loader = ManifestBasedPluginLoader(plugin_name=plugin_name)

    with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
        manifest = json.load(manifest_handle)
        manifest_loader.validate_manifest(manifest)


@pytest.mark.parametrize(
    "manifest, manifest_error",
    [
        ("malformed_manifest0.json", ManifestTypeException),
        ("malformed_manifest1.json", ManifestMutuallyExclusivePropertyException),
        ("malformed_manifest2.json", ManifestAdditionalPropertyException),
        ("malformed_manifest3.json", ManifestMissingPropertyException),
        ("malformed_manifest4.json", ManifestIncorrectEnumException),
        ("malformed_manifest5.json", ManifestMinimumRequiredItemsException),
    ],
)
def test_invalid_manifest_validation(manifest: str, manifest_error: Exception, temporary_data_storage: pathlib.Path):
    """
    Explores the manifest validation procedure to ensure that different
    types of invalid manifests aren't loaded and raise explicit and easy to follow
    errors

    Different errors covered:
    0) uploaders section must be an array (type error)
    1) include both an uploader and uploaders section (malformed manifest)
    2) includes an additional property at the root of the manifest (additional properties)
    3) missing a required property (dumper) at the root of the manifest (missing properties)
    4) incorrect literal value for uploader.on_duplicates (incorrect literal value)
    5) unpopulated list (dumper.data_url) that expects at least one value (empty list)
    """
    plugin_name = "turbographicsfx"

    manifest_directory = temporary_data_storage.joinpath("manifests")
    manifest_file = manifest_directory.joinpath(manifest)
    assert manifest_file.exists()

    manifest_loader = ManifestBasedPluginLoader(plugin_name=plugin_name)
    with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
        manifest_contents = json.load(manifest_handle)

    with pytest.raises(jsonschema.exceptions.ValidationError):
        manifest_loader.validate_manifest(manifest_contents)

    try:
        manifest_loader.validate_manifest(manifest_contents)
    except jsonschema.exceptions.ValidationError as validation_error:
        assert isinstance(validation_error, manifest_error)
        logger.info("Explicit Error Message [%s]", validation_error.message)
