import sys
from pathlib import Path

from biothings.hub.dataplugin.metatypes.dumper import manifest_dumper_factory
from biothings.hub.dataplugin.metatypes.uploader import manifest_uploader_factory
from biothings.hub.dataplugin.metatypes.shared import (
    dumper_registry,
    uploader_registry,
    load_manifest_defined_function,
    validate_class_name,
)
from biothings.utils.hub_db import get_data_plugin

__all__ = [
    "dumper_registry",
    "uploader_registry",
    "load_manifest_defined_function",
    "validate_class_name",
    "manifest_dumper_factory",
    "manifest_uploader_factory",
]


def load_registry():
    dp = get_data_plugin()
    plugin = dp.find({})

    for plugin_entry in plugin:

        # build our dumper from the factory
        manifest_dumper_arguments = plugin_entry.get("manifest_dumper", None)
        if manifest_dumper_arguments is not None:
            manifest_dumper_factory(
                manifest_dumper_arguments["plugin_name"],
                Path(manifest_dumper_arguments["plugin_directory"]),
                Path(manifest_dumper_arguments["source_root_directory"]),
                manifest_dumper_arguments["manifest_mapping"],
            )


load_registry()
