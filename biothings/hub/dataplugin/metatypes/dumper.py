"""
Data type definitions used for construction of plugin classes specified
via the manifest files provided by both internal and external users

Types defined here for the dumper
"""

from __future__ import annotations
from pathlib import Path
import copyreg
import dataclasses
import sys
import types


from biothings.utils.common import uncompressall

from biothings.hub.dataplugin.exceptions import LoaderException
from biothings.hub.dataplugin.transport import DumperTransport
from biothings.hub.dataplugin.metatypes.shared import (
    load_manifest_defined_function,
    validate_class_name,
    dumper_registry,
)


@dataclasses.dataclass
class MetaDumperAttributes:
    SRC_NAME: str
    SRC_ROOT_FOLDER: str | Path
    DATA_PLUGIN_FOLDER: str | Path
    SRC_FOLDER_NAME: str
    SRC_URLS: list[str]
    UNCOMPRESS: bool

    manifest_based: bool
    manifest: dict
    schedule: str
    metadata: dict
    disabled: bool
    uncompress: bool
    get_release_function: str


# --- internal class instance methods to add to the metadumper ---
def post_dump(self, *args, **kwargs) -> None:
    """Default post dump operation method."""
    if self.UNCOMPRESS:
        self.logger.info("Uncompress all archive files in [%s]", self.new_data_folder)
        uncompressall(self.new_data_folder)


def set_release(self) -> None:
    self.release = self.get_release()


def get_release(self) -> str:
    if self.get_release_function is not None:
        get_release_callback = load_manifest_defined_function(self.DATA_PLUGIN_FOLDER, self.get_release_function)
        return get_release_callback(self)
    else:  # default case
        return "version unknown"


def manifest_dumper_factory(
    plugin_name: str, plugin_directory: Path | str, source_root_directory: Path | str, manifest_mapping: dict
) -> type:
    """Factory method for generating our manifest-based dumper class"""
    try:
        dumper_name = f"{plugin_name.capitalize()}Dumper"
        validate_class_name(dumper_name)
    except SyntaxError as syntax_error:
        raise syntax_error

    if dumper_name in dumper_registry:
        cached_dumper = dumper_registry[dumper_name]
        print(f"Loading cached dumper instance {cached_dumper}")
        return cached_dumper

    try:
        dumper_transport = DumperTransport(manifest_mapping["dumper"]["data_url"])
        dumper_get_release_function = manifest_mapping["dumper"].get("release", None)
        dumper_schedule = manifest_mapping["dumper"].get("schedule", "")
        dumper_uncompress = manifest_mapping["dumper"].get("uncompress", False)

        class_attributes = MetaDumperAttributes(
            SRC_NAME=plugin_name,
            SRC_ROOT_FOLDER=source_root_directory,
            DATA_PLUGIN_FOLDER=plugin_directory,
            SRC_FOLDER_NAME=source_root_directory.name,
            SRC_URLS=dumper_transport.dumper_urls,
            UNCOMPRESS=dumper_uncompress,
            manifest_based=True,
            manifest=manifest_mapping,
            schedule=f"'{dumper_schedule}'",
            metadata=manifest_mapping["dumper"].get("metadata", None),
            disabled=manifest_mapping["dumper"].get("disabled", None),
            uncompress=manifest_mapping["dumper"].get("uncompress", None),
            get_release_function=dumper_get_release_function,
        )

        # construct our metatype
        # >>> class name
        # >>> inherited classes
        # >>> namespace
        dumper_namespace = dataclasses.asdict(class_attributes)
        dumper_base_classes = (dumper_transport.dumper_class,)

        # This the type which creates instances
        dumper_class_type: type = type(dumper_name, dumper_base_classes, dumper_namespace)

        # populate the method attributes
        methods_namespace = {
            "post_dump": post_dump,
            "set_release": set_release,
            "get_release": get_release,
        }

        # *** NOTE ***
        # It's important we add these after the creation of the class so they
        # are instance methods and not class methods
        for function_name, function in methods_namespace.items():
            setattr(dumper_class_type, function_name, function)

        dumper_class_type.__module__ = __name__
        setattr(sys.modules[__name__], dumper_name, dumper_class_type)
        dumper_registry[dumper_name] = dumper_class_type

        return dumper_class_type
    except Exception as gen_exc:
        raise LoaderException("Unable to build dumper class from manifest") from gen_exc
