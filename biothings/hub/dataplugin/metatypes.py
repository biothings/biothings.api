"""
Data type definitions used for construction of plugin classes specified
via the manifest files provided by both internal and external users
"""

from __future__ import annotations

import ast
import dataclasses
import importlib
from pathlib import Path
from typing import Callable, TYPE_CHECKING

from biothings.utils.common import uncompressall

from biothings.hub.dataplugin.transport import DumperTransport
from biothings.hub.dataplugin.exceptions import LoaderException

if TYPE_CHECKING:
    from biothings.hub.dataplugin.manager import DataPluginManager
    from biothings.hub.dataload.dumper import DumperManager
    from biothings.hub.dataload.uploader import UploaderManager


@dataclasses.dataclass
class MetaDumperAttributes:
    SRC_NAME: str
    SRC_ROOT_FOLDER: str | Path
    SRC_FOLDER_NAME: str | Path
    SRC_URLS: list[str]

    dumper_manager: DumperManager
    uploader_manager: UploaderManager
    data_plugin_manager: DataPluginManager
    keylookup: Callable

    schedule: str
    metadata: dict
    disabled: bool
    uncompress: bool
    _set_release: Callable


class DumperMetaClass(type):
    """
    The dumper metaclass is a class for our manifest-based
    dumper classes.

    If a class defines how the instance behaves, then the metaclass
    defines how the class behaves, so we're defining the custom behavior
    of how we want the dumper class to work
    (metaclass <- class <- class instance)
    """

    DATA_PLUGIN_FOLDER = None

    def __new__(mcs, class_name: str, class_attributes: MetaDumperAttributes, transport: DumperTransport):
        bases = (transport.dumper_class,)
        dumper_class = super().__new__(mcs, class_name, bases, dataclasses.asdict(class_attributes))
        mcs.release = None
        return dumper_class

    def post_dump(cls) -> None:
        """Default post dump operation method."""
        if cls.UNCOMPRESS:
            cls.logger.info("Uncompress all archive files in [%s]", cls.new_data_folder)
            uncompressall(cls.new_data_folder)

    def set_release(cls) -> None:
        cls.release = cls._set_release()


def manifest_dumper_factory(
    plugin_name: str, plugin_directory: Path | str, source_root_directory: Path | str, manifest_mapping: dict
) -> DumperMetaClass:
    """Factory method for generating our manifest-based dumper class"""

    def validate_class_name(name: str) -> None:
        class_definition = f"class {name}: pass"
        try:
            ast.parse(class_definition)
        except SyntaxError as syntax_error:
            raise syntax_error

    try:
        dumper_name = f"{plugin_name.capitalize()}Dumper"
        validate_class_name(dumper_name)
    except SyntaxError as syntax_error:
        raise syntax_error

    dumper_transport = DumperTransport(manifest_mapping["dumper"]["data_url"])
    dumper_release_function = manifest_mapping["dumper"].get("release", None)

    if dumper_release_function is not None:
        set_release_callback = load_manifest_defined_function(plugin_directory, dumper_release_function)
    else:
        set_release_callback = lambda: "version unknown"

    dumper_schedule = manifest_mapping["dumper"].get("schedule", "")
    class_attributes = MetaDumperAttributes(
        SRC_NAME=plugin_name,
        SRC_ROOT_FOLDER=source_root_directory,
        SRC_FOLDER_NAME=source_root_directory.name,
        SRC_URLS=dumper_transport.dumper_urls,
        dumper_manager=None,
        uploader_manager=None,
        data_plugin_manager=None,
        keylookup=None,
        schedule=f"'{dumper_schedule}'",
        metadata=manifest_mapping["dumper"].get("metadata", None),
        disabled=manifest_mapping["dumper"].get("disabled", None),
        uncompress=manifest_mapping["dumper"].get("uncompress", None),
        _set_release=set_release_callback,
    )

    dumper_class = DumperMetaClass(dumper_name, class_attributes, dumper_transport)
    return dumper_class


def load_manifest_defined_function(plugin_directory: Path | str, module_name: str) -> Callable:
    """Loads a function callback from an attribute pointer in the manifest."""
    try:
        module, funcname = map(str.strip, module_name.split(":"))
    except ValueError as value_error:
        raise LoaderException(
            "Invalid format for module '%s', it must be use the following format 'module:func'", module_name
        ) from value_error

    plugin_directory = Path(plugin_directory).resolve().absolute()
    module_file = plugin_directory.joinpath(module).with_suffix(".py")

    if module_file.exists():  # Plugin specific module
        module_spec = importlib.util.spec_from_file_location(module, module_file)
        plugin_module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(plugin_module)
    else:  # Generic biothings hub module
        # Some data plugins use BioThings generic parser.
        # >>> pending.api/plugins/doid/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # >>> pending.api/plugins/mondo/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # >>> pending.api/plugins/ncit/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # >>> pending.api/plugins/go/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # >>> pending.api/plugins/chebi/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # In such cases, `plugin_path_name` is not part of the module path.
        plugin_module = importlib.import_module(module)
        importlib.reload(plugin_module)

    module_function = getattr(plugin_module, funcname, None)

    if module_function is None:
        missing_function_error = f"Unable to find function {funcname} in loaded module {plugin_module}"
        raise LoaderException(missing_function_error)
    return module_function
