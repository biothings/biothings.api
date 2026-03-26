"""
Common functionality used across all metatypes for building their definitions
"""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path
from typing import Callable

from biothings.hub.dataplugin.exceptions import LoaderException

# global dumper registry
dumper_registry: dict = {}

# global uploader registry
uploader_registry: dict = {}


def load_manifest_defined_function(plugin_directory: Path | str, module_name: str) -> Callable:
    """Loads a function callback from an attribute pointer in the manifest."""
    try:
        module, funcname = map(str.strip, module_name.split(":"))
    except ValueError as value_error:
        raise LoaderException(
            f"Invalid format for module '{module_name}', it must use the format 'module:func'"
        ) from value_error

    plugin_directory = Path(plugin_directory).resolve().absolute()
    module_file = plugin_directory.joinpath(module).with_suffix(".py")

    if module_file.exists():  # Plugin specific module
        module_spec = importlib.util.spec_from_file_location(module, module_file)
        plugin_module = importlib.util.module_from_spec(module_spec)
        sys.modules[module] = plugin_module

        module_spec.loader.exec_module(plugin_module)
    else:  # Generic biothings hub module
        # Some data plugins use BioThings generic parser.
        # > pending.api/plugins/doid/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # > pending.api/plugins/mondo/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # > pending.api/plugins/ncit/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # > pending.api/plugins/go/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # > pending.api/plugins/chebi/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
        # In such cases, `plugin_path_name` is not part of the module path.
        plugin_module = importlib.import_module(module)
        importlib.reload(plugin_module)
        sys.modules[module] = plugin_module

    module_function = getattr(plugin_module, funcname, None)

    if module_function is None:
        missing_function_error = f"Unable to find function {funcname} in loaded module {plugin_module}"
        raise LoaderException(missing_function_error)
    return module_function


def validate_class_name(name: str) -> None:
    """Evaluates if we can use a class name from the manifest.

    Leverages `ast.parse` in the context of the class definition to
    see if we can build an AST node from our class
    """
    class_definition = f"class {name}: pass"
    try:
        ast.parse(class_definition)
    except SyntaxError as syntax_error:
        raise syntax_error
