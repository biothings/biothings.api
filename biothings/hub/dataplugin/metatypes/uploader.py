"""
Data type definitions used for construction of plugin classes specified
via the manifest files provided by both internal and external users

Types defined here for the uploader
"""

from __future__ import annotations

import dataclasses
import functools
from pathlib import Path
from typing import Callable, TYPE_CHECKING, Optional

from biothings.hub.dataload.uploader import (
    BaseSourceUploader,
    ParallelizedSourceUploader,
)
from biothings.hub.dataplugin.metatypes.shared import load_manifest_defined_function, validate_class_name
from biothings.hub.dataplugin.exceptions import LoaderException
from biothings.utils.storage import (
    BasicStorage,
    IgnoreDuplicatedStorage,
    MergerStorage,
)

if TYPE_CHECKING:
    from biothings.hub.dataplugin.manager import DataPluginManager
    from biothings.hub.dataload.dumper import DumperManager
    from biothings.hub.dataload.uploader import UploaderManager


@dataclasses.dataclass
class MetaUploaderAttributes:
    main_source: str

    dumper_manager: Optional[DumperManager]
    uploader_manager: Optional[UploaderManager]
    data_plugin_manager: Optional[DataPluginManager]
    keylookup: Optional[Callable]

    storage_class: BaseSourceUploader | IgnoreDuplicatedStorage | MergerStorage
    __metadata__: dict

    _parser_function: Callable
    parser_arguments: dict
    _jobs_function: Callable
    _mapping_function: Callable
    _idconverter_function: Optional[Callable]


class UploaderMetaClass(type):
    """
    The uploader metaclass is a class for our manifest-based
    uploader classes.

    If a class defines how the instance behaves, then the metaclass
    defines how the class behaves, so we're defining the custom behavior
    of how we want the uploader class to work
    (metaclass <- class <- class instance)
    """

    def __new__(mcs, class_name: str, bases: tuple[Callable], class_attributes: MetaUploaderAttributes):
        uploader_class = super().__new__(mcs, class_name, bases, dataclasses.asdict(class_attributes))
        mcs.release = None
        return uploader_class

    def load_data(cls, data_path: str | Path):
        cls.logger.info("Load data from directory or file: '%s'", data_path)

        confdict["CALL_PARSER_FUNC"] = "self.__class__.idconverter(parser_func)(data_path, **parser_kwargs)"
        if cls._idconverter_function is not None:
            cls._idconverter_function(cls._parser_function)(data_path, **cls.parser_arguments)
        else:
            cls._parser_function(**cls.parser_arguments)

    def jobs(cls):
        return cls.jobs_callback()

    def mapping(cls):
        return cls.mapping_callback()


def manifest_uploader_factory(
    plugin_name: str,
    uploader_manifest_mapping: dict,
    manifest_metadata: dict,
    plugin_directory: Path | str,
    keylookup_function: Optional[Callable] = None,
) -> UploaderMetaClass:
    """Factory method for generating our manifest-based uploader class"""

    try:
        sub_source_name = uploader_manifest_mapping.get("name", None)
        if sub_source_name is not None:
            uploader_name = f"{plugin_name.capitalize()}{sub_source_name.capitalize()}Uploader"
        else:
            uploader_name = f"{plugin_name.capitalize()}Uploader"
        validate_class_name(uploader_name)
    except SyntaxError as syntax_error:
        raise syntax_error

    try:
        # build our parser callback functionality
        try:
            parser_callback = load_manifest_defined_function(plugin_directory, uploader_manifest_mapping["parser"])
            parser_callback_arguments = uploader_manifest_mapping.get("parser_kwargs", {})
        except Exception as gen_exc:
            parser_callback_error = "Unable to load the parser function from the manifest"
            raise LoaderException(parser_callback_error) from gen_exc

        # storage class duplicate record strategy
        on_duplicates = uploader_manifest_mapping.get("on_duplicates", "error")
        storage_class = None
        if on_duplicates == "error":
            storage_class = BasicStorage
        elif on_duplicates == "merge":

            storage_class = MergerStorage
        elif on_duplicates == "ignore":
            storage_class = IgnoreDuplicatedStorage
        else:
            storage_class = BasicStorage

        # upload parallization
        uploader_parallel_jobs_function = uploader_manifest_mapping.get("parallelizer", None)
        base_classes: tuple = ()
        if uploader_parallel_jobs_function is not None:
            base_classes = (ParallelizedSourceUploader,)
            jobs_callback = load_manifest_defined_function(plugin_directory, uploader_parallel_jobs_function)
        else:
            base_classes = (BaseSourceUploader,)
            jobs_callback = lambda: ""

        # elasticsearch mapping function
        uploader_mapping_generator = uploader_manifest_mapping.get("mapping", None)
        if uploader_mapping_generator is not None:
            mapping_callback = load_manifest_defined_function(plugin_directory, uploader_mapping_generator)
        else:
            mapping_callback = lambda: ""

        # keylookup transformation function
        manifest_uploader_keylookup = uploader_manifest_mapping.get("keylookup", None)
        if manifest_uploader_keylookup is not None:
            if keylookup_function is None:
                keylookup_missing_error = (
                    f"Plugin {plugin_name} requires an id conversion function in the uploader manifest"
                )
                raise LoaderException(keylookup_missing_error)
            else:
                keylookup_callback = functools.partial(keylookup_function, manifest_uploader_keylookup)
        else:
            keylookup_callback = None

        # data-plugin metadata
        plugin_metadata = {"src_meta": manifest_metadata}

        class_attributes = MetaUploaderAttributes(
            main_source=plugin_name,
            dumper_manager=None,
            uploader_manager=None,
            data_plugin_manager=None,
            keylookup=None,
            storage_class=storage_class,
            __metadata__=plugin_metadata,
            _jobs_function=jobs_callback,
            _mapping_function=mapping_callback,
            _idconverter_function=keylookup_callback,
            _parser_function=parser_callback,
            parser_arguments=parser_callback_arguments,
        )

        uploader_class = UploaderMetaClass(uploader_name, base_classes, class_attributes)
        return uploader_class
    except Exception as gen_exc:
        raise LoaderException("Unable to generate an uploader instance from the manifest")
