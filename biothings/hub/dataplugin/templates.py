"""
A temporary stopgap due to the limitations of pickle

the pickle serialization library cannot handle nested
modules. With the loaders in their own submodules now
(hub.dataplugin.loaders), pickle can no longer
serialize the generated AssistedDumper / AssistedUploader
classes.

I plan to migrate to a language agnostic serialization library
to avoid this problem and improve our job management, but in
the interim we need the class generation to be at the root level of a module
"""

from pathlib import Path
from string import Template
from typing import Union
import importlib

from biothings.hub.dataload.dumper import AssistedDumper
from biothings.hub.dataload.uploader import AssistedUploader


def generate_assisted_dumper_class(template_file: Union[str, Path], dumper_configuration: dict):
    with open(template_file, "r", encoding="utf-8") as tpl_handle:
        template = Template(tpl_handle)
        pystr = template.substitute(dumper_configuration)

        code = compile(pystr, "<string>", "exec")
        spec = importlib.util.spec_from_loader(dumper_configuration["SRC_NAME"], loader=None)
        mod = importlib.util.module_from_spec(spec)
        exec(code, mod.__dict__, mod.__dict__)
        dklass = getattr(mod, dumper_configuration["DUMPER_NAME"], None)

        # we need to inherit from a class here in this file so it can be pickled
        assisted_dumper_class = type(
            f"AssistedDumper_{dumper_configuration['SRC_NAME']}",
            (AssistedDumper, dklass),
            {},
        )
        assisted_dumper_class.python_code = pystr
        return assisted_dumper_class


def generate_assisted_uploader_class(template_file: Union[str, Path], uploader_configuration: dict):
    with open(template_file, "r", encoding="utf-8") as tpl_handle:
        tpl = Template(tpl_handle)
        pystr = tpl.substitute(uploader_configuration)

        code = compile(pystr, "<string>", "exec")
        spec = importlib.util.spec_from_loader(
            uploader_configuration["SRC_NAME"] + uploader_configuration["SUB_SRC_NAME"], loader=None
        )
        mod = importlib.util.module_from_spec(spec)
        exec(code, mod.__dict__, mod.__dict__)
        uklass = getattr(mod, uploader_name, None)

        # we need to inherit from a class here in this file so it can be pickled
        assisted_uploader_class = type(
            "AssistedUploader_%s" % uploader_configuration["SRC_NAME"] + uploader_configuration["SUB_SRC_NAME"],
            (AssistedUploader, uklass),
            {},
        )
        assisted_uploader_class.python_code = pystr
        return assisted_uploader_class
