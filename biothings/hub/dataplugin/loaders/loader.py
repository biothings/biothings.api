import abc
import importlib
import inspect
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import textwrap
import urllib.parse
from typing import Dict, Tuple, Union

import jsonschema
import yaml

from biothings import config as btconfig
from biothings.hub.dataload.dumper import DockerContainerDumper, LastModifiedFTPDumper, LastModifiedHTTPDumper
from biothings.hub.dataplugin.loaders.schema import load_manifest_schema
from biothings.hub.dataplugin.loaders.schema.exceptions import determine_validation_error_category
from biothings.hub.dataplugin.templates import generate_assisted_dumper_class, generate_assisted_uploader_class
from biothings.utils import storage
from biothings.utils.common import get_class_from_classpath
from biothings.utils.hub_db import get_data_plugin
from biothings.utils.loggers import get_logger


class LoaderException(Exception):
    pass


class BasePluginLoader(abc.ABC):
    loader_type = None  # set in subclass

    def __init__(self, plugin_name: str):
        self.plugin_name = plugin_name
        self.plugin_path_name = None  # This will be set on loading step
        self._plugin = None
        self.setup_log()

    def setup_log(self):
        """
        Setup and return a logger instance
        """
        log_folder = None
        if btconfig.LOG_FOLDER:
            log_folder = os.path.join(btconfig.LOG_FOLDER, "dataload")
        self.logger, self.logfile = get_logger("loader_%s" % self.plugin_name, log_folder=log_folder)

    def get_plugin_obj(self):
        if self._plugin:
            return self._plugin

        dp = get_data_plugin()
        plugin = dp.find_one({"_id": self.plugin_name})
        if not plugin.get("download", {}).get("data_folder"):
            raise LoaderException("Can't find data_folder, not available yet ?")
        self._plugin = plugin
        return plugin

    def invalidate_plugin(self, error: str):
        self.logger.error("Invalid plugin '%s' because: %s" % (self.plugin_name, error))
        # flag all plugin associated (there should only one though, but no need to care here)
        try:
            for klass in self.__class__.data_plugin_manager[self.plugin_name]:
                klass.data_plugin_error = error
        except KeyError:
            # plugin_name is not registered yet
            pass
        raise LoaderException(error)

    @abc.abstractmethod
    def can_load_plugin(self) -> bool:
        """
        Return True if loader is able to load plugin (check data folder content)
        """

    @abc.abstractmethod
    def load_plugin(self):
        """
        Load plugin and register its components
        """


class ManifestBasedPluginLoader(BasePluginLoader):
    loader_type = "manifest"

    # should match a _dict_for_***
    dumper_registry = {
        "http": LastModifiedHTTPDumper,
        "https": LastModifiedHTTPDumper,
        "ftp": LastModifiedFTPDumper,
        "docker": DockerContainerDumper,
    }

    def _dict_for_base(self, data_url):
        if isinstance(data_url, str):
            data_url = [data_url]
        return {
            "SRC_NAME": self.plugin_name,
            "SRC_ROOT_FOLDER": os.path.join(btconfig.DATA_ARCHIVE_ROOT, self.plugin_path_name),
            "SRC_FOLDER_NAME": self.plugin_path_name,
            "SRC_URLS": data_url,
        }

    def _dict_for_http(self, data_url):
        return self._dict_for_base(data_url)

    def _dict_for_https(self, data_url):
        d = self._dict_for_http(data_url)
        # not secure, but we want to make sure things will work as much as possible...
        d["VERIFY_CERT"] = False
        return d

    def _dict_for_ftp(self, data_url):
        return self._dict_for_base(data_url)

    def _dict_for_docker(self, data_url):
        d = self._dict_for_base(data_url)
        return d

    def can_load_plugin(self) -> bool:
        plugin = self.get_plugin_obj()
        df = pathlib.Path(plugin["download"]["data_folder"])
        return pathlib.Path(df, "manifest.json").exists() or pathlib.Path(df, "manifest.yaml").exists()

    def validate_manifest(self, manifest: Dict):
        """
        Validate a manifest instance using the biothings-manifest schema.

        Handles manifest validation to provide proper error messaging when a user
        provides an invalid manifest. Given these manifests can be written by anyone
        we want particularly clear error messages when validating the manifest

        A lot of the logic taken from jsonschema.validate function because want to provide
        validation but not necessarily overload the end-user with schema details
        """
        manifest_schema = load_manifest_schema()
        schema_validator_class = jsonschema.validators.validator_for(manifest_schema)

        try:
            schema_validator_class.check_schema(manifest_schema)
        except jsonschema.exceptions.SchemaError as schema_error:
            self.logger.exception(schema_error)
            raise schema_error

        validator = schema_validator_class(manifest_schema)
        validation_error = jsonschema.exceptions.best_match(validator.iter_errors(manifest))
        if validation_error is not None:
            refined_validation_error = determine_validation_error_category(validation_error)
            raise refined_validation_error

    def load_plugin(self):
        plugin = self.get_plugin_obj()
        data_folder = pathlib.Path(plugin["download"]["data_folder"])
        self.plugin_path_name = data_folder.name
        if data_folder.exists():
            mf = pathlib.Path(data_folder, "manifest.json")
            mf_yaml = pathlib.Path(data_folder, "manifest.yaml")
            manifest = None
            if mf.exists():
                self.logger.debug(f"Loading manifest: {mf}")
                with open(mf, "r", encoding="utf-8") as manifest_handle:
                    manifest = json.load(manifest_handle)
            elif mf_yaml.exists():
                self.logger.debug(f"Loading manifest: {mf_yaml}")
                with open(mf_yaml, "r", encoding="utf-8") as manifest_handle:
                    manifest = yaml.safe_load(manifest_handle)
            else:
                self.logger.error("No manifest found for plugin: %s" % plugin["plugin"]["url"])
                self.invalidate_plugin("No manifest found")

            try:
                self.validate_manifest(manifest)
            except jsonschema.exceptions.ValidationError as validation_error:
                self.logger.exception(validation_error)
                raise LoaderException from validation_error
            except Exception as gen_exc:
                self.logger.error("Unable to validate the manifest")
                raise LoaderException from gen_exc

            try:
                self.interpret_manifest(manifest, data_folder.as_posix())
            except Exception as gen_exc:
                self.invalidate_plugin("Error loading manifest: %s" % str(gen_exc))
        else:
            self.invalidate_plugin("Missing plugin folder '%s'" % data_folder)

    def get_code_for_mod_name(self, plugin_directory: Union[str, pathlib.Path], mod_name: str) -> Tuple[str, str]:
        """
        Returns string literal and name of function, given a path

        Args:
            mod_name: string with module name and function name, separated by colon

        Returns:
            Tuple[str, str]: containing
                - indented string literal for the function specified
                - name of the function
        """
        try:
            module, funcname = map(str.strip, mod_name.split(":"))
        except ValueError:
            raise LoaderException(
                "Invalid format for module '%s', it must be use the following format 'module:func'", mod_name
            )

        plugin_directory = pathlib.Path(plugin_directory).resolve().absolute()
        module_file = plugin_directory.joinpath(module).with_suffix(".py")

        if module_file.exists():  # Plugin specific module
            module_spec = importlib.util.spec_from_file_location(module, module_file)
            plugin_module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(plugin_module)
            self.logger.debug("Imported custom module %s for plugin %s", plugin_module, self.plugin_path_name)
        else:  # Generic biothings hub module
            # Some data plugins use BioThings generic parser.
            # >>> pending.api/plugins/doid/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
            # >>> pending.api/plugins/mondo/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
            # >>> pending.api/plugins/ncit/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
            # >>> pending.api/plugins/go/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
            # >>> pending.api/plugins/chebi/manifest.json {"parser": "hub.dataload.data_parsers:load_obo"}
            # In such cases, `self.plugin_path_name` is not part of the module path.
            plugin_module = importlib.import_module(module)
            importlib.reload(plugin_module)
            self.logger.debug("Imported generic module %s for plugin %s", plugin_module, self.plugin_path_name)

        module_function = getattr(plugin_module, funcname, None)
        if module_function is None:
            missing_function_error = f"Unable to find function {funcname} in loaded module {plugin_module}"
            raise LoaderException(missing_function_error)

        strfunc = inspect.getsource(module_function)

        # always indent with spaces, normalize to avoid mixed indenting chars
        indentfunc = textwrap.indent(strfunc.replace("\t", "    "), prefix="    ")

        return indentfunc, funcname

    def get_dumper_dynamic_class(
        self, plugin_directory: Union[str, pathlib.Path], dumper_section: Dict, metadata: Dict
    ):
        if dumper_section.get("data_url"):
            if not isinstance(dumper_section["data_url"], list):
                dumper_urls = [dumper_section["data_url"]]
            else:
                dumper_urls = dumper_section["data_url"]

            schemes = {urllib.parse.urlsplit(durl).scheme for durl in dumper_urls}

            # https = http regarding dumper generation
            if len({sch.replace("https", "http") for sch in schemes}) > 1:
                raise LoaderException(
                    "Manifest specifies URLs of different types (%s), " % schemes + "expecting only one"
                )
            scheme = schemes.pop()
            if "docker" in scheme:
                scheme = "docker"

            klass = dumper_section.get("class")
            dumper_configuration = getattr(self, "_dict_for_%s" % scheme)(dumper_urls)

            # Add disabled flag to dumper configuration with default value False
            dumper_configuration["DISABLED"] = dumper_section.get("disabled", False)

            if klass:
                dumper_class = get_class_from_classpath(klass)
                dumper_configuration["BASE_CLASSES"] = klass
            else:
                dumper_class = self.dumper_registry.get(scheme)
                dumper_configuration["BASE_CLASSES"] = "biothings.hub.dataload.dumper.%s" % dumper_class.__name__

            if not dumper_class:
                raise LoaderException("No dumper class registered to handle scheme '%s'", scheme)

            if metadata:
                dumper_configuration["__metadata__"] = metadata
            else:
                dumper_configuration["__metadata__"] = {}

            if dumper_section.get("release"):
                indentfunc, func = self.get_code_for_mod_name(plugin_directory, dumper_section["release"])
                assert func != "set_release", "'set_release' is a reserved method name, pick another name"
                dumper_configuration["SET_RELEASE_FUNC"] = (
                    """
%s

    def set_release(self):
        self.release = self.%s()
"""
                    % (
                        indentfunc,
                        func,
                    )
                )

            else:
                dumper_configuration["SET_RELEASE_FUNC"] = ""

            pnregex = r"^[A-z_][\w\d]+$"
            assert re.compile(pnregex).match(
                self.plugin_name
            ), "Incorrect plugin name '%s' (doesn't match regex '%s'" % (self.plugin_name, pnregex)
            dumper_name = f"{self.plugin_name.capitalize()}Dumper"
            "%s"
            try:
                dumper_configuration["DUMPER_NAME"] = dumper_name
                dumper_configuration["SRC_NAME"] = self.plugin_name
                if dumper_section.get("schedule"):
                    schedule = """'%s'""" % dumper_section["schedule"]
                else:
                    schedule = "None"
                dumper_configuration["SCHEDULE"] = schedule
                dumper_configuration["UNCOMPRESS"] = dumper_section.get("uncompress", False)

                if hasattr(btconfig, "DUMPER_TEMPLATE"):
                    tpl_file = btconfig.DUMPER_TEMPLATE
                else:
                    # default: assuming in ..../biothings/hub/dataplugin/
                    curmodpath = os.path.realpath(__file__)
                    if scheme == "docker":
                        tpl_file = os.path.join(os.path.dirname(curmodpath), "docker_dumper.py.tpl")
                    else:
                        tpl_file = os.path.join(os.path.dirname(curmodpath), "dumper.py.tpl")
                assisted_dumper_class = generate_assisted_dumper_class(tpl_file, dumper_configuration)
                return assisted_dumper_class

            except Exception:
                self.logger.exception("Can't generate dumper code for '%s'" % self.plugin_name)
                raise
        else:
            raise LoaderException("Invalid manifest, expecting 'data_url' key in 'dumper' section")

    def get_uploader_dynamic_class(
        self, plugin_directory: Union[str, pathlib.Path], uploader_section, metadata, sub_source_name=""
    ):
        if uploader_section.get("parser"):
            uploader_name = self.plugin_name.capitalize() + sub_source_name + "Uploader"
            confdict = {
                "SRC_NAME": self.plugin_name,
                "SUB_SRC_NAME": sub_source_name,
                "UPLOADER_NAME": uploader_name,
            }
            try:
                mod, func = uploader_section.get("parser").split(":")
                # make sure the parser module is able to load
                # otherwise, the error log should be shown in the UI
                self.get_code_for_mod_name(plugin_directory, uploader_section["parser"])
                confdict["PARSER_MOD"] = mod
                confdict["PARSER_FUNC"] = func
                if uploader_section.get("parser_kwargs"):
                    parser_kwargs_serialized = repr(uploader_section["parser_kwargs"])

                    confdict["PARSER_FACTORY_CODE"] = textwrap.dedent(
                        f"""
                        # Setup parser to parser factory
                        from {mod} import {func} as parser_func

                        parser_kwargs = {parser_kwargs_serialized}
                    """
                    )
                else:
                    # create empty parser_kwargs to pass to parser_func
                    parser_kwargs_serialized = repr({})

                    confdict["PARSER_FACTORY_CODE"] = textwrap.dedent(
                        f"""
                    # when code is exported, import becomes relative
                    try:
                        from {self.plugin_path_name}.{mod} import {func} as parser_func
                    except ImportError:
                        try:
                            from .{mod} import {func} as parser_func
                        except ImportError:
                            # When relative import fails, try to import it directly
                            import sys
                            import importlib
                            sys.path.insert(0, \"{plugin_directory}\")
                            import {mod}
                            importlib.reload({mod})
                            from {mod} import {func} as parser_func
                    parser_kwargs = {parser_kwargs_serialized}
                    """
                    )
            except ValueError as value_error:
                loader_error_message = (
                    f"`parser` must be defined as `module:parser_func` but got: `{uploader_section['parser']}`"
                )
                raise LoaderException(loader_error_message) from value_error
            try:
                ondups = uploader_section.get("on_duplicates")
                storage_class = storage.get_storage_class(ondups)
                if "ignore_duplicates" in uploader_section:
                    raise LoaderException(
                        "'ignore_duplicates' key not supported anymore, use 'on_duplicates' : 'error|ignore|merge'"
                    )
                confdict["STORAGE_CLASS"] = storage_class
                # default is not ID conversion at all
                confdict["IMPORT_IDCONVERTER_FUNC"] = ""
                confdict["IDCONVERTER_FUNC"] = None
                confdict["CALL_PARSER_FUNC"] = "parser_func(data_path, **parser_kwargs)"
                if uploader_section.get("keylookup"):
                    assert self.__class__.keylookup, (
                        "Plugin %s needs _id conversion " % self.plugin_name + "but no keylookup instance was found"
                    )
                    self.logger.info("Keylookup conversion required: %s" % uploader_section["keylookup"])
                    klmod = inspect.getmodule(self.__class__.keylookup)
                    confdict["IMPORT_IDCONVERTER_FUNC"] = "from %s import %s" % (
                        klmod.__name__,
                        self.__class__.keylookup.__name__,
                    )
                    convargs = ",".join(["%s=%s" % (k, v) for k, v in uploader_section["keylookup"].items()])
                    confdict["IDCONVERTER_FUNC"] = "%s(%s)" % (
                        self.__class__.keylookup.__name__,
                        convargs,
                    )
                    confdict["CALL_PARSER_FUNC"] = "self.__class__.idconverter(parser_func)(data_path, **parser_kwargs)"
                if metadata:
                    confdict["__metadata__"] = metadata
                else:
                    confdict["__metadata__"] = {}

                if uploader_section.get("parallelizer"):
                    indentfunc, func = self.get_code_for_mod_name(plugin_directory, uploader_section["parallelizer"])
                    assert func != "jobs", "'jobs' is a reserved method name, pick another name"
                    confdict["BASE_CLASSES"] = "biothings.hub.dataload.uploader.ParallelizedSourceUploader"
                    confdict["IMPORT_FROM_PARALLELIZER"] = ""
                    confdict["JOBS_FUNC"] = (
                        """
%s
    def jobs(self):
        return self.%s()
"""
                        % (
                            indentfunc,
                            func,
                        )
                    )
                else:
                    confdict["BASE_CLASSES"] = "biothings.hub.dataload.uploader.BaseSourceUploader"
                    confdict["JOBS_FUNC"] = ""

                if uploader_section.get("mapping"):
                    indentfunc, func = self.get_code_for_mod_name(plugin_directory, uploader_section["mapping"])
                    assert func != "get_mapping", "'get_mapping' is a reserved class method name, pick another name"
                    confdict["MAPPING_FUNC"] = (
                        """
    @classmethod
%s

    @classmethod
    def get_mapping(cls):
        return cls.%s()
"""
                        % (
                            indentfunc,
                            func,
                        )
                    )
                else:
                    confdict["MAPPING_FUNC"] = ""

                if hasattr(btconfig, "DUMPER_TEMPLATE"):
                    tpl_file = btconfig.DUMPER_TEMPLATE
                elif sub_source_name:
                    curmodpath = os.path.realpath(__file__)
                    tpl_file = os.path.join(os.path.dirname(curmodpath), "subuploader.py.tpl")
                else:
                    # default: assuming in ..../biothings/hub/dataplugin/
                    curmodpath = os.path.realpath(__file__)
                    tpl_file = os.path.join(os.path.dirname(curmodpath), "uploader.py.tpl")

                assisted_uploader_class = generate_assisted_uploader_class(tpl_file, confdict)
                return assisted_uploader_class

            except Exception as gen_exc:
                self.logger.exception(gen_exc)
                loader_exception = LoaderException("Error loading plugin: can't interpret manifest")
                raise loader_exception from gen_exc
        else:
            raise LoaderException("Invalid manifest, expecting 'parser' key in 'uploader' section")

    def get_uploader_dynamic_classes(self, plugin_directory: Union[str, pathlib.Path], uploader_section, metadata):
        uploader_classes = []
        for uploader_conf in uploader_section:
            sub_source_name = uploader_conf.get("name", "")
            uploader_class = self.get_uploader_dynamic_class(plugin_directory, uploader_conf, metadata, sub_source_name)
            uploader_class.DATA_PLUGIN_FOLDER = plugin_directory

            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedUploader_%s" % self.plugin_name + sub_source_name
            ] = uploader_class

            uploader_classes.append(uploader_class)
        return uploader_classes

    def interpret_manifest(self, manifest: Dict, data_plugin_folder: Union[str, pathlib.Path]) -> None:
        """
        Handles the interpretation and loading of the manifest contents
        to determine how to build the dumper and uploader classes,
        installation of the plugin requirements, and assigning of the plugin
        metadata
        """

        # start with requirements before importing anything
        if manifest.get("requires"):
            requirements = manifest["requires"]
            if not isinstance(requirements, list):
                requirements = [requirements]

            uninstalled_requirements = set()
            for req in requirements:
                subprocess_result = subprocess.run(
                    [sys.executable, "-m", "pip", "show", req], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                if subprocess_result.returncode != 0:
                    uninstalled_requirements.add(req)
                else:
                    self.logger.debug("Requirement %s already found on system. Skipping installation ...")

            if len(uninstalled_requirements) > 0:
                uninstalled_requirements = " ".join(uninstalled_requirements)
                installation_command = f"{sys.executable} -m pip install {uninstalled_requirements}"
                try:
                    subprocess.check_call(shlex.split(installation_command))
                except subprocess.CalledProcessError as subprocess_error:
                    self.logger.exception(subprocess_error)
                    raise LoaderException from subprocess_error
                self.logger.info("Installed requirement(s) %s", uninstalled_requirements)

        if manifest.get("dumper"):
            assisted_dumper_class = self.get_dumper_dynamic_class(
                data_plugin_folder, manifest["dumper"], manifest.get("__metadata__")
            )
            assisted_dumper_class.DATA_PLUGIN_FOLDER = data_plugin_folder
            self.__class__.dumper_manager.register_classes([assisted_dumper_class])
            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedDumper_%s" % self.plugin_name
            ] = assisted_dumper_class

        if manifest.get("uploader"):
            assisted_uploader_class = self.get_uploader_dynamic_class(
                data_plugin_folder, manifest["uploader"], manifest.get("__metadata__")
            )
            assisted_uploader_class.DATA_PLUGIN_FOLDER = data_plugin_folder
            self.__class__.uploader_manager.register_classes([assisted_uploader_class])
            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedUploader_%s" % self.plugin_name
            ] = assisted_uploader_class
        if manifest.get("uploaders"):
            assisted_uploader_classes = self.get_uploader_dynamic_classes(
                data_plugin_folder, manifest["uploaders"], manifest.get("__metadata__")
            )
            self.__class__.uploader_manager.register_classes(assisted_uploader_classes)
        if manifest.get("display_name"):
            dp = get_data_plugin()
            dp.update(
                {"_id": self.plugin_name},
                {
                    "$set": {
                        "plugin.display_name": manifest.get("display_name"),
                    }
                },
            )
        if manifest.get("biothing_type"):
            dp = get_data_plugin()
            dp.update(
                {"_id": self.plugin_name},
                {
                    "$set": {
                        "plugin.biothing_type": manifest.get("biothing_type"),
                    }
                },
            )


class AdvancedPluginLoader(BasePluginLoader):
    loader_type = "advanced"

    def can_load_plugin(self) -> bool:
        plugin = self.get_plugin_obj()
        df = pathlib.Path(plugin["download"]["data_folder"])
        if df.exists():
            data_folder_files = {file.name for file in df.iterdir()}
            return "__init__.py" in data_folder_files
        else:
            return False

    def load_plugin(self):
        plugin = self.get_plugin_obj()
        df = plugin["download"]["data_folder"]
        if os.path.exists(df):
            # we assume there's a __init__ module exposing Dumper and Uploader classes
            # as necessary
            modpath = df.split("/")[-1]
            # before registering, process optional requirements.txt
            reqfile = os.path.join(df, "requirements.txt")
            if os.path.exists(reqfile):
                self.logger.info("Installing requirements from %s for plugin '%s'" % (reqfile, self.plugin_name))
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", reqfile])

            # submit to managers to register datasources
            self.logger.info("Registering '%s' to dump manager", modpath)
            try:
                self.dumper_manager.register_source(modpath)
            except Exception as gen_exc:
                self.logger.exception(gen_exc)
                self.logger.error("Couldn't register dumper from module '%s': %s", modpath, gen_exc)
                self.invalidate_plugin(f"Unable to load dumper module for plugin: '{df}'")

            self.logger.info("Registering '%s' to upload manager(s)", modpath)
            try:
                self.uploader_manager.register_source(modpath)
            except Exception as gen_exc:
                self.logger.exception(gen_exc)
                self.logger.error("Couldn't register uploader from module '%s': %s", modpath, gen_exc)
                self.invalidate_plugin(f"Unable to load uploader module for plugin: '{df}'")
        else:
            self.invalidate_plugin("Missing plugin folder '%s'", df)
