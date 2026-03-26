import abc
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import jsonschema
import yaml

from biothings import config as btconfig
from biothings.hub.dataplugin.exceptions import LoaderException
from biothings.hub.dataplugin.schema import load_manifest_schema
from biothings.hub.dataplugin.schema.exceptions import determine_validation_error_category
from biothings.hub.dataplugin.metatypes import (
    manifest_dumper_factory,
    manifest_uploader_factory,
)

from biothings.utils.hub_db import get_data_plugin
from biothings.utils.loggers import get_logger


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

    def __init__(self, plugin_name: str):
        super().__init__(plugin_name)
        self.source_root_directory = None

    def can_load_plugin(self) -> bool:
        plugin = self.get_plugin_obj()
        df = Path(plugin["download"]["data_folder"])
        return Path(df, "manifest.json").exists() or Path(df, "manifest.yaml").exists()

    def _add_dumper_metadata_plugin_entry(self, data_plugin_folder: Path, manifest: dict):
        """Generates the metadata to construct the manifest classes."""
        dp = get_data_plugin()
        plugin = self.get_plugin_obj()
        manifest_dumper_arguments = {
            "plugin_name": self.plugin_name,
            "plugin_directory": str(data_plugin_folder),
            "source_root_directory": str(self.source_root_directory),
            "manifest_mapping": manifest,
        }
        dp.update_one({"_id": self.plugin_name}, {"$set": {"manifest_dumper": manifest_dumper_arguments}})

    def validate_manifest(self, manifest: dict):
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
        data_plugin_folder = Path(plugin["download"]["data_folder"])
        self.plugin_path_name = data_plugin_folder.name
        self.source_root_directory = (
            Path(btconfig.DATA_ARCHIVE_ROOT).resolve().absolute().joinpath(self.plugin_path_name)
        )
        if data_plugin_folder.exists():
            manifest_json = Path(data_plugin_folder, "manifest.json")
            manifest_yaml = Path(data_plugin_folder, "manifest.yaml")
            manifest = None
            try:
                if manifest_json.exists():
                    self.logger.debug("Loading manifest: %s", manifest_json)
                    with open(manifest_json, "r", encoding="utf-8") as manifest_handle:
                        manifest = json.load(manifest_handle)
                elif manifest_yaml.exists():
                    self.logger.debug("Loading manifest: %s", manifest_yaml)
                    with open(manifest_yaml, "r", encoding="utf-8") as manifest_handle:
                        manifest = yaml.safe_load(manifest_handle)
                else:
                    self.logger.error("No manifest found for plugin: %s", plugin["plugin"]["url"])
                    self.invalidate_plugin("No manifest found")
            except Exception:
                self.invalidate_plugin("Improperly formatted manifest file")

            try:
                self.validate_manifest(manifest)
            except jsonschema.exceptions.ValidationError as validation_error:
                self.logger.exception(validation_error)

                raise LoaderException from validation_error
            except Exception as gen_exc:
                self.logger.error("Unable to validate the manifest")
                raise LoaderException from gen_exc

            self._add_dumper_metadata_plugin_entry(data_plugin_folder, manifest)

            try:
                self.interpret_manifest(manifest, data_plugin_folder.as_posix())
            except Exception as gen_exc:
                self.invalidate_plugin(f"Error loading manifest: {gen_exc}")
        else:
            self.invalidate_plugin(f"Missing plugin folder [{data_plugin_folder}]")

    def interpret_manifest(self, manifest: dict, data_plugin_folder: Path | str) -> None:
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

            uninstalled_requirements: set[str] = set()
            for req in requirements:
                try:
                    subprocess.run(
                        [sys.executable, "-m", "pip", "show", req],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        check=True,
                    )
                except subprocess.CalledProcessError as subprocess_error:
                    self.logger.exception(subprocess_error)
                    self.logger.error(
                        "Unable to check install requirement for [%s]. " "Will attempt to batch install it ...", req
                    )
                    uninstalled_requirements.add(req)
                else:
                    self.logger.debug("Requirement [%s] already found on system. Skipping installation ...", req)

            if len(uninstalled_requirements) > 0:
                batch_install_group: str = " ".join(uninstalled_requirements)
                installation_command = f"{sys.executable} -m pip install {batch_install_group}"
                try:
                    subprocess.check_call(shlex.split(installation_command))
                except subprocess.CalledProcessError as subprocess_error:
                    self.logger.exception(subprocess_error)
                    raise LoaderException from subprocess_error
                self.logger.info("Installed requirement(s) %s", batch_install_group)

        if manifest.get("dumper"):
            assisted_dumper_class = manifest_dumper_factory(
                plugin_name=self.plugin_name,
                plugin_directory=data_plugin_folder,
                source_root_directory=self.source_root_directory,
                manifest_mapping=manifest,
            )
            self.dumper_manager.register_classes([assisted_dumper_class])

        if manifest.get("uploader"):
            assisted_uploader_class = manifest_uploader_factory(
                plugin_name=self.plugin_name,
                plugin_directory=data_plugin_folder,
                uploader_manifest_mapping=manifest["uploader"],
                manifest_metadata=manifest.get("metadata", {}),
                keylookup_function=self.__class__.keylookup,
            )
            self.__class__.uploader_manager.register_classes([assisted_uploader_class])

        if manifest.get("uploaders"):
            uploader_classes = [
                manifest_uploader_factory(
                    plugin_name=self.plugin_name,
                    plugin_directory=data_plugin_folder,
                    uploader_manifest_mapping=uploader_manifest,
                    manifest_metadata=manifest.get("metadata", {}),
                    keylookup_function=self.__class__.keylookup,
                )
                for uploader_manifest in manifest.get("uploaders")
            ]
            self.__class__.uploader_manager.register_classes(uploader_classes)

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
        df = Path(plugin["download"]["data_folder"])
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
