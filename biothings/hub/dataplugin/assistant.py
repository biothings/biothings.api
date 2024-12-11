import inspect
import json
import os
import pprint
import re
import sys
import textwrap
import urllib.parse

# we switched to use black for code formatting
# from yapf.yapflib import yapf_api
try:
    import black

    black_avail = True
except ImportError:
    black_avail = False
import requests
import yaml

from biothings import config as btconfig
from biothings.hub.dataload.dumper import DockerContainerDumper, LastModifiedFTPDumper, LastModifiedHTTPDumper
from biothings.hub.dataplugin.manager import GitDataPlugin, ManualDataPlugin
from biothings.hub.dataplugin.loader import ManifestBasedPluginLoader, AdvancedPluginLoader
from biothings.utils import storage
from biothings.utils.common import (
    get_class_from_classpath,
    get_plugin_name_from_local_manifest,
    get_plugin_name_from_remote_manifest,
    parse_folder_name_from_url,
    rmdashfr,
)
from biothings.utils.hub_db import get_data_plugin, get_src_dump, get_src_master
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseSourceManager


class AssistantException(Exception):
    pass


class BaseAssistant:
    plugin_type = None  # to be defined in subblass
    data_plugin_manager = None  # set by assistant manager
    dumper_manager = None  # set by assistant manager
    uploader_manager = None  # set by assistant manager
    keylookup = None  # set by assistant manager

    # known plugin loaders
    loaders = {
        "manifest": ManifestBasedPluginLoader,
        "advanced": AdvancedPluginLoader,
    }

    def __init__(self, url):
        self.url = url
        self._plugin_name = None
        self._src_folder = None
        self._loader = None
        self.logfile = None
        self.logger = None
        self.setup_log()

    def setup_log(self):
        """
        Setup and return a logger instance
        """
        self.logger, self.logfile = get_logger("assistant_%s" % self.__class__.plugin_type)

    def register_loader(self):
        dp = get_data_plugin()
        dp.update(
            {"_id": self.plugin_name},
            {"$set": {"plugin.loader": self.loader.loader_type}},
            upsert=True,
        )

    @property
    def loader(self):
        """
        Return loader object able to interpret plugin's folder content
        """
        if not self._loader:
            # iterate over known loaders, the first one which can interpret plugin content is kept
            for klass in self.loaders.values():
                # propagate managers
                klass.dumper_manager = self.dumper_manager
                klass.uploader_manager = self.uploader_manager
                klass.data_plugin_manager = self.data_plugin_manager
                klass.keylookup = self.keylookup
                loader = klass(self.plugin_name)
                if loader.can_load_plugin():
                    self._loader = loader
                    self.logger.debug(
                        'For plugin "%s", selecting loader class "%s"',
                        self.plugin_name,
                        self._loader.__class__.__name__,
                    )
                    self.register_loader()
                    break
                else:
                    self.logger.debug('Loader %s cannot load plugin "%s"', loader, self.plugin_name)
                    continue
        return self._loader

    @property
    def plugin_name(self):
        """
        Return plugin name, parsed from self.url and set self._src_folder as
        path to folder containing dataplugin source code
        """
        raise NotImplementedError("implement 'plugin_name' in subclass")

    def handle(self):
        """Access self.url and do whatever is necessary to bring code to life within the hub...
        (hint: that may involve creating a dumper on-the-fly and register that dumper to
        a manager...)
        """
        raise NotImplementedError("implement 'handle' in subclass")

    def can_handle(self):
        """Return true if assistant can handle the code"""
        raise NotImplementedError("implement 'can_handle' in subclass")

    def load_plugin(self):
        """
        Load plugin and register its components
        """
        raise NotImplementedError("implement 'load_plugin' in subclass")


class GithubAssistant(BaseAssistant):
    plugin_type = "github"

    @property
    def plugin_name(self):
        folder_name = parse_folder_name_from_url(self.url)
        if not self._plugin_name:
            self._src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, folder_name)
            # Try to load plugin name from the local first, if exist that mean we are working with a cloned and updated plugin
            # If plugin name is empty that mean this plugin has not cloned to local then we try to fetch its name from the Github
            # Otherwise we use the path_name as the fallback.
            plugin_name = get_plugin_name_from_local_manifest(os.path.join(btconfig.DATA_PLUGIN_FOLDER, folder_name))
            if not plugin_name:
                plugin_name = get_plugin_name_from_remote_manifest(self.url)
            if not plugin_name:
                plugin_name = folder_name
            self._plugin_name = plugin_name
        return self._plugin_name

    def can_handle(self):
        # analyze headers to guess type of required assitant
        try:
            headers = requests.head(self.url).headers
            if headers.get("server").lower() == "github.com":
                return True
        except Exception as e:
            self.logger.error("%s plugin can't handle URL '%s': %s" % (self.plugin_type, self.url, e))
            return False

    def get_classdef(self):
        # generate class dynamically and register
        confdict = {
            "SRC_NAME": self.plugin_name,
            "GIT_REPO_URL": self.url,
            "SRC_ROOT_FOLDER": self._src_folder,
        }
        # TODO: store confdict in hubconf collection
        k = type("AssistedGitDataPlugin_%s" % self.plugin_name, (GitDataPlugin,), confdict)
        return k

    def handle(self):
        assert self.__class__.data_plugin_manager, "Please set data_plugin_manager attribute"
        klass = self.get_classdef()
        self.__class__.data_plugin_manager.register_classes([klass])


class LocalAssistant(BaseAssistant):
    plugin_type = "local"

    @property
    def plugin_name(self):
        if not self._plugin_name:
            split = urllib.parse.urlsplit(self.url)
            # format local://pluginname so it's in hostname.
            # if path is set, it means format is  local://subdir/pluginname
            # and we don't support that for import reason (we would need to
            # add .../plugins/subdir to sys.path, not impossible but might have side effects
            # so for now we stay on the safe (and also let's remember 1st version of
            # MS DOS didn't support subdirs, so I guess we're on the right path :))
            assert not split.path, "It seems URL '%s' references a sub-directory (%s)," % (
                self.url,
                split.hostname,
            ) + " with plugin name '%s', sub-directories are not supported (yet)" % split.path.strip("/")
            # don't use hostname here because it's lowercased, netloc isn't
            # (and we're matching directory names on the filesystem, it's case-sensitive)
            src_folder_name = os.path.basename(split.netloc)
            try:
                self._plugin_name = (
                    get_plugin_name_from_local_manifest(os.path.join(btconfig.DATA_PLUGIN_FOLDER, src_folder_name))
                    or src_folder_name
                )
            except Exception as ex:
                self.logger.exception(ex)
                self._plugin_name = src_folder_name
            self._src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, src_folder_name)
        return self._plugin_name

    def can_handle(self) -> bool:
        return self.url.startswith(self.__class__.plugin_type + "://")

    def get_classdef(self):
        # generate class dynamically and register
        confdict = {"SRC_NAME": self.plugin_name, "SRC_ROOT_FOLDER": self._src_folder}
        k = type("AssistedManualDataPlugin_%s" % self.plugin_name, (ManualDataPlugin,), confdict)
        return k

    def handle(self):
        assert self.__class__.data_plugin_manager, "Please set data_plugin_manager attribute"
        klass = self.get_classdef()
        self.__class__.data_plugin_manager.register_classes([klass])


class AssistantManager(BaseSourceManager):
    def __init__(
        self,
        data_plugin_manager,
        dumper_manager,
        uploader_manager,
        keylookup=None,
        default_export_folder="hub/dataload/sources",
        *args,
        **kwargs,
    ):
        super(AssistantManager, self).__init__(*args, **kwargs)
        self.data_plugin_manager = data_plugin_manager
        self.dumper_manager = dumper_manager
        self.uploader_manager = uploader_manager
        self.keylookup = keylookup
        if not os.path.exists(btconfig.DATA_PLUGIN_FOLDER):
            os.makedirs(btconfig.DATA_PLUGIN_FOLDER)
        self.default_export_folder = default_export_folder
        # register data plugin folder in python path so we can import
        # plugins (sub-folders) as packages
        sys.path.insert(0, btconfig.DATA_PLUGIN_FOLDER)
        self.logfile = None
        self.setup_log()

    def setup_log(self):
        """Setup and return a logger instance"""
        self.logger, self.logfile = get_logger("assistantmanager")

    def create_instance(self, klass, url):
        return klass(url)

    def configure(self, klasses=[GithubAssistant, LocalAssistant]):  # noqa: B006
        self.register_classes(klasses)

    def register_classes(self, klasses):
        for klass in klasses:
            klass.data_plugin_manager = self.data_plugin_manager
            klass.dumper_manager = self.dumper_manager
            klass.uploader_manager = self.uploader_manager
            klass.keylookup = self.keylookup
            self.register[klass.plugin_type] = klass

    def submit(self, url):
        # submit url to all registered assistants (in order)
        # and return the first claiming it can handle that URLs
        for typ in self.register:
            aklass = self.register[typ]
            inst = self.create_instance(aklass, url)
            if inst.can_handle():
                return inst
        return None

    def unregister_url(self, url=None, name=None):
        dp = get_data_plugin()
        if url:
            url = url.strip()
            doc = dp.find_one({"plugin.url": url})
        elif name:
            doc = dp.find_one({"_id": name})
            url = doc["plugin"]["url"]
        else:
            raise ValueError("Specify 'url' or 'name'")
        if not doc:
            raise AssistantException("Plugin is not registered (url=%s, name=%s)" % (url, name))
        # should be only one but just in case
        dp.remove({"_id": doc["_id"]})
        # delete plugin code so it won't be auto-register
        # by 'local' plugin assistant (issue studio #7)
        if doc.get("download", {}).get("data_folder"):
            codefolder = doc["download"]["data_folder"]
            self.logger.info("Delete plugin source code in '%s'" % codefolder)
            rmdashfr(codefolder)
        assistant = self.submit(url)
        try:
            self.data_plugin_manager.register.pop(assistant.plugin_name)
        except KeyError:
            raise AssistantException("Plugin '%s' is not registered" % url)
        self.dumper_manager.register.pop(assistant.plugin_name, None)
        self.uploader_manager.register.pop(assistant.plugin_name, None)

    def register_url(self, url):
        url = url.strip()
        dp = get_data_plugin()
        folder_name = parse_folder_name_from_url(url)
        if dp.find_one({"plugin.url": url}) or dp.find_one(
            {"download.data_folder": f"{btconfig.DATA_PLUGIN_FOLDER}/{folder_name}"}
        ):
            self.logger.info("Plugin '%s' already registered" % url)
            return
        assistant = self.submit(url)
        self.logger.info("For data-plugin URL '%s', selected assistant is: %s" % (url, assistant))
        if assistant:
            # register plugin info
            # if a github url was used, by default, we assume it's a manifest-based plugin
            # (we can't know until we have a look at the content). So assistant will have
            # manifest-based loader. If it fails, another assistant with advanced loader will
            # be used to try again.
            dp.update(
                {"_id": assistant.plugin_name},
                {"$set": {"plugin": {"url": url, "type": assistant.plugin_type, "active": True}}},
                upsert=True,
            )
            assistant.handle()
            job = self.data_plugin_manager.load(assistant.plugin_name)
            assert len(job) == 1, "Expecting one job, got: %s" % job
            job = job.pop()

            def loaded(f):
                try:
                    _ = f.result()
                    self.logger.debug("Plugin '%s' downloaded, now loading manifest" % assistant.plugin_name)
                    assistant.loader.load_plugin()
                except Exception as e:
                    self.logger.exception("Unable to download plugin '%s': %s" % (assistant.plugin_name, e))

            job.add_done_callback(loaded)
            return job
        else:
            raise AssistantException("Could not find any assistant able to handle URL '%s'" % url)

    def load_plugin(self, plugin):
        ptype = plugin["plugin"]["type"]
        url = plugin["plugin"]["url"]
        if not plugin["plugin"]["active"]:
            self.logger.info("Data plugin '%s' is deactivated, skip" % url)
            return
        self.logger.info("Loading data plugin '%s' (type: %s)" % (plugin["_id"], ptype))
        if ptype in self.register:
            try:
                aklass = self.register[ptype]
                assistant = self.create_instance(aklass, url)
                assistant.handle()
                assistant.loader.load_plugin()
            except Exception as e:
                self.logger.exception("Unable to load plugin '%s': %s" % (url, e))
        else:
            raise AssistantException("Unknown data plugin type '%s'" % ptype)

    def load(self, autodiscover=True):
        """
        Load plugins registered in internal Hub database and generate/register
        dumpers & uploaders accordingly.
        If autodiscover is True, also search DATA_PLUGIN_FOLDER for existing
        plugin directories not registered yet in the database, and register
        them automatically.
        """
        plugin_dirs = []
        if autodiscover:
            try:
                plugin_dirs = os.listdir(btconfig.DATA_PLUGIN_FOLDER)
            except FileNotFoundError as e:
                raise AssistantException("Invalid DATA_PLUGIN_FOLDER: %s" % e)
        dp = get_data_plugin()
        cur = dp.find()
        for plugin in cur:
            try:
                plugin_dir_name = os.path.basename(plugin["download"]["data_folder"])
            except Exception as e:
                self.logger.warning("Couldn't load plugin '%s': %s" % (plugin["_id"], e))
                continue
            plugin_name = get_plugin_name_from_local_manifest(plugin.get("download").get("data_folder"))
            if plugin_name and plugin["_id"] != plugin_name:
                plugin = self.update_plugin_name(plugin, plugin_name)
            # remove plugins from folder list if already register
            if plugin_dir_name in plugin_dirs:
                plugin_dirs.remove(plugin_dir_name)
            try:
                self.load_plugin(plugin)
            except Exception as e:
                self.logger.warning("Couldn't load plugin '%s': %s" % (plugin["_id"], e))
                continue
        # some still unregistered ? (note: list always empty if autodiscover=False)
        if plugin_dirs:
            for pdir in plugin_dirs:
                os.path.join(btconfig.DATA_PLUGIN_FOLDER, pdir)
                try:
                    self.logger.info("Found unregistered manifest-based plugin '%s', auto-register it" % pdir)
                    self.register_url(f"local://{pdir}")
                except Exception as e:
                    self.logger.exception("Couldn't auto-register plugin '%s': %s" % (pdir, e))
                    continue

    def update_plugin_name(self, plugin, new_name):
        dp = get_data_plugin()
        old_name = plugin.pop("_id")
        dp.update({"_id": new_name}, {"$set": plugin}, upsert=True)
        dp.remove({"_id": old_name})
        plugin["_id"] = new_name
        src_dump_db = get_src_dump()
        src_dump_doc = src_dump_db.find_one({"_id": old_name})
        if src_dump_doc:
            src_dump_doc.pop("_id")
            src_dump_db.update({"_id": new_name}, {"$set": src_dump_doc}, upsert=True)
            src_dump_db.remove({"_id": old_name})
        src_master_db = get_src_master()
        src_master_doc = src_master_db.find_one({"_id": old_name})
        if src_master_doc:
            src_master_doc.pop("_id")
            src_master_doc["name"] = new_name
            src_master_db.update({"_id": new_name}, {"$set": src_master_doc}, upsert=True)
            src_master_db.remove({"_id": old_name})
        return plugin

    def export_dumper(self, plugin_name, folder):
        res = {"dumper": {"status": None, "file": None, "class": None, "message": None}}
        try:
            dclass = self.dumper_manager[plugin_name]
        except KeyError:
            res["dumper"]["status"] = "warning"
            res["dumper"]["message"] = "No dumper found for plugin '%s'" % plugin_name
        try:
            dumper_name = plugin_name.capitalize() + "Dumper"
            self.logger.debug("Exporting dumper %s" % dumper_name)
            assert len(dclass) == 1, "More than one dumper found: %s" % dclass
            dclass = dclass[0]
            assert hasattr(dclass, "python_code"), "No generated code found"
            dinit = os.path.join(folder, "__init__.py")
            dfile = os.path.join(folder, "dump.py")
            # clear init, we'll append code
            # we use yapf (from Google) as autopep8 (for instance) doesn't give
            # good results in term in indentation (input_type list for keylookup for instance)
            # switched to use black from yapf
            # beauty, _ = yapf_api.FormatCode(dclass.python_code)
            if black_avail:
                beauty = black.format_str(dclass.python_code, mode=black.Mode())
            else:
                raise ImportError('"black" package is required for exporting formatted code.')
            with open(dfile, "w") as fout:
                fout.write(beauty)
            with open(dinit, "a") as fout:
                fout.write("from .dump import %s\n" % dumper_name)
            res["dumper"]["status"] = "ok"
            res["dumper"]["file"] = dfile
            res["dumper"]["class"] = dumper_name
        except Exception as e:
            res["dumper"]["status"] = "error"
            res["dumper"]["message"] = "Error exporting dumper: %s" % e
            return res

        return res

    def export_uploader(self, plugin_name, folder):
        res = {"uploader": {"status": None, "file": [], "class": [], "message": None}}
        try:
            uclasses = self.uploader_manager[plugin_name]
        except KeyError:
            res["uploader"]["status"] = "warning"
            res["uploader"]["message"] = "No uploader found for plugin '%s'" % plugin_name
            return res
        status = "ok"
        message = ""
        for uclass in uclasses:
            try:
                uploader_name = uclass.__name__.split("_")[1].capitalize() + "Uploader"
                self.logger.debug("Exporting uploader %s" % uploader_name)
                # assert len(uclass) == 1, "More than one uploader found: %s" % uclass
                assert hasattr(uclass, "python_code"), "No generated code found"
                dinit = os.path.join(folder, "__init__.py")
                mod_name = f"{uclass.__name__.split('_')[1]}_upload"
                ufile = os.path.join(folder, mod_name + ".py")
                # switched to use black from yapf
                # beauty, _ = yapf_api.FormatCode(uclass.python_code)
                if black_avail:
                    beauty = black.format_str(uclass.python_code, mode=black.Mode())
                else:
                    raise ImportError('"black" package is required for exporting formatted code.')
                with open(ufile, "w") as fout:
                    fout.write(beauty)
                with open(dinit, "a") as fout:
                    fout.write(f"from .{mod_name} import %s\n" % uploader_name)

                res["uploader"]["file"].append(ufile)
                res["uploader"]["class"].append(uploader_name)
            except Exception as e:
                status = "error"
                message = "Error exporting uploader: %s" % e
        res["uploader"]["status"] = status
        res["uploader"]["message"] = message
        return res

    def export_mapping(self, plugin_name, folder):
        res = {"mapping": {"status": None, "file": None, "message": None, "origin": None}}
        # first check if plugin defines a custom mapping in manifest
        # if that's the case, we don't need to export mapping there
        # as it'll be exported with "uploader" code
        plugindoc = get_data_plugin().find_one({"_id": plugin_name})
        assert plugindoc, "Can't find plugin named '%s'" % plugin_name
        plugin_folder = plugindoc.get("download", {}).get("data_folder")
        assert plugin_folder, "Can't find plugin folder for '%s'" % plugin_name
        try:
            manifest = json.load(open(os.path.join(plugin_folder, "manifest.json")))
            if "mapping" in manifest.get("uploader", {}):
                res["mapping"]["message"] = "Custom mapping included in uploader export"
                res["mapping"]["status"] = "warning"
                res["mapping"]["origin"] = "custom"
                return res
        except Exception as e:
            self.logger.error("Can't read manifest while exporting code: %s" % e)
        # try to export mapping from src_master (official)
        doc = get_src_master().find_one({"_id": plugin_name})
        if doc:
            mapping = doc.get("mapping")
            res["mapping"]["origin"] = "registered"
        else:
            doc = get_src_dump().find_one({"_id": plugin_name})
            mapping = doc and doc.get("inspect", {}).get("jobs", {}).get(plugin_name, {}).get("inspect", {}).get(
                "results", {}
            ).get("mapping")
            res["mapping"]["origin"] = "inspection"
        if not mapping:
            res["mapping"]["origin"] = None
            res["mapping"]["status"] = "warning"
            res["mapping"]["message"] = "Can't find registered or generated (inspection) mapping"
            return res
        else:
            ufile = os.path.join(folder, "upload.py")
            # switched to use black from yapf
            # strmap, _ = yapf_api.FormatCode(pprint.pformat(mapping))
            strmap = black.format_str(pprint.pformat(mapping), mode=black.Mode())
            with open(ufile, "a") as fout:
                fout.write(
                    """
    @classmethod
    def get_mapping(klass):
        return %s\n"""
                    % textwrap.indent((strmap), prefix="    " * 2)
                )

        res["mapping"]["file"] = ufile
        res["mapping"]["status"] = "ok"

        return res

    def export(
        self,
        plugin_name,
        folder=None,
        what=["dumper", "uploader", "mapping"],  # noqa: B006
        purge=False,
    ):
        """
        Export generated code for a given plugin name, in given folder
        (or use DEFAULT_EXPORT_FOLDER if None). Exported information can be:
        - dumper: dumper class generated from the manifest
        - uploader: uploader class generated from the manifest
        - mapping: mapping generated from inspection or from the manifest
        If "purge" is true, any existing folder/code will be deleted first, otherwise,
        will raise an error if some folder/files already exist.
        """
        res = {}
        # sanity checks
        if type(what) == str:
            what = [what]
        folder = folder or self.default_export_folder
        assert os.path.exists(folder), "Folder used to export code doesn't exist: %s" % os.path.abspath(folder)
        assert plugin_name  # avoid deleting the whole export folder when purge=True...
        dp = get_data_plugin()
        plugin = dp.find_one({"_id": plugin_name})
        plugin_path_name = os.path.basename(plugin["download"]["data_folder"])
        if not plugin:
            raise Exception(f"Data plugin {plugin_name} does not exist!")
        folder = os.path.join(folder, plugin_path_name)
        if purge:
            rmdashfr(folder)
        if not os.path.exists(folder):
            os.makedirs(folder)
        elif not purge:
            raise FileExistsError("Folder '%s' already exists, use purge=True" % folder)
        dinit = os.path.join(folder, "__init__.py")
        with open(dinit, "w") as fout:
            fout.write("")
        if "dumper" in what:
            res.update(self.export_dumper(plugin_name, folder))
        if "uploader" in what:
            res.update(self.export_uploader(plugin_name, folder))
        if "mapping" in what:
            assert "uploader" in what, "'uploader' needs to be exported too to export mapping"
            res.update(self.export_mapping(plugin_name, folder))
        # there's also at least a parser module, maybe a release module, and some more
        # dependencies, indirect, not listed in the manifest. We'll just copy everything from
        # the plugin folder to the export folder
        plugin_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, plugin_path_name)
        for f in os.listdir(plugin_folder):
            src = os.path.join(plugin_folder, f)
            dst = os.path.join(folder, f)
            # useless or strictly plugin-machinery-specific, skip
            if f in ["__pycache__", "manifest.json", "__init__.py"] or f.startswith("."):
                self.logger.debug("Skipping '%s', not necessary" % src)
                continue
            self.logger.debug("Copying %s to %s" % (src, dst))
            try:
                with open(src) as fin:
                    with open(dst, "w") as fout:
                        fout.write(fin.read())
            except IsADirectoryError:
                self.logger.error("%s is a directory, expecting only files to copy" % src)
                continue

        return res
