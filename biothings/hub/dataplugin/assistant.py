import importlib
import inspect
import json
import os
import pathlib
import pprint
import re
import subprocess
import sys
import textwrap
import urllib.parse
from string import Template

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


class LoaderException(Exception):
    pass


class BasePluginLoader(object):
    loader_type = None  # set in subclass

    def __init__(self, plugin_name):
        self.plugin_name = plugin_name
        self.plugin_path_name = None  # This will be set on loading step
        self.setup_log()
        self._plugin = None

    def setup_log(self):
        """Setup and return a logger instance"""
        log_folder = os.path.join(btconfig.LOG_FOLDER, "dataload") if btconfig.LOG_FOLDER else None
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

    def invalidate_plugin(self, error):
        self.logger.exception("Invalidate plugin '%s' because: %s" % (self.plugin_name, error))
        # flag all plugin associated (there should only one though, but no need to care here)
        try:
            for klass in self.__class__.data_plugin_manager[self.plugin_name]:
                klass.data_plugin_error = error
        except KeyError:
            # plugin_name is not registered yet
            pass
        raise LoaderException(error)

    def can_load_plugin(self):
        """
        Return True if loader is able to load plugin (check data folder content)
        """
        raise NotImplementedError("implement 'can_load_plugin' in subclass")

    def load_plugin(self):
        """
        Load plugin and register its components
        """
        raise NotImplementedError("implement 'load_plugin' in subclass")


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
        if type(data_url) == str:
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

    def can_load_plugin(self):
        plugin = self.get_plugin_obj()
        df = pathlib.Path(plugin["download"]["data_folder"])
        # if "manifest.json" in os.listdir(df) and os.path.exists(os.path.join(df, "manifest.json")):
        if pathlib.Path(df, "manifest.json").exists():
            return True
        # elif "manifest.yaml" in os.listdir(df) and os.path.exists(os.path.join(df, "manifest.yaml")):
        elif pathlib.Path(df, "manifest.yaml").exists():
            return True
        else:
            return False

    def load_plugin(self):
        plugin = self.get_plugin_obj()
        df = pathlib.Path(plugin["download"]["data_folder"])
        # self.plugin_path_name = os.path.basename(df)
        self.plugin_path_name = df.name
        # if os.path.exists(df):
        if df.exists():
            # mf = os.path.join(df, "manifest.json")
            # mf_yaml = os.path.join(df, "manifest.yaml")
            mf = pathlib.Path(df, "manifest.json")
            mf_yaml = pathlib.Path(df, "manifest.yaml")
            manifest = None
            # if os.path.exists(mf):
            if mf.exists():
                self.logger.debug(f"Loading manifest: {mf}")
                manifest = json.load(open(mf))
            # elif os.path.exists(mf_yaml):
            elif mf_yaml.exists():
                self.logger.debug(f"Loading manifest: {mf_yaml}")
                manifest = yaml.safe_load(open(mf_yaml))
            if manifest:
                try:
                    self.interpret_manifest(manifest, df.as_posix())
                except Exception as e:
                    self.invalidate_plugin("Error loading manifest: %s" % str(e))
            else:
                self.logger.info("No manifest found for plugin: %s" % plugin["plugin"]["url"])
                self.invalidate_plugin("No manifest found")
        else:
            self.invalidate_plugin("Missing plugin folder '%s'" % df)

    def get_code_for_mod_name(self, mod_name):
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
            mod, funcname = map(str.strip, mod_name.split(":"))
        except ValueError as e:
            raise AssistantException(
                "'Wrong format for '%s', it must be defined following format 'module:func': %s" % (mod_name, e)
            )
        modpath = self.plugin_path_name + "." + mod
        try:
            pymod = importlib.import_module(modpath)
            # self.logger.info("Imported custom module %s for plugin %s", modpath, self.plugin_path_name)
        except (ImportError, TypeError):
            # Some data plugins use BioThings generic parser, e.g. CHEBI plugin uses {"parser" : "hub.dataload.data_parsers:load_obo"}
            # In such cases, `self.plugin_path_name` is not part of the module path.
            pymod = importlib.import_module(mod)
            # self.logger.info("Imported generic module %s for plugin %s", mod, self.plugin_path_name)
        # reload in case we need to refresh plugin's code
        importlib.reload(pymod)
        assert funcname in dir(pymod), "%s not found in module %s" % (funcname, pymod)
        func = getattr(pymod, funcname)
        # fetch source and indent to class method level in the template
        strfunc = inspect.getsource(func)
        # always indent with spaces, normalize to avoid mixed indenting chars
        indentfunc = textwrap.indent(strfunc.replace("\t", "    "), prefix="    ")

        return indentfunc, funcname

    def get_dumper_dynamic_class(self, dumper_section, metadata):
        if dumper_section.get("data_url"):
            if not type(dumper_section["data_url"]) is list:
                durls = [dumper_section["data_url"]]
            else:
                durls = dumper_section["data_url"]
            schemes = set([urllib.parse.urlsplit(durl).scheme for durl in durls])
            # https = http regarding dumper generation
            if len(set([sch.replace("https", "http") for sch in schemes])) > 1:
                raise AssistantException(
                    "Manifest specifies URLs of different types (%s), " % schemes + "expecting only one"
                )
            scheme = schemes.pop()
            if "docker" in scheme:
                scheme = "docker"
            klass = dumper_section.get("class")
            confdict = getattr(self, "_dict_for_%s" % scheme)(durls)
            if klass:
                dumper_class = get_class_from_classpath(klass)
                confdict["BASE_CLASSES"] = klass
            else:
                dumper_class = self.dumper_registry.get(scheme)
                confdict["BASE_CLASSES"] = "biothings.hub.dataload.dumper.%s" % dumper_class.__name__
            if not dumper_class:
                raise AssistantException("No dumper class registered to handle scheme '%s'" % scheme)
            if metadata:
                confdict["__metadata__"] = metadata
            else:
                confdict["__metadata__"] = {}

            if dumper_section.get("release"):
                indentfunc, func = self.get_code_for_mod_name(dumper_section["release"])
                assert func != "set_release", "'set_release' is a reserved method name, pick another name"
                confdict[
                    "SET_RELEASE_FUNC"
                ] = """
%s

    def set_release(self):
        self.release = self.%s()
""" % (
                    indentfunc,
                    func,
                )

            else:
                confdict["SET_RELEASE_FUNC"] = ""

            dklass = None
            pnregex = r"^[A-z_][\w\d]+$"
            assert re.compile(pnregex).match(
                self.plugin_name
            ), "Incorrect plugin name '%s' (doesn't match regex '%s'" % (self.plugin_name, pnregex)
            dumper_name = self.plugin_name.capitalize() + "Dumper"
            "%s"
            try:
                if hasattr(btconfig, "DUMPER_TEMPLATE"):
                    tpl_file = btconfig.DUMPER_TEMPLATE
                else:
                    # default: assuming in ..../biothings/hub/dataplugin/
                    curmodpath = os.path.realpath(__file__)
                    if scheme == "docker":
                        tpl_file = os.path.join(os.path.dirname(curmodpath), "docker_dumper.py.tpl")
                    else:
                        tpl_file = os.path.join(os.path.dirname(curmodpath), "dumper.py.tpl")
                tpl = Template(open(tpl_file).read())
                confdict["DUMPER_NAME"] = dumper_name
                confdict["SRC_NAME"] = self.plugin_name
                if dumper_section.get("schedule"):
                    schedule = """'%s'""" % dumper_section["schedule"]
                else:
                    schedule = "None"
                confdict["SCHEDULE"] = schedule
                confdict["UNCOMPRESS"] = dumper_section.get("uncompress") or False
                pystr = tpl.substitute(confdict)
                # print(pystr)
                code = compile(pystr, "<string>", "exec")
                spec = importlib.util.spec_from_loader(self.plugin_name, loader=None)
                mod = importlib.util.module_from_spec(spec)
                exec(code, mod.__dict__, mod.__dict__)
                dklass = getattr(mod, dumper_name)
                # we need to inherit from a class here in this file so it can be pickled
                assisted_dumper_class = type(
                    "AssistedDumper_%s" % self.plugin_name,
                    (
                        AssistedDumper,
                        dklass,
                    ),
                    {},
                )
                assisted_dumper_class.python_code = pystr

                return assisted_dumper_class

            except Exception:
                self.logger.exception("Can't generate dumper code for '%s'" % self.plugin_name)
                raise
        else:
            raise AssistantException("Invalid manifest, expecting 'data_url' key in 'dumper' section")

    def get_uploader_dynamic_class(self, uploader_section, metadata, sub_source_name=""):
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
                self.get_code_for_mod_name(uploader_section["parser"])
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
                            sys.path.insert(0, ".")
                            from {mod} import {func} as parser_func
                    parser_kwargs = {parser_kwargs_serialized}
                    """
                    )
            except ValueError:
                raise AssistantException(
                    "'parser' must be defined as 'module:parser_func' but got: '%s'" % uploader_section["parser"]
                )
            try:
                ondups = uploader_section.get("on_duplicates")
                storage_class = storage.get_storage_class(ondups)
                if "ignore_duplicates" in uploader_section:
                    raise AssistantException(
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

                if hasattr(btconfig, "DUMPER_TEMPLATE"):
                    tpl_file = btconfig.DUMPER_TEMPLATE
                elif sub_source_name:
                    curmodpath = os.path.realpath(__file__)
                    tpl_file = os.path.join(os.path.dirname(curmodpath), "subuploader.py.tpl")
                else:
                    # default: assuming in ..../biothings/hub/dataplugin/
                    curmodpath = os.path.realpath(__file__)
                    tpl_file = os.path.join(os.path.dirname(curmodpath), "uploader.py.tpl")
                tpl = Template(open(tpl_file).read())

                if uploader_section.get("parallelizer"):
                    indentfunc, func = self.get_code_for_mod_name(uploader_section["parallelizer"])
                    assert func != "jobs", "'jobs' is a reserved method name, pick another name"
                    confdict["BASE_CLASSES"] = "biothings.hub.dataload.uploader.ParallelizedSourceUploader"
                    confdict["IMPORT_FROM_PARALLELIZER"] = ""
                    confdict[
                        "JOBS_FUNC"
                    ] = """
%s
    def jobs(self):
        return self.%s()
""" % (
                        indentfunc,
                        func,
                    )
                else:
                    confdict["BASE_CLASSES"] = "biothings.hub.dataload.uploader.BaseSourceUploader"
                    confdict["JOBS_FUNC"] = ""

                if uploader_section.get("mapping"):
                    indentfunc, func = self.get_code_for_mod_name(uploader_section["mapping"])
                    assert func != "get_mapping", "'get_mapping' is a reserved class method name, pick another name"
                    confdict[
                        "MAPPING_FUNC"
                    ] = """
    @classmethod
%s

    @classmethod
    def get_mapping(cls):
        return cls.%s()
""" % (
                        indentfunc,
                        func,
                    )
                else:
                    confdict["MAPPING_FUNC"] = ""

                pystr = tpl.substitute(confdict)
                # print(pystr)
                code = compile(pystr, "<string>", "exec")
                spec = importlib.util.spec_from_loader(self.plugin_name + sub_source_name, loader=None)
                mod = importlib.util.module_from_spec(spec)
                exec(code, mod.__dict__, mod.__dict__)
                uklass = getattr(mod, uploader_name)
                # we need to inherit from a class here in this file so it can be pickled
                assisted_uploader_class = type(
                    "AssistedUploader_%s" % self.plugin_name + sub_source_name,
                    (
                        AssistedUploader,
                        uklass,
                    ),
                    {},
                )
                assisted_uploader_class.python_code = pystr

                return assisted_uploader_class

            except Exception as e:
                self.logger.exception("Error loading plugin: %s" % e)
                raise AssistantException("Can't interpret manifest: %s" % e)
        else:
            raise AssistantException("Invalid manifest, expecting 'parser' key in 'uploader' section")

    def get_uploader_dynamic_classes(self, uploader_section, metadata, data_plugin_folder):
        uploader_classes = []
        for uploader_conf in uploader_section:
            sub_source_name = uploader_conf.get("name", "")
            uploader_class = self.get_uploader_dynamic_class(uploader_conf, metadata, sub_source_name)
            uploader_class.DATA_PLUGIN_FOLDER = data_plugin_folder

            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedUploader_%s" % self.plugin_name + sub_source_name
            ] = uploader_class

            uploader_classes.append(uploader_class)
        return uploader_classes

    def interpret_manifest(self, manifest, data_plugin_folder):
        # start with requirements before importing anything
        if manifest.get("requires"):
            reqs = manifest["requires"]
            if not type(reqs) == list:
                reqs = [reqs]
            for req in reqs:
                self.logger.info("Install requirement '%s'" % req)
                subprocess.check_call([sys.executable, "-m", "pip", "install", req])
        if manifest.get("dumper"):
            assisted_dumper_class = self.get_dumper_dynamic_class(manifest["dumper"], manifest.get("__metadata__"))
            assisted_dumper_class.DATA_PLUGIN_FOLDER = data_plugin_folder
            self.__class__.dumper_manager.register_classes([assisted_dumper_class])
            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedDumper_%s" % self.plugin_name
            ] = assisted_dumper_class

        if manifest.get("uploader"):
            assisted_uploader_class = self.get_uploader_dynamic_class(
                manifest["uploader"], manifest.get("__metadata__")
            )
            assisted_uploader_class.DATA_PLUGIN_FOLDER = data_plugin_folder
            self.__class__.uploader_manager.register_classes([assisted_uploader_class])
            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedUploader_%s" % self.plugin_name
            ] = assisted_uploader_class
        if manifest.get("uploaders"):
            assisted_uploader_classes = self.get_uploader_dynamic_classes(
                manifest["uploaders"], manifest.get("__metadata__"), data_plugin_folder
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

    def can_load_plugin(self):
        plugin = self.get_plugin_obj()
        df = plugin["download"]["data_folder"]
        if "__init__.py" in os.listdir(df):
            return True
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
            self.logger.info("Registering '%s' to dump/upload managers" % modpath)
            # register dumpers if any
            try:
                self.__class__.dumper_manager.register_source(modpath)
            except Exception as e:
                self.logger.info("Couldn't register dumper from module '%s': %s" % (modpath, e))
            # register uploaders if any
            try:
                self.__class__.uploader_manager.register_source(modpath)
            except Exception as e:
                self.logger.info("Couldn't register uploader from module '%s': %s" % (modpath, e))
        else:
            self.invalidate_plugin("Missing plugin folder '%s'" % df)


class BaseAssistant(object):
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
        """Setup and return a logger instance"""
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


class AssistedDumper(object):
    DATA_PLUGIN_FOLDER = None


class AssistedUploader(object):
    DATA_PLUGIN_FOLDER = None


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
                self._plugin_name = get_plugin_name_from_local_manifest(
                    os.path.join(btconfig.DATA_PLUGIN_FOLDER, src_folder_name)
                ) or src_folder_name
            except Exception as ex:
                self.logger.exception(ex)
                self._plugin_name = src_folder_name
            self._src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, src_folder_name)
        return self._plugin_name

    def can_handle(self):
        if self.url.startswith(self.__class__.plugin_type + "://"):
            return True
        else:
            return False

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
