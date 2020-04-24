import os
import pprint
import json
import sys
import re
import urllib.parse
import requests
import inspect
import importlib
import textwrap
import pip  # noqa: F401
import subprocess
import copy
from string import Template
from yapf.yapflib import yapf_api

from biothings.utils.hub_db import get_data_plugin, get_src_dump, get_src_master
from biothings.utils.common import rmdashfr, get_class_from_classpath
from biothings.utils.loggers import get_logger
from biothings import config as btconfig

from biothings.utils.manager import BaseSourceManager
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper, LastModifiedFTPDumper  # noqa: F402
from biothings.hub.dataload.uploader import BaseSourceUploader, ParallelizedSourceUploader  # noqa: F401
from biothings.hub.dataload.storage import IgnoreDuplicatedStorage, BasicStorage, MergerStorage  # noqa: F401
from biothings.hub.dataplugin.manager import GitDataPlugin, ManualDataPlugin


class AssistantException(Exception):
    pass


class BaseAssistant(object):

    plugin_type = None  # to be defined in subblass
    data_plugin_manager = None  # set by assistant manager
    dumper_manager = None  # set by assistant manager
    uploader_manager = None  # set by assistant manager
    keylookup = None  # set by assistant manager
    # should match a _dict_for_***
    dumper_registry = {
        "http": LastModifiedHTTPDumper,
        "https": LastModifiedHTTPDumper,
        "ftp": LastModifiedFTPDumper
    }

    def _dict_for_base(self, data_url):
        if type(data_url) == str:
            data_url = [data_url]
        return {
            "SRC_NAME":
            self.plugin_name,
            "SRC_ROOT_FOLDER":
            os.path.join(btconfig.DATA_ARCHIVE_ROOT, self.plugin_name),
            "SRC_URLS":
            data_url
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

    def __init__(self, url):
        self.url = url
        self._plugin_name = None
        self._src_folder = None
        self.logfile = None
        self.setup_log()

    def setup_log(self):
        """Setup and return a logger instance"""
        self.logger, self.logfile = get_logger('assistant_%s' %
                                               self.__class__.plugin_type)

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


class ManifestBasedPluginAssistant(BaseAssistant):

    def load_plugin(self):
        dp = get_data_plugin()
        p = dp.find_one({"_id": self.plugin_name})
        if not p.get("download", {}).get("data_folder"):
            # not yet available
            self.logger.warning("Can't find data_folder, not available yet ?")
            return
        df = p["download"]["data_folder"]
        if os.path.exists(df):
            mf = os.path.join(df, "manifest.json")
            if os.path.exists(mf):
                try:
                    self.logger.debug("Loading manifest: %s" % mf)
                    self.interpret_manifest(mf)
                except Exception as e:
                    self.invalidate_plugin("Error loading manifest: %s" %
                                           str(e))
            else:
                self.logger.info("No manifest found for plugin: %s" %
                                 p["plugin"]["url"])
                self.invalidate_plugin("No manifest found")
        else:
            self.invalidate_plugin("Missing plugin folder '%s'" % df)

    def invalidate_plugin(self, error):
        self.logger.exception("Invalidate plugin '%s' because: %s" %
                              (self.plugin_name, error))
        # flag all plugin associated (there should only one though, but no need to care here)
        for klass in self.__class__.data_plugin_manager[self.plugin_name]:
            klass.data_plugin_error = error
        pass

    def get_code_for_mod_name(self, mod_name):
        try:
            mod, funcname = map(str.strip, mod_name.split(":"))
        except ValueError as e:
            raise AssistantException("'Wrong format for '%s', it must be defined following format 'module:func': %s" % (mod_name, e))
        modpath = self.plugin_name + "." + mod
        pymod = importlib.import_module(modpath)
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
            schemes = set(
                [urllib.parse.urlsplit(durl).scheme for durl in durls])
            # https = http regarding dumper generation
            if len(set([sch.replace("https", "http") for sch in schemes])) > 1:
                raise AssistantException("Manifest specifies URLs of different types (%s), " % schemes
                                         + "expecting only one")
            scheme = schemes.pop()
            klass = dumper_section.get("class")
            confdict = getattr(self, "_dict_for_%s" % scheme)(durls)
            dumper_class = None
            if klass:
                dumper_class = get_class_from_classpath(klass)
                confdict["BASE_CLASSES"] = klass
            else:
                dumper_class = self.dumper_registry.get(scheme)
                confdict[
                    "BASE_CLASSES"] = "biothings.hub.dataload.dumper.%s" % dumper_class.__name__
            if not dumper_class:
                raise AssistantException(
                    "No dumper class registered to handle scheme '%s'" %
                    scheme)
            if metadata:
                confdict["__metadata__"] = metadata
            else:
                confdict["__metadata__"] = {}

            if dumper_section.get("release"):
                indentfunc, func = self.get_code_for_mod_name(dumper_section["release"])
                assert func != "set_release", "'set_release' is a reserved method name, pick another name"
                confdict["SET_RELEASE_FUNC"] = """
%s

    def set_release(self):
        self.release = self.%s()
""" % (indentfunc, func)

            else:
                confdict["SET_RELEASE_FUNC"] = ""

            dklass = None
            pnregex = r"^[A-z_][\w\d]+$"
            assert re.compile(pnregex).match(self.plugin_name), \
                "Incorrect plugin name '%s' (doesn't match regex '%s'" % (self.plugin_name, pnregex)
            dumper_name = self.plugin_name.capitalize() + "Dumper"
            '%s'
            try:
                if hasattr(btconfig, "DUMPER_TEMPLATE"):
                    tpl_file = btconfig.DUMPER_TEMPLATE
                else:
                    # default: assuming in ..../biothings/hub/dataplugin/
                    curmodpath = os.path.realpath(__file__)
                    tpl_file = os.path.join(os.path.dirname(curmodpath),
                                            "dumper.py.tpl")
                tpl = Template(open(tpl_file).read())
                confdict["DUMPER_NAME"] = dumper_name
                confdict["SRC_NAME"] = self.plugin_name
                if dumper_section.get("schedule"):
                    schedule = """'%s'""" % dumper_section["schedule"]
                else:
                    schedule = "None"
                confdict["SCHEDULE"] = schedule
                confdict["UNCOMPRESS"] = dumper_section.get(
                    "uncompress") or False
                pystr = tpl.substitute(confdict)
                #print(pystr)
                import imp
                code = compile(pystr, "<string>", "exec")
                mod = imp.new_module(self.plugin_name)
                exec(code, mod.__dict__, mod.__dict__)
                dklass = getattr(mod, dumper_name)
                # we need to inherit from a class here in this file so it can be pickled
                assisted_dumper_class = type(
                    "AssistedDumper_%s" % self.plugin_name, (
                        AssistedDumper,
                        dklass,
                    ), {})
                assisted_dumper_class.python_code = pystr

                return assisted_dumper_class

            except Exception:
                self.logger.exception("Can't generate dumper code for '%s'" % self.plugin_name)
                raise
        else:
            raise AssistantException(
                "Invalid manifest, expecting 'data_url' key in 'dumper' section"
            )

    def get_uploader_dynamic_class(self, uploader_section, metadata):
        if uploader_section.get("parser"):
            uploader_name = self.plugin_name.capitalize() + "Uploader"
            confdict = {
                "SRC_NAME": self.plugin_name,
                "UPLOADER_NAME": uploader_name
            }
            try:
                mod, func = uploader_section.get("parser").split(":")
                confdict["PARSER_MOD"] = mod
                confdict["PARSER_FUNC"] = func
            except ValueError:
                raise AssistantException("'parser' must be defined as 'module:parser_func' but got: '%s'" %
                                         uploader_section["parser"])
            try:
                ondups = uploader_section.get("on_duplicates")
                if ondups and ondups != "error":
                    if ondups == "merge":
                        storage_class = "biothings.hub.dataload.storage.MergerStorage"
                    elif ondups == "ignore":
                        storage_class = "biothings.hub.dataload.storage.IgnoreDuplicatedStorage"
                else:
                    storage_class = "biothings.hub.dataload.storage.BasicStorage"
                if uploader_section.get("ignore_duplicates"):
                    raise AssistantException(
                        "'ignore_duplicates' key not supported anymore, "
                        + "use 'on_duplicates' : 'error|ignore|merge'")
                confdict["STORAGE_CLASS"] = storage_class
                # default is not ID conversion at all
                confdict["IMPORT_IDCONVERTER_FUNC"] = ""
                confdict["IDCONVERTER_FUNC"] = None
                confdict["CALL_PARSER_FUNC"] = "parser_func(data_folder)"
                if uploader_section.get("keylookup"):
                    assert self.__class__.keylookup, "Plugin %s needs _id conversion " % self.plugin_name + \
                                                     "but no keylookup instance was found"
                    self.logger.info("Keylookup conversion required: %s" %
                                     uploader_section["keylookup"])
                    klmod = inspect.getmodule(self.__class__.keylookup)
                    confdict[
                        "IMPORT_IDCONVERTER_FUNC"] = "from %s import %s" % (
                            klmod.__name__, self.__class__.keylookup.__name__)
                    convargs = ",".join([
                        "%s=%s" % (k, v)
                        for k, v in uploader_section["keylookup"].items()
                    ])
                    confdict["IDCONVERTER_FUNC"] = "%s(%s)" % (
                        self.__class__.keylookup.__name__, convargs)
                    confdict[
                        "CALL_PARSER_FUNC"] = "self.__class__.idconverter(parser_func)(data_folder)"
                if metadata:
                    confdict["__metadata__"] = metadata
                else:
                    confdict["__metadata__"] = {}

                if hasattr(btconfig, "DUMPER_TEMPLATE"):
                    tpl_file = btconfig.DUMPER_TEMPLATE
                else:
                    # default: assuming in ..../biothings/hub/dataplugin/
                    curmodpath = os.path.realpath(__file__)
                    tpl_file = os.path.join(os.path.dirname(curmodpath),
                                            "uploader.py.tpl")
                tpl = Template(open(tpl_file).read())

                if uploader_section.get("parallelizer"):
                    indentfunc, func = self.get_code_for_mod_name(uploader_section["parallelizer"])
                    assert func != "jobs", "'jobs' is a reserved method name, pick another name"
                    confdict[
                        "BASE_CLASSES"] = "biothings.hub.dataload.uploader.ParallelizedSourceUploader"
                    confdict["IMPORT_FROM_PARALLELIZER"] = ""
                    confdict["JOBS_FUNC"] = """
%s
    def jobs(self):
        return self.%s()
""" % (indentfunc, func)
                else:
                    confdict[
                        "BASE_CLASSES"] = "biothings.hub.dataload.uploader.BaseSourceUploader"
                    confdict["JOBS_FUNC"] = ""

                if uploader_section.get("mapping"):
                    indentfunc, func = self.get_code_for_mod_name(uploader_section["mapping"])
                    assert func != "get_mapping", "'get_mapping' is a reserved class method name, pick another name"
                    confdict["MAPPING_FUNC"] = """
    @classmethod
%s

    @classmethod
    def get_mapping(cls):
        return cls.%s()
""" % (indentfunc, func)
                else:
                    confdict["MAPPING_FUNC"] = ""

                pystr = tpl.substitute(confdict)
                #print(pystr)
                import imp
                code = compile(pystr, "<string>", "exec")
                mod = imp.new_module(self.plugin_name)
                exec(code, mod.__dict__, mod.__dict__)
                uklass = getattr(mod, uploader_name)
                # we need to inherit from a class here in this file so it can be pickled
                assisted_uploader_class = type(
                    "AssistedUploader_%s" % self.plugin_name, (
                        AssistedUploader,
                        uklass,
                    ), {})
                assisted_uploader_class.python_code = pystr

                return assisted_uploader_class

            except Exception as e:
                self.logger.exception("Error loading plugin: %s" % e)
                raise AssistantException("Can't interpret manifest: %s" % e)
        else:
            raise AssistantException(
                "Invalid manifest, expecting 'parser' key in 'uploader' section"
            )

    def interpret_manifest(self, manifest_file):
        manifest = json.load(open(manifest_file))
        # start with requirements before importing anything
        if manifest.get("requires"):
            reqs = manifest["requires"]
            if not type(reqs) == list:
                reqs = [reqs]
            for req in reqs:
                self.logger.info("Install requirement '%s'" % req)
                subprocess.check_call(
                    [sys.executable, '-m', 'pip', 'install', req])
        if manifest.get("dumper"):
            assisted_dumper_class = self.get_dumper_dynamic_class(
                manifest["dumper"], manifest.get("__metadata__"))
            assisted_dumper_class.DATA_PLUGIN_FOLDER = os.path.dirname(
                manifest_file)
            self.__class__.dumper_manager.register_classes(
                [assisted_dumper_class])
            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedDumper_%s" % self.plugin_name] = assisted_dumper_class

        if manifest.get("uploader"):
            assisted_uploader_class = self.get_uploader_dynamic_class(
                manifest["uploader"], manifest.get("__metadata__"))
            assisted_uploader_class.DATA_PLUGIN_FOLDER = os.path.dirname(
                manifest_file)
            self.__class__.uploader_manager.register_classes(
                [assisted_uploader_class])
            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__[
                "AssistedUploader_%s" %
                self.plugin_name] = assisted_uploader_class


class AdvancedPluginAssistant(BaseAssistant):

    def load_plugin(self):
        dp = get_data_plugin()
        p = dp.find_one({"_id": self.plugin_name})
        if not p.get("download", {}).get("data_folder"):
            # not yet available
            self.logger.warning("Can't find data_folder, not available yet ?")
            return
        df = p["download"]["data_folder"]
        if os.path.exists(df):
            # we assume there's a __init__ module exposing Dumper and Uploader classes
            # as necessary
            # we could strip and split on "/" manually but let's use os.path for that
            # just to make sure we'll handle any cases (supposedly)
            tmpdf = copy.deepcopy(df)
            path_elements = []
            while tmpdf:
                tmpdf, elem = os.path.split(tmpdf)
                if elem == ".":
                    continue
                path_elements.append(elem)
            path_elements.reverse()  # we got elems from the end, back to forward
            modpath = ".".join(path_elements)
            # before registering, process optional requirements.txt
            reqfile = os.path.join(df,"requirements.txt")
            if os.path.exists(reqfile):
                self.logger.info("Installing requirements from %s for plugin '%s'" % (reqfile,self.plugin_name))
                subprocess.check_call([sys.executable,"-m","pip","install","-r",reqfile])
            # submit to managers to register datasources
            self.logger.info("Registering '%s' to dump/upload managers" % modpath)
            self.__class__.dumper_manager.register_source(modpath)
            self.__class__.uploader_manager.register_source(modpath)
        else:
            self.invalidate_plugin("Missing plugin folder '%s'" % df)


class AssistedDumper(object):
    DATA_PLUGIN_FOLDER = None


class AssistedUploader(object):
    DATA_PLUGIN_FOLDER = None


class GithubAssistant(ManifestBasedPluginAssistant):

    plugin_type = "github"

    @property
    def plugin_name(self):
        if not self._plugin_name:
            split = urllib.parse.urlsplit(self.url)
            self._plugin_name = os.path.basename(split.path).replace(".git", "")
            self._src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, self.plugin_name)
        return self._plugin_name

    def can_handle(self):
        # analyze headers to guess type of required assitant
        try:
            headers = requests.head(self.url).headers
            if headers.get("server").lower() == "github.com":
                return True
        except Exception as e:
            self.logger.error("%s plugin can't handle URL '%s': %s" %
                             (self.plugin_type, self.url, e))
            return False

    def get_classdef(self):
        # generate class dynamically and register
        confdict = {
            "SRC_NAME": self.plugin_name,
            "GIT_REPO_URL": self.url,
            "SRC_ROOT_FOLDER": self._src_folder
        }
        # TODO: store confdict in hubconf collection
        k = type("AssistedGitDataPlugin_%s" % self.plugin_name,
                 (GitDataPlugin, ), confdict)
        return k

    def handle(self):
        assert self.__class__.data_plugin_manager, "Please set data_plugin_manager attribute"
        klass = self.get_classdef()
        self.__class__.data_plugin_manager.register_classes([klass])


class LocalAssistant(ManifestBasedPluginAssistant):

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
            assert not split.path, "It seems URL '%s' references a sub-directory (%s)," % (self.url, split.hostname) \
                                   + " with plugin name '%s', sub-directories are not supported (yet)" % split.path.strip("/")
            # don't use hostname here because it's lowercased, netloc isn't
            # (and we're matching directory names on the filesystem, it's case-sensitive)
            self._plugin_name = os.path.basename(split.netloc)
            self._src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, self._plugin_name)
        return self._plugin_name

    def can_handle(self):
        if self.url.startswith(self.__class__.plugin_type + "://"):
            return True
        else:
            return False

    def get_classdef(self):
        # generate class dynamically and register
        confdict = {
            "SRC_NAME": self.plugin_name,
            "SRC_ROOT_FOLDER": self._src_folder
        }
        k = type("AssistedManualDataPlugin_%s" % self.plugin_name,
                 (ManualDataPlugin, ), confdict)
        return k

    def handle(self):
        assert self.__class__.data_plugin_manager, "Please set data_plugin_manager attribute"
        klass = self.get_classdef()
        self.__class__.data_plugin_manager.register_classes([klass])


class AdvancedLocalAssistant(AdvancedPluginAssistant, LocalAssistant):

    plugin_type = "advanced-local"

    #def can_handle(self):
    #    return super(LocalAssistant,self).can_handle()

    @property
    def plugin_name(self):
        if not self._plugin_name:
            split = urllib.parse.urlsplit(self.url)
            path, src_name = os.path.split(split.path)
            assert path.endswith("datasources"), "Expecting a folder name 'datasources' in path '%s'" % self.url
            self._plugin_name = src_name
            # netloc is '.' when plugin dir is relative
            # Keep it so path doesn't become absolute (netloc is '' when absolute so it's safe)
            self._src_folder = split.netloc + split.path
        return self._plugin_name


class AssistantManager(BaseSourceManager):

    def __init__(self,
                 data_plugin_manager,
                 dumper_manager,
                 uploader_manager,
                 keylookup=None,
                 default_export_folder="hub/dataload/sources",
                 *args,
                 **kwargs):
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
        self.logger, self.logfile = get_logger('assistantmanager')

    def create_instance(self, klass, url):
        return klass(url)

    def configure(self, klasses=[GithubAssistant, LocalAssistant, AdvancedLocalAssistant, ]):
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
            raise AssistantException(
                "Plugin is not registered (url=%s, name=%s)" % (url, name))
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
        if dp.find_one({"plugin.url": url}):
            self.logger.info("Plugin '%s' already registered" % url)
            return
        assistant = self.submit(url)
        if assistant:
            # register plugin info
            dp.update({"_id": assistant.plugin_name}, {
                "$set": {
                    "plugin": {
                        "url": url,
                        "type": assistant.plugin_type,
                        "active": True
                    }
                }
            },
                upsert=True)
            assistant.handle()
            job = self.data_plugin_manager.load(assistant.plugin_name)
            assert len(job) == 1, "Expecting one job, got: %s" % job
            job = job.pop()

            def loaded(f):
                try:
                    _ = f.result()
                    self.logger.debug(
                        "Plugin '%s' loaded, now loading manifest" %
                        assistant.plugin_name)
                    assistant.load_plugin()
                except Exception as e:
                    self.logger.exception("Unable to load plugin '%s': %s" %
                                          (assistant.plugin_name, e))

            job.add_done_callback(loaded)
            return job
        else:
            raise AssistantException(
                "Could not find any assistant able to handle URL '%s'" % url)

    def load_plugin(self, plugin):
        ptype = plugin["plugin"]["type"]
        url = plugin["plugin"]["url"]
        if not plugin["plugin"]["active"]:
            self.logger.info("Data plugin '%s' is deactivated, skip" % url)
            return
        self.logger.info("Loading data plugin '%s' (type: %s)" % (url, ptype))
        if ptype in self.register:
            try:
                aklass = self.register[ptype]
                assistant = self.create_instance(aklass, url)
                assistant.handle()
                assistant.load_plugin()
            except Exception as e:
                self.logger.exception("Unable to load plugin '%s': %s" %
                                      (url, e))
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
            # remove plugins from folder list if already register
            if plugin_dirs and plugin["_id"] in plugin_dirs:
                plugin_dirs.remove(plugin["_id"])
            try:
                self.load_plugin(plugin)
            except Exception as e:
                self.logger.warning("Couldn't load plugin '%s': %s" %
                                    (plugin["_id"], e))
                continue

        # some still unregistered ? (note: list always empty if autodiscover=False)
        if plugin_dirs:
            for pdir in plugin_dirs:
                fulldir = os.path.join(btconfig.DATA_PLUGIN_FOLDER, pdir)
                # basic sanity check to make sure it's plugin
                try:
                    if "manifest.json" in os.listdir(fulldir) and \
                       json.load(open(os.path.join(fulldir, "manifest.json"))):
                        self.logger.info(
                            "Found unregistered manifest-based plugin '%s', auto-register it"
                            % pdir)
                        self.register_url("local://%s" %
                                          pdir.strip().strip("/"))
                    elif "datasources" in os.listdir(fulldir) and os.path.isdir(os.path.join(fulldir,"datasources")):
                        sources_folder = os.path.join(fulldir,"datasources")
                        for src_folder in os.listdir(sources_folder):
                            advanced_src_folder = os.path.join(sources_folder,src_folder)
                            self.logger.info(
                                "Found unregister plugin (\"advanced\") in '%s', auto-register it"
                                % advanced_src_folder)
                            self.register_url("advanced-local://%s" % advanced_src_folder.strip().strip("/"))

                except Exception as e:
                    self.logger.exception(
                        "Couldn't auto-register plugin '%s': %s" % (pdir, e))
                    continue

    def export_dumper(self, plugin_name, folder):
        res = {
            "dumper": {
                "status": None,
                "file": None,
                "class": None,
                "message": None
            }
        }
        try:
            dclass = self.dumper_manager[plugin_name]
        except KeyError:
            res["dumper"]["status"] = "warning"
            res["dumper"][
                "message"] = "No dumper found for plugin '%s'" % plugin_name
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
            beauty, _ = yapf_api.FormatCode(dclass.python_code)
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
        res = {
            "uploader": {
                "status": None,
                "file": None,
                "class": None,
                "message": None
            }
        }
        try:
            uclass = self.uploader_manager[plugin_name]
        except KeyError:
            res["uploader"]["status"] = "warning"
            res["uploader"][
                "message"] = "No uploader found for plugin '%s'" % plugin_name
            return res
        try:
            uploader_name = plugin_name.capitalize() + "Uploader"
            self.logger.debug("Exporting uploader %s" % uploader_name)
            assert len(
                uclass) == 1, "More than one uploader found: %s" % uclass
            uclass = uclass[0]
            assert hasattr(uclass, "python_code"), "No generated code found"
            dinit = os.path.join(folder, "__init__.py")
            ufile = os.path.join(folder, "upload.py")
            beauty, _ = yapf_api.FormatCode(uclass.python_code)
            with open(ufile, "w") as fout:
                fout.write(beauty)
            with open(dinit, "a") as fout:
                fout.write("from .upload import %s\n" % uploader_name)
            res["uploader"]["status"] = "ok"
            res["uploader"]["file"] = ufile
            res["uploader"]["class"] = uploader_name
        except Exception as e:
            res["uploader"]["status"] = "error"
            res["uploader"]["message"] = "Error exporting uploader: %s" % e
            return res

        return res

    def export_mapping(self, plugin_name, folder):
        res = {
            "mapping": {
                "status": None,
                "file": None,
                "message": None,
                "origin": None
            }
        }
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
            mapping = doc and doc.get("inspect", {}).get("jobs", {}).get(plugin_name, {}).get("inspect", {}).\
                get("results", {}).get("mapping")
            res["mapping"]["origin"] = "inspection"
        if not mapping:
            res["mapping"]["origin"] = None
            res["mapping"]["status"] = "warning"
            res["mapping"][
                "message"] = "Can't find registered or generated (inspection) mapping"
            return res
        else:
            ufile = os.path.join(folder, "upload.py")
            strmap, _ = yapf_api.FormatCode(pprint.pformat(mapping))
            with open(ufile, "a") as fout:
                fout.write("""
    @classmethod
    def get_mapping(klass):
        return %s\n""" % textwrap.indent((strmap), prefix="    " * 2))

        res["mapping"]["file"] = ufile
        res["mapping"]["status"] = "ok"

        return res

    def export(self,
               plugin_name,
               folder=None,
               what=["dumper", "uploader", "mapping"],
               purge=False):
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
        assert os.path.exists(
            folder
        ), "Folder used to export code doesn't exist: %s" % os.path.abspath(
            folder)
        assert plugin_name  # avoid deleting the whole export folder when purge=True...
        folder = os.path.join(folder, plugin_name)
        if purge:
            rmdashfr(folder)
        if not os.path.exists(folder):
            os.makedirs(folder)
        elif not purge:
            raise FileExistsError(
                "Folder '%s' already exists, use purge=True" % folder)
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
        plugin_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, plugin_name)
        for f in os.listdir(plugin_folder):
            src = os.path.join(plugin_folder, f)
            dst = os.path.join(folder, f)
            # useless or strictly plugin-machinery-specific, skip
            if f in ["__pycache__", "manifest.json", "__init__.py"
                     ] or f.startswith("."):
                self.logger.debug("Skipping '%s', not necessary" % src)
                continue
            self.logger.debug("Copying %s to %s" % (src, dst))
            try:
                with open(src) as fin:
                    with open(dst, "w") as fout:
                        fout.write(fin.read())
            except IsADirectoryError:
                self.logger.error(
                    "%s is a directory, expecting only files to copy" % src)
                continue

        return res
