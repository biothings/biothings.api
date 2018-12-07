import time, copy
import os, pprint, json, sys
import urllib.parse, requests
from datetime import datetime
import asyncio
from functools import partial
import inspect, importlib

from biothings.utils.hub_db import get_data_plugin
from biothings.utils.common import timesofar, rmdashfr, uncompressall, \
                                   get_class_from_classpath
from biothings.utils.loggers import get_logger
from biothings.hub import DUMPER_CATEGORY, UPLOADER_CATEGORY
from biothings import config as btconfig

from biothings.utils.manager import BaseSourceManager
from biothings.hub.dataload.uploader import set_pending_to_upload
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper, LastModifiedFTPDumper
from biothings.hub.dataload.uploader import BaseSourceUploader, ParallelizedSourceUploader
from biothings.hub.dataload.storage import IgnoreDuplicatedStorage, BasicStorage, \
                                           MergerStorage
from biothings.hub.dataplugin.manager import GitDataPlugin, ManualDataPlugin


class AssistantException(Exception):
    pass


class BaseAssistant(object):

    plugin_type = None # to be defined in subblass
    data_plugin_manager = None # set by assistant manager
    dumper_manager = None # set by assistant manager
    uploader_manager = None # set by assistant manager
    keylookup = None # set by assistant manager
    # should match a _dict_for_***
    dumper_registry = {"http"  : LastModifiedHTTPDumper,
                       "https" : LastModifiedHTTPDumper,
                       "ftp"   : LastModifiedFTPDumper}

    def _dict_for_base(self,data_url):
        if type(data_url) == str:
            data_url = [data_url]
        return {
                "SRC_NAME" : self.plugin_name,
                "SRC_ROOT_FOLDER" : os.path.join(btconfig.DATA_ARCHIVE_ROOT,self.plugin_name),
                "SRC_URLS" : data_url
                }

    def _dict_for_http(self, data_url):
        return self._dict_for_base(data_url)

    def _dict_for_https(self, data_url):
        d = self._dict_for_http(data_url)
        # not secure, but we want to make sure things will work as much as possible...
        d["VERIFY_CERT"] = False
        return d

    def _dict_for_ftp(self,data_url):
        return self._dict_for_base(data_url)

    def __init__(self, url):
        self.url = url
        self._plugin_name = None
        self.logfile = None
        self.setup_log()

    def setup_log(self):
        """Setup and return a logger instance"""
        self.logger, self.logfile = get_logger('assistant_%s' % self.__class__.plugin_type)

    @property
    def plugin_name(self):
        if not self._plugin_name:
            split = urllib.parse.urlsplit(self.url)
            self._plugin_name = os.path.basename(split.path).replace(".git","")
        return self._plugin_name

    def handle(self):
        """Access self.url and do whatever is necessary to bring code to life within the hub...
        (hint: that may involve creating a dumper on-the-fly and register that dumper to
        a manager...)
        """
        raise NotImplementedError("implement in subclass")

    def can_handle(self):
        """Return true if assistant can handle the code"""
        raise NotImplementedError("implement in subclass")

    def load_manifest(self):
        dp = get_data_plugin()
        p = dp.find_one({"_id":self.plugin_name})
        if not p.get("download",{}).get("data_folder"):
            # not yet available
            self.logger.warning("Can't find data_folder, not available yet ?")
            return
        df = p["download"]["data_folder"]
        if os.path.exists(df):
            mf = os.path.join(df,"manifest.json")
            if os.path.exists(mf):
                try:
                    manifest = json.load(open(mf))
                    self.logger.debug("Loading manifest: %s" % pprint.pformat(manifest))
                    self.interpret_manifest(manifest)
                except Exception as e:
                    self.invalidate_plugin("Error loading manifest: %s" % str(e))
            else:
                self.logger.info("No manifest found for plugin: %s" % p["plugin"]["url"])
                self.invalidate_plugin("No manifest found")
        else:
            self.invalidate_plugin("Missing plugin folder '%s'" % df)

    def invalidate_plugin(self,error):
        self.logger.warning("Invalidate plugin '%s' because: %s" % (self.plugin_name,error))
        # flag all plugin associated (there should only one though, but no need to care here)
        for klass in self.__class__.data_plugin_manager[self.plugin_name]:
            klass.data_plugin_error = error
        pass

    def interpret_manifest(self, manifest):
        # dumper section: generate 
        dumper_class = None
        assisted_dumper_class = None
        assisted_uploader_class = None
        if manifest.get("dumper"):
            if manifest["dumper"].get("data_url"):
                if not type(manifest["dumper"]["data_url"]) is list:
                    durls = [manifest["dumper"]["data_url"]]
                else:
                    durls = manifest["dumper"]["data_url"]
                schemes = set([urllib.parse.urlsplit(durl).scheme for durl in durls])
                # https = http regarding dumper generation
                if len(set([sch.replace("https","http") for sch in schemes])) > 1:
                    raise AssistantException("Manifest specifies URLs of different types (%s), " % schemes + \
                            "expecting only one")
                scheme = schemes.pop()
                klass = manifest["dumper"].get("class")
                if klass:
                    dumper_class = get_class_from_classpath(klass)
                else:
                    dumper_class = self.dumper_registry.get(scheme)
                if not dumper_class:
                    raise AssistantException("No dumper class registered to handle scheme '%s'" % scheme)
                confdict = getattr(self,"_dict_for_%s" % scheme)(durls)
                if manifest.get("__metadata__"):
                    confdict["__metadata__"] = {"src_meta" : manifest.get("__metadata__")}
                assisted_dumper_class = type("AssistedDumper_%s" % self.plugin_name,(AssistedDumper,dumper_class,),confdict)
                if manifest["dumper"].get("uncompress"):
                    assisted_dumper_class.UNCOMPRESS = True
                self.__class__.dumper_manager.register_classes([assisted_dumper_class])
                # register class in module so it can be pickled easily
                sys.modules["biothings.hub.dataplugin.assistant"].__dict__["AssistedDumper_%s" % self.plugin_name] = assisted_dumper_class
            else:
                raise AssistantException("Invalid manifest, expecting 'data_url' key in 'dumper' section")

            assert assisted_dumper_class
            if manifest["dumper"].get("release"):
                try:
                    mod,func = manifest["dumper"].get("release").split(":")
                except ValueError as e:
                    raise AssistantException("'release' must be defined as 'module:parser_func' but got: '%s'" % \
                            manifest["dumper"]["release"])
                try:
                    modpath = self.plugin_name + "." + mod
                    pymod = importlib.import_module(modpath)
                    # reload in case we need to refresh plugin's code
                    importlib.reload(pymod)
                    assert func in dir(pymod), "%s not found in module %s" % (func,pymod)
                    get_release_func = getattr(pymod,func)
                    # replace existing method to connect custom release setter
                    def set_release(self):
                        self.release = get_release_func(self)
                    assisted_dumper_class.set_release = set_release
                except Exception as e:
                    self.logger.exception("Error loading plugin: %s" % e)
                    raise AssistantException("Can't interpret manifest: %s" % e)
        if manifest.get("uploader"):
            if manifest["uploader"].get("parser"):
                try:
                    mod,func = manifest["uploader"].get("parser").split(":")
                except ValueError as e:
                    raise AssistantException("'parser' must be defined as 'module:parser_func' but got: '%s'" % \
                            manifest["uploader"]["parser"])
                try:
                    modpath = self.plugin_name + "." + mod
                    pymod = importlib.import_module(modpath)
                    # reload in case we need to refresh plugin's code
                    importlib.reload(pymod)
                    assert func in dir(pymod)
                    parser_func = getattr(pymod,func)
                    ondups = manifest["uploader"].get("on_duplicates")
                    if ondups and ondups != "error":
                        if ondups == "merge":
                            storage_class = MergerStorage
                        elif ondups == "ignore":
                            storage_class= IgnoreDuplicatedStorage
                    else:
                        storage_class = BasicStorage
                    if manifest["uploader"].get("ignore_duplicates"):
                        raise AssistantException("'ignore_duplicates' key not supported anymore, " +
                                                 "use 'on_duplicates' : 'error|ignore|merge'")
                    confdict = {"name":self.plugin_name,"storage_class":storage_class, "parser_func":parser_func}
                    if manifest["uploader"].get("keylookup"):
                        assert self.__class__.keylookup, "Plugin %s needs _id conversion " % self.plugin_name + \
                                                         "but no keylookup instance was found"
                        self.logger.info("Keylookup conversion required: %s" % manifest["uploader"]["keylookup"])
                        confdict["idconverter"] = self.__class__.keylookup(**manifest["uploader"]["keylookup"])
                    if manifest.get("__metadata__"):
                        confdict["__metadata__"] = {"src_meta" : manifest.get("__metadata__")}

                    if manifest["uploader"].get("parallelizer"):
                        assisted_uploader_class = type("AssistedUploader_%s" % self.plugin_name,(AssistedUploader,ParallelizedSourceUploader,),confdict)
                        try:
                            mod,func = manifest["uploader"].get("parallelizer").split(":")
                        except ValueError as e:
                            raise AssistantException("'parallelizer' must be defined as 'module:parallelizer_func' but got: '%s'" % \
                                    manifest["uploader"]["parallelizer"])
                        try:
                            modpath = self.plugin_name + "." + mod
                            pymod = importlib.import_module(modpath)
                            # reload in case we need to refresh plugin's code
                            importlib.reload(pymod)
                            assert func in dir(pymod), "%s not found in module %s" % (func,pymod)
                            jobs_func = getattr(pymod,func)
                            # replace existing method to connect jobs parallelized func
                            assisted_uploader_class.jobs = jobs_func
                        except Exception as e:
                            self.logger.exception("Error loading plugin: %s" % e)
                            raise AssistantException("Can't interpret manifest: %s" % e)
                    else:
                        assisted_uploader_class = type("AssistedUploader_%s" % self.plugin_name,(AssistedUploader,),confdict)
                    self.__class__.uploader_manager.register_classes([assisted_uploader_class])
                    # register class in module so it can be pickled easily
                    sys.modules["biothings.hub.dataplugin.assistant"].__dict__["AssistedUploader_%s" % self.plugin_name] = assisted_uploader_class
                except Exception as e:
                    self.logger.exception("Error loading plugin: %s" % e)
                    raise AssistantException("Can't interpret manifest: %s" % e)
            else:
                raise AssistantException("Invalid manifest, expecting 'parser' key in 'uploader' section")


class AssistedDumper(object):
    UNCOMPRESS = False
    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" % self.new_data_folder)
            uncompressall(self.new_data_folder)

# this is a transparent wrapper over parsing func, performining no conversion at all
def transparent(f):
    return f

class AssistedUploader(BaseSourceUploader):
    storage_class = None
    parser_func = None
    idconverter = transparent

    def load_data(self,data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return self.__class__.idconverter(self.__class__.parser_func)(data_folder)


class GithubAssistant(BaseAssistant):

    plugin_type = "github"

    def can_handle(self):
        # analyze headers to guess type of required assitant
        try:
            headers = requests.head(self.url).headers
            if headers.get("server").lower() == "github.com":
                return True
        except Exception as e:
            self.logger.info("%s plugin can't handle URL '%s': %s" % (self.plugin_type,self.url,e))
            return False

    def get_classdef(self):
        # generate class dynamically and register
        src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, self.plugin_name)
        confdict = {"SRC_NAME":self.plugin_name,"GIT_REPO_URL":self.url,"SRC_ROOT_FOLDER":src_folder}
        # TODO: store confdict in hubconf collection
        k = type("AssistedGitDataPlugin_%s" % self.plugin_name,(GitDataPlugin,),confdict)
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
            assert not split.path, "It seems URL '%s' references a sub-directory (%s)," % (self.url,split.hostname) + \
                    " with plugin name '%s', sub-directories are not supported (yet)" % split.path.strip("/")
            # don't use hostname here because it's lowercased, netloc isn't
            # (and we're matching directory names on the filesystem, it's case-sensitive)
            self._plugin_name = os.path.basename(split.netloc)
        return self._plugin_name

    def can_handle(self):
        if self.url.startswith("local://"):
            return True
        else:
            return False

    def get_classdef(self):
        # generate class dynamically and register
        src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, self.plugin_name)
        confdict = {"SRC_NAME":self.plugin_name,"SRC_ROOT_FOLDER":src_folder}
        k = type("AssistedManualDataPlugin_%s" % self.plugin_name,(ManualDataPlugin,),confdict)
        return k

    def handle(self):
        assert self.__class__.data_plugin_manager, "Please set data_plugin_manager attribute"
        klass = self.get_classdef()
        self.__class__.data_plugin_manager.register_classes([klass])


class AssistantManager(BaseSourceManager):

    ASSISTANT_CLASS = BaseAssistant

    def __init__(self, data_plugin_manager, dumper_manager, uploader_manager,
                 keylookup=None, *args, **kwargs):
        super(AssistantManager,self).__init__(*args,**kwargs)
        self.data_plugin_manager = data_plugin_manager
        self.dumper_manager = dumper_manager
        self.uploader_manager = uploader_manager
        self.keylookup = keylookup
        if not os.path.exists(btconfig.DATA_PLUGIN_FOLDER):
            os.makedirs(btconfig.DATA_PLUGIN_FOLDER)
        # register data plugin folder in python path so we can import
        # plugins (sub-folders) as packages
        sys.path.insert(0,btconfig.DATA_PLUGIN_FOLDER)
        self.logfile = None
        self.setup_log()

    def setup_log(self):
        """Setup and return a logger instance"""
        self.logger, self.logfile = get_logger('assistantmanager')

    def create_instance(self, klass, url):
        self.logger.debug("Creating new %s instance" % klass.__name__)
        return klass(url)

    def configure(self, klasses=[GithubAssistant,LocalAssistant]):
        self.register_classes(klasses)

    def register_classes(self, klasses):
        for klass in klasses:
            klass.data_plugin_manager = self.data_plugin_manager
            klass.dumper_manager = self.dumper_manager
            klass.uploader_manager = self.uploader_manager
            klass.keylookup = self.keylookup
            self.register[klass.plugin_type] = klass

    def submit(self,url):
        # submit url to all registered assistants (in order)
        # and return the first claiming it can handle that URLs
        for typ in self.register:
            aklass = self.register[typ]
            inst = self.create_instance(aklass,url)
            if inst.can_handle():
                return inst
        return None

    def unregister_url(self, url):
        url = url.strip()
        dp = get_data_plugin()
        doc = dp.find_one({"plugin.url":url})
        # should be only one but just in case
        dp.remove({"plugin.url":url})
        # delete plugin code so it won't be auto-register
        # by 'local' plugin assistant (issue studio #7)
        if doc.get("download",{}).get("data_folder"):
            codefolder = doc["download"]["data_folder"]
            self.logger.info("Delete plugin source code in '%s'" % codefolder)
            rmdashfr(codefolder)
        assistant = self.submit(url)
        try:
            self.data_plugin_manager.register.pop(assistant.plugin_name)
        except KeyError:
            raise AssistantException("Plugin '%s' is not registered" % url)
        self.dumper_manager.register.pop(assistant.plugin_name,None)
        self.uploader_manager.register.pop(assistant.plugin_name,None)

    def register_url(self, url):
        url = url.strip()
        dp = get_data_plugin()
        if dp.find_one({"plugin.url":url}):
            raise AssistantException("Plugin '%s' already registered" % url)
        assistant = self.submit(url)
        if assistant:
            # register plugin info
            dp.update({"_id":assistant.plugin_name},
                    {"$set":{"plugin":{"url":url,"type":assistant.plugin_type,"active":True}}},
                    upsert=True)
            assistant.handle()
            job = self.data_plugin_manager.load(assistant.plugin_name)
            assert len(job) == 1, "Expecting one job, got: %s" % job
            job = job.pop()
            def loaded(f):
                try:
                    res = f.result()
                    self.logger.debug("Plugin '%s' loaded, now loading manifest" % assistant.plugin_name)
                    assistant.load_manifest()
                except Exception as e:
                    self.logger.exception("Unable to load plugin '%s': %s" % (assistant.plugin_name,e))
            job.add_done_callback(loaded)
            return job
        else:
            raise AssistantException("Could not find any assistant able to handle URL '%s'" % url)

    def load_plugin(self,plugin):
        ptype = plugin["plugin"]["type"]
        url = plugin["plugin"]["url"]
        if not plugin["plugin"]["active"]:
            self.logger.info("Data plugin '%s' is deactivated, skip" % url)
            return
        self.logger.info("Loading data plugin '%s' (type: %s)" % (url,ptype))
        if ptype in self.register:
            try:
                aklass = self.register[ptype]
                assistant = self.create_instance(aklass,url)
                assistant.handle()
                assistant.load_manifest()
            except Exception as e:
                self.logger.exception("Unable to load plugin '%s': %s" % (url,e))
        else:
            raise AssistantException("Unknown data plugin type '%s'" % ptype)

    def load(self,autodiscover=True):
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
                self.logger.warning("Couldn't load plugin '%s': %s" % (plugin["_id"],e))
                continue

        # some still unregistered ? (note: list always empty if autodiscover=False)
        if plugin_dirs:
            for pdir in plugin_dirs:
                fulldir = os.path.join(btconfig.DATA_PLUGIN_FOLDER, pdir)
                # basic sanity check to make sure it's plugin
                try:
                    if "manifest.json" in os.listdir(fulldir) and \
                            json.load(open(os.path.join(fulldir,"manifest.json"))):
                        self.logger.info("Found unregistered plugin '%s', auto-register it" % pdir)
                        self.register_url("local://%s" % pdir.strip().strip("/"))
                except Exception as e:
                    self.logger.exception("Couldn't auto-register plugin '%s': %s" % (pdir,e))
                    continue
                else:
                    self.logger.warning("Directory '%s' doesn't contain a plugin, skip it" % pdir)
                    continue

