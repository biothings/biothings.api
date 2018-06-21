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
from biothings.utils.loggers import HipchatHandler
from biothings.hub import DUMPER_CATEGORY, UPLOADER_CATEGORY
from config import logger as logging, HIPCHAT_CONFIG, LOG_FOLDER, \
                             DATA_PLUGIN_FOLDER, DATA_ARCHIVE_ROOT

from biothings.utils.manager import BaseSourceManager
from biothings.hub.dataload.uploader import set_pending_to_upload
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper, LastModifiedFTPDumper
from biothings.hub.dataload.uploader import BaseSourceUploader
from biothings.hub.dataload.storage import IgnoreDuplicatedStorage, BasicStorage
from biothings.hub.dataplugin.manager import GitDataPlugin, ManualDataPlugin


class AssistantException(Exception):
    pass


class BaseAssistant(object):

    plugin_type = None # to be defined in subblass
    data_plugin_manager = None # set by assistant manager
    dumper_manager = None # set by assistant manager
    uploader_manager = None # set by assistant manager
    # should match a _dict_for_***
    dumper_registry = {"http"  : LastModifiedHTTPDumper,
                       "https" : LastModifiedHTTPDumper,
                       "ftp"   : LastModifiedFTPDumper}

    def _dict_for_base(self,data_url):
        return {
                "SRC_NAME" : self.plugin_name,
                "SRC_ROOT_FOLDER" : os.path.join(DATA_ARCHIVE_ROOT,self.plugin_name),
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
            logging.warning("Can't find data_folder, not available yet ?")
            return
        df = p["download"]["data_folder"]
        if os.path.exists(df):
            mf = os.path.join(df,"manifest.json")
            if os.path.exists(mf):
                manifest = json.load(open(mf))
                logging.debug("Loading manifest: %s" % pprint.pformat(manifest))
                self.interpret_manifest(manifest)
            else:
                logging.info("No manifest found for plugin: %s" % p["plugin"]["url"])
        else:
            raise FileNotFoundError("Data plugin '%s' says data folder is 's%' but it doesn't exist" % \
                    (p["plugin"]["url"],df))

    def interpret_manifest(self, manifest):
        # dumper section: generate 
        if manifest.get("dumper"):
            if manifest["dumper"].get("data_url"):
                if not type(manifest["dumper"]["data_url"]) is list:
                    durls = [manifest["dumper"]["data_url"]]
                else:
                    durls = manifest["dumper"]["data_url"]
                schemes = set([urllib.parse.urlsplit(durl).scheme for durl in durls])
                # https = http regarding dumper generation
                if len([sch.replace("https","http") for sch in schemes]) > 1:
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
                k = type("AssistedDumper_%s" % self.plugin_name,(AssistedDumper,dumper_class,),confdict)
                if manifest["dumper"].get("uncompress"):
                    k.UNCOMPRESS = True
                self.__class__.dumper_manager.register_classes([k])
                # register class in module so it can be pickled easily
                sys.modules["biothings.hub.dataplugin.assistant"].__dict__["AssistedDumper_%s" % self.plugin_name] = k
            else:
                raise AssistantException("Invalid manifest, expecting 'data_url' key in 'dumper' section")
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
                    storage_class = manifest["uploader"].get("ignore_duplicates") \
                            and IgnoreDuplicatedStorage or BasicStorage
                    confdict = {"name":self.plugin_name,"storage_class":storage_class, "parser_func":parser_func}
                    k = type("AssistedUploader_%s" % self.plugin_name,(AssistedUploader,),confdict)
                    self.__class__.uploader_manager.register_classes([k])
                    # register class in module so it can be pickled easily
                    sys.modules["biothings.hub.dataplugin.assistant"].__dict__["AssistedUploader_%s" % self.plugin_name] = k
                except Exception as e:
                    logging.exception("Error loading plugin: %s" % e)
                    raise AssistantException("Can't interpret manifest: %s" % e)
            else:
                raise AssistantException("Invalid manifest, expecting 'parser' key in 'uploader' section")

class AssistedDumper(object):
    UNCOMPRESS = False
    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" % self.new_data_folder)
            uncompressall(self.new_data_folder)

class AssistedUploader(BaseSourceUploader):
    storage_class = None
    parser_func = None
    def load_data(self,data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return self.__class__.parser_func(data_folder)

class GithubAssistant(BaseAssistant):

    plugin_type = "github"

    def can_handle(self):
        # analyze headers to guess type of required assitant
        try:
            headers = requests.head(self.url).headers
            if headers.get("server").lower() == "github.com":
                return True
        except Exception as e:
            logging.info("%s plugin can't handle URL '%s': %s" % (self.plugin_type,self.url,e))
            return False

    def get_classdef(self):
        # generate class dynamically and register
        src_folder = os.path.join(DATA_PLUGIN_FOLDER, self.plugin_name)
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
            self._plugin_name = os.path.basename(split.hostname)
        return self._plugin_name

    def can_handle(self):
        if self.url.startswith("local://"):
            return True
        else:
            return False

    def get_classdef(self):
        # generate class dynamically and register
        src_folder = os.path.join(DATA_PLUGIN_FOLDER, self.plugin_name)
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
                 *args, **kwargs):
        super(AssistantManager,self).__init__(*args,**kwargs)
        self.data_plugin_manager = data_plugin_manager
        self.dumper_manager = dumper_manager
        self.uploader_manager = uploader_manager
        if not os.path.exists(DATA_PLUGIN_FOLDER):
            os.makedirs(DATA_PLUGIN_FOLDER)
        # register data plugin folder in python path so we can import
        # plugins (sub-folders) as packages
        sys.path.insert(0,DATA_PLUGIN_FOLDER)

    def create_instance(self, klass, url):
        logging.debug("Creating new %s instance" % klass.__name__)
        return klass(url)

    def configure(self, klasses=[GithubAssistant,LocalAssistant]):
        self.register_classes(klasses)

    def register_classes(self, klasses):
        for klass in klasses:
            klass.data_plugin_manager = self.data_plugin_manager
            klass.dumper_manager = self.dumper_manager
            klass.uploader_manager = self.uploader_manager
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
        # should be only one but just in case
        dp.remove({"plugin.url":url})
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
                    logging.debug("Plugin '%s' loaded, now loading manifest" % assistant.plugin_name)
                    assistant.load_manifest()
                except Exception as e:
                    logging.exception("Unable to load plugin '%s': %s" % (assistant.plugin_name,e))
            job.add_done_callback(loaded)
            return job
        else:
            raise AssistantException("Could not find any assistant able to handle URL '%s'" % url)

    def load_plugin(self,plugin):
        ptype = plugin["plugin"]["type"]
        url = plugin["plugin"]["url"]
        if not plugin["plugin"]["active"]:
            logging.info("Data plugin '%s' is deactivated, skip" % url)
            return
        logging.info("Loading data plugin '%s' (type: %s)" % (url,ptype))
        if ptype in self.register:
            try:
                aklass = self.register[ptype]
                assistant = self.create_instance(aklass,url)
                assistant.handle()
                assistant.load_manifest()
            except Exception as e:
                logging.exception("Unable to load plugin '%s': %s" % (url,e))
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
                plugin_dirs = os.listdir(DATA_PLUGIN_FOLDER)
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
                logging.warning("Couldn't load plugin '%s': %s" % (plugin["_id"],e))
                continue

        # some still unregistered ? (note: list always empty if autodiscover=False)
        if plugin_dirs:
            for pdir in plugin_dirs:
                fulldir = os.path.join(DATA_PLUGIN_FOLDER, pdir)
                # basic sanity check to make sure it's plugin
                if "manifest.json" in os.listdir(fulldir) and \
                        json.load(open(os.path.join(fulldir,"manifest.json"))):
                    logging.info("Found unregistered plugin '%s', auto-register it" % pdir)
                try:
                    self.register_url("local://%s" % pdir.strip().strip("/"))
                except Exception as e:
                    logging.warning("Couldn't auto-register plugin '%s': %s" % (pdir,e))
                    continue
                else:
                    logging.warning("Directory '%s' doesn't contain a plugin, skip it" % pdir)
                    continue

