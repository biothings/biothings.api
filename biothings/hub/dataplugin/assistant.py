import time, copy
import os, pprint, json, sys
import urllib.parse, requests
from datetime import datetime
import asyncio
from functools import partial
import inspect

from biothings.utils.hub_db import get_data_plugin
from biothings.utils.common import timesofar, rmdashfr, uncompressall
from biothings.utils.loggers import HipchatHandler
from biothings.hub import DUMPER_CATEGORY, UPLOADER_CATEGORY
from config import logger as logging, HIPCHAT_CONFIG, LOG_FOLDER, \
                             DATA_PLUGIN_FOLDER, DATA_ARCHIVE_ROOT

from biothings.utils.manager import BaseSourceManager
from biothings.hub.dataload.uploader import set_pending_to_upload
from biothings.hub.dataload.dumper import LastModifiedHTTPDumper
from biothings.hub.dataplugin.manager import GitDataPlugin

class AssistantException(Exception):
    pass


class BaseAssistant(object):

    plugin_type = None # to be defined in subblass
    data_plugin_manager = None # set by assistant manager
    dumper_manager = None # set by assistant manager
    # should match a _dict_for_***
    dumper_registry = {"http"  : LastModifiedHTTPDumper,
                       "https" : LastModifiedHTTPDumper}

    def _dict_for_http(self, data_url):
        return {
                "SRC_NAME" : self.plugin_name,
                "SRC_ROOT_FOLDER" : os.path.join(DATA_ARCHIVE_ROOT,self.plugin_name),
                "SRC_URLS" : [data_url]
                }

    def _dict_for_https(self, data_url):
        d = self._dict_for_http(data_url)
        # not secure, but we want to make sure things will work as much as possible...
        d["VERIFY_CERT"] = False
        return d

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
        if not p.get("data_folder"):
            # not yet available
            return
        df = p["data_folder"]
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
        if manifest.get("dumper") and manifest["dumper"].get("data_url"):
            durl = manifest["dumper"]["data_url"]
            split = urllib.parse.urlsplit(self.url)
            dumper_class = self.dumper_registry.get(split.scheme)
            if not dumper_class:
                raise AssistantException("No dumper class registered to handle scheme '%s'" % split.scheme)
            confdict = getattr(self,"_dict_for_%s" % split.scheme)(durl)
            k = type("AssistedDumper_%s" % self.plugin_name,(AssistedDumper,dumper_class,),confdict)
            if manifest["dumper"].get("uncompress"):
                k.UNCOMPRESS = True
            self.__class__.dumper_manager.register_classes([k])
            # register class in module so it can be pickled easily
            sys.modules["biothings.hub.dataplugin.assistant"].__dict__["AssistedDumper_%s" % self.plugin_name] = k

class AssistedDumper(object):
    UNCOMPRESS = False
    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" % self.new_data_folder)
            uncompressall(self.new_data_folder)


class GithubAssistant(BaseAssistant):

    plugin_type = "github"

    def can_handle(self):
        # analyze headers to guess type of required assitant
        headers = requests.head(self.url).headers
        if headers.get("server").lower() == "github.com":
            return True

    def handle(self):
        assert self.__class__.data_plugin_manager, "Please set data_plugin_manager attribute"
        # generate class dynamically and register
        src_folder = os.path.join(DATA_PLUGIN_FOLDER, self.plugin_name)
        confdict = {"SRC_NAME":self.plugin_name,"GIT_REPO_URL":self.url,"SRC_ROOT_FOLDER":src_folder}
        # TODO: store confdict in hubconf collection
        k = type("AssistedGitDataPlugin_%s" % self.plugin_name,(GitDataPlugin,),confdict)
        self.__class__.data_plugin_manager.register_classes([k])


class AssistantManager(BaseSourceManager):

    ASSISTANT_CLASS = BaseAssistant

    def __init__(self, data_plugin_manager, dumper_manager, *args, **kwargs):
        super(AssistantManager,self).__init__(*args,**kwargs)
        self.data_plugin_manager = data_plugin_manager
        self.dumper_manager = dumper_manager

    def create_instance(self, klass, url):
        logging.debug("Creating new %s instance" % klass.__name__)
        return klass(url)

    def configure(self, klasses=[GithubAssistant,]):
        self.register_classes(klasses)

    def register_classes(self, klasses):
        for klass in klasses:
            klass.data_plugin_manager = self.data_plugin_manager
            klass.dumper_manager = self.dumper_manager
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

    def register_url(self, url, force=False, **kwargs):
        assistant = self.submit(url)
        if assistant:
            # register plugin info
            dp = get_data_plugin()
            dp.update({"_id":assistant.plugin_name},
                    {"$set":{"plugin":{"url":url,"type":assistant.plugin_type,"active":True}}},
                    upsert=True)
            assistant.handle()
            job = self.data_plugin_manager.load(assistant.plugin_name)
            assert len(job) == 1
            job = job.pop()
            def loaded(f):
                try:
                    res = f.result()
                    logging.debug("Plugin '%s' loaded, now loading manifest" % assistant.plugin_name)
                    assistant.load_manifest()
                except Exception as e:
                    logging.exception("Unable to load plugin '%s': %s" % (assistant.plugin_name,e))
            job.add_done_callback(loaded)
        else:
            raise AssistantException("Could not find any assistant able to handle URL '%s'" % url)

    def load(self):
        dp = get_data_plugin()
        cur = dp.find()
        for plugin in cur:
            ptype = plugin["plugin"]["type"]
            url = plugin["plugin"]["url"]
            if not plugin["plugin"]["active"]:
                logging.info("Data plugin '%s' is deactivated, skip" % url)
                continue
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

