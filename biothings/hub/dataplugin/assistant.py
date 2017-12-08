import time, copy
import os, pprint
import urllib.parse, requests
from datetime import datetime
import asyncio
from functools import partial
import inspect

from biothings.utils.hub_db import get_src_dump
from biothings.utils.common import timesofar, rmdashfr
from biothings.utils.loggers import HipchatHandler
from biothings.hub import DUMPER_CATEGORY, UPLOADER_CATEGORY
from config import logger as logging, HIPCHAT_CONFIG, LOG_FOLDER, \
                             CODE_ARCHIVE_ROOT

from biothings.utils.manager import BaseSourceManager
from biothings.hub.dataload.uploader import set_pending_to_upload
from .dumper import GitDumper

class AssistantException(Exception):
    pass


class BaseAssistant(object):

    name = None # to be defined in subblass
    code_manager = None # set by assistant manager

    def handle(self, url):
        """Access URL as do whatever is necessary to bring code to life within the hub...
        (hint: that may involve creating a dumper on-the-fly and register that dumper to
        a manager...)
        """
        raise NotImplementedError("implement in subclass")

    def can_handle(self,url):
        """Return true if assistant can handle the code behind given URLs"""
        raise NotImplementedError("implement in subclass")


class GithubAssistant(BaseAssistant):

    name = "github"

    def can_handle(self, url):
        # analyze headers to guess type of required assitant
        headers = requests.head(url).headers
        if headers.get("server").lower() == "github.com":
            return True

    def handle(self, url):
        assert self.__class__.code_manager, "Please set code_manager attribute"
        split = urllib.parse.urlsplit(url)
        project_name = os.path.basename(split.path).replace(".git","")

        # generate class dynamically and register
        src_folder = os.path.join(CODE_ARCHIVE_ROOT, project_name)
        k = type("AssistedGitDumper_%s" % project_name,(GitDumper,),
                {"SRC_NAME":project_name,"GIT_REPO_URL":url,"SRC_ROOT_FOLDER":src_folder})

        self.__class__.code_manager.register_classes([k])


class AssistantManager(BaseSourceManager):

    ASSISTANT_CLASS = BaseAssistant

    def __init__(self, code_manager, *args, **kwargs):
        super(AssistantManager,self).__init__(*args,**kwargs)
        self.code_manager = code_manager

    def create_instance(self,klass):
        logging.debug("Creating new %s instance" % klass.__name__)
        return klass()

    def configure(self, klasses=[GithubAssistant,]):
        self.register_classes(klasses)

    def register_classes(self, klasses):
        for klass in klasses:
            klass.code_manager = self.code_manager
            self.register[klass.name] = klass

    def submit(self,url):
        # submit url to all registered assistants (in order)
        # and return the first claiming it can handle that URLs
        for typ in self.register:
            aklass = self.register[typ]
            inst = self.create_instance(aklass)
            if inst.can_handle(url):
                return inst
        return None

    def import_url(self, url, force=False, **kwargs):
        assistant = self.submit(url)
        if assistant:
            assistant.handle(url)
        else:
            raise AssistantException("Could not find any assistant able to handle URL '%s'" % url)

