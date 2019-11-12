import sys, re, os, time, glob, types
from datetime import datetime
from dateutil.parser import parse as dtparse
import pickle, json
from pprint import pformat
import asyncio
from functools import partial
import socket

from biothings.utils.hub_db import get_src_build, get_api
from biothings.utils.common import timesofar, get_random_string
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.es import ESIndexer

import biothings.web

from biothings import config as btconfig


class APIManagerException(Exception):
    pass

class APIManager(BaseManager):

    def __init__(self, log_folder=None, *args, **kwargs):
        self.api = get_api()
        self.register = {}
        self.timestamp = datetime.now()
        self.log_folder = log_folder or btconfig.LOG_FOLDER
        self.setup()
        self.restore_running_apis()

    def setup(self):
        self.setup_log()

    def setup_log(self):
        self.logger,_ = get_logger('apimanager')

    def restore_running_apis(self):
        """
        If some APIs were running but the hub stopped, re-start APIs
        as hub restarts
        """
        apis = self.get_apis()
        # these were running but had to stop when hub stopped
        running_apis = [api for api in apis if api.get("status") == "running"]
        for api in running_apis:
            self.logger.info("Restart API '%s'" % api["_id"])
            self.start_api(api["_id"])


    def register_status(self, api_id, status, **extra):
        apidoc = self.api.find_one({"_id":api_id})
        apidoc.update(extra)
        # clean according to status
        if status == "running":
            apidoc.pop("err",None)
        else:
            apidoc.pop("url",None)
        apidoc["status"] = status
        self.api.save(apidoc)

    def get_apis(self):
        return [d for d in self.api.find()]

    def start_api(self,api_id):
        apidoc = self.api.find_one({"_id":api_id})
        if not apidoc:
            raise ApiManagerException("No such API with ID '%s'" % api_id)
        if "entry_point" in apidoc:
            raise NotImplementedError("Custom entry point not implemented yet, " + \
                    "only basic generated APIs are currently supported")
        config_mod = types.ModuleType("config_mod")
        config_str = """
from biothings.web.settings.default import *

ES_HOST = "%(es_host)s"
ES_INDEX = "%(index)s"
ES_DOC_TYPE = "%(doc_type)s"

from biothings.web.api.es.handlers import BiothingHandler
from biothings.web.api.es.handlers import MetadataHandler
from biothings.web.api.es.handlers import QueryHandler
from biothings.web.api.es.handlers import StatusHandler

# doc_type involved there:
APP_LIST = [
    (r"/metadata/?", MetadataHandler),
    (r"/metadata/fields/?", MetadataHandler),
    (r"/%(doc_type)s/(.+)/?", BiothingHandler),
    (r"/%(doc_type)s/?$", BiothingHandler),
    (r"/query/?", QueryHandler),
]
""" % apidoc["config"]
        code = compile(config_str,"<string>","exec")
        # propagate config on module
        eval(code,{},config_mod.__dict__)
        app = biothings.web.BiothingsAPIApp(config_module=config_mod)
        port = int(apidoc["config"]["port"])
        try:
            server = app.get_server(config_mod)
            server.listen(port)
            self.register[api_id] = server
            self.logger.info("Running API '%s' on port %s" % (api_id,port))
            url = "http://%s:%s" % (socket.gethostname(),port)
            self.register_status(api_id,"running",url=url)
        except Exception as e:
            self.logger.exception("Failed to start API '%s'" % api_id)
            self.register_status(api_id,"failed",err=str(e))
            raise

    def stop_api(self,api_id):
        try:
            assert api_id in self.register, "API '%s' is not running" % api_id
            server = self.register.pop(api_id)
            server.stop()
            if server._stopped:
                self.register_status(api_id,"stopped")
        except Exception as e:
            self.logger.exception("Failed to stop API '%s'" % api_id)
            self.register_status(api_id,"failed",err=str(e))
            raise

    def delete_api(self,api_id):
        try:
            self.stop_api(api_id)
        except Exception as e:
            self.logger.warning("While trying to stop API '%s': %s" % (api_id,e))
        finally:
            self.api.remove({"_id":api_id})

    def create_api(self, api_id, es_host, index, doc_type, port, description=None, **kwargs):
        apidoc = {
                "_id" : api_id,
                "config" : {
                    "port" : port,
                    "es_host" : es_host,
                    "index" : index,
                    "doc_type" : doc_type,
                    },
                "description" : description,
                }
        apidoc.update(kwargs)
        self.api.save(apidoc)
        return apidoc


