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
from biothings.utils.loggers import HipchatHandler, get_logger
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

    def setup(self):
        self.setup_log()

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, 'apimanager_%s.log' % time.strftime("%Y%m%d",self.timestamp.timetuple()))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("apimanager")
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not nh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(nh)
        return self.logger

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

HIPCHAT_ROOM = {}

ES_HOST = "%(es_host)s"
ES_INDEX = "%(index)s"
ES_DOC_TYPE = "%(doc_type)s"

from biothings.web.api.es.handlers import BiothingHandler
from biothings.web.api.es.handlers import MetadataHandler
from biothings.web.api.es.handlers import QueryHandler
from biothings.web.api.es.handlers import StatusHandler

# doc_type involved there:
APP_LIST = [
    (r"/status", StatusHandler),
    (r"/metadata/?", MetadataHandler),
    (r"/metadata/fields/?", MetadataHandler),
    (r"/variant/(.+)/?", BiothingHandler),
    (r"/variant/?$", BiothingHandler),
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


