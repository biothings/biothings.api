import asyncio
import logging
import tornado.web

from .base import BaseHandler
from biothings.utils.dataload import to_boolean

from biothings.hub.dataload.dumper import DumperManager
from biothings.hub.dataload.uploader import UploaderManager
from biothings.hub.databuild.builder import BuilderManager
from biothings.hub.databuild.differ import DifferManager
from biothings.hub.databuild.syncer import SyncerManager
from biothings.hub.dataindex.indexer import IndexerManager
from biothings.utils.manager import JobManager

def dump_info(dump_manager):

    res = {}
    for name,klasses in dump_manager.register.items():
        res[name] = [klass.__name__ for klass in klasses]
    return res

def upload_info(upload_manager):
    res = {}
    for name,klasses in upload_manager.register.items():
        res[name] = [klass.__name__ for klass in klasses]
    return res

def build_info(builder_manager):
    res = {}
    for name in builder_manager.register:
        builder = builder_manager[name]
        res[name] = {
                "class" : builder.__class__.__name__,
                "build_config" : builder.build_config,
                "source_backend" : {
                    "type" : builder.source_backend.__class__.__name__,
                    "source_db" : builder.source_backend.sources.client.address,
                },
                "target_backend" : {
                    "type" : builder.source_backend.__class__.__name__,
                    "target_db" : builder.target_backend.target_db.client.address
                }
                }
        res[name]["mapper"] = {}
        for mappername,mapper in builder.mappers.items():
            res[name]["mapper"][mappername] = mapper.__class__.__name__
    return res

def diff_info(differ_manager):
    return {"tdb":True}


def job_info(job_manager):
    return {
            "queue" : {
                "process" : {
                    "max_workers" : job_manager.process_queue._max_workers,
                    "workers" : len(job_manager.process_queue._processes),
                    },
                "thread" : {
                    "max_workers" : job_manager.thread_queue._max_workers,
                    "workers" : len(job_manager.thread_queue._threads),
                    },
                },
            "memory" : job_manager.hub_memory,
            "available_system_memory" : job_manager.avail_memory,
            "max_memory_usage" : job_manager.max_memory_usage,
            "hub_pid" : job_manager.hub_process.pid
            }



class ManagerHandler(BaseHandler):

    info_map = {
            JobManager : job_info,
            DumperManager : dump_info,
            UploaderManager : upload_info,
            BuilderManager : build_info,
            DifferManager : diff_info,
            }

    @asyncio.coroutine
    def get(self,name=None):
        res = {}
        found = False
        for managername,manager in self.managers.items():
            if name and managername != name:
                continue
            found = True
            info_func = self.__class__.info_map.get(manager.__class__)
            if info_func:
                res[managername] = info_func(manager)
        if not found:
            raise tornado.web.HTTPError(404,reason="No such manager named '%s'" % name)

        self.write(res)


class JobManagerHandler(BaseHandler):

    @asyncio.coroutine
    def get(self,name,queue_name=None):
        res = {}
        manager = None
        for managername,manager in self.managers.items():
            if name and managername != name:
                continue
            break
        if not manager:
            raise tornado.web.HTTPError(404,reason="No such JobManager named '%s'" % name)
        if queue_name is None:
            self.write(self.job_info(manager))
        elif queue_name == "process":
            self.write(manager.get_process_summary())
        else:
            raise tornado.web.HTTPError(404,reason="No such queue named '%s' (process|thread)" % queue_name)
