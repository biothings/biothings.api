import time, copy
import os, pprint
from datetime import datetime
import asyncio
from functools import partial
import inspect
import subprocess

from biothings.utils.hub_db import get_src_dump, get_src_build, get_source_fullname
from biothings.utils.common import timesofar
from biothings.utils.dataload import dict_walk
from biothings.utils.loggers import HipchatHandler
from biothings.hub import INSPECTOR_CATEGORY
from biothings.hub.databuild.backend import create_backend
from biothings.hub.dataload.uploader import ParallelizedSourceUploader
import biothings.utils.inspect as btinspect
from config import logger as logging, HIPCHAT_CONFIG, LOG_FOLDER

from biothings.utils.manager import BaseManager


class InspectorError(Exception):
    pass

class InspectorManager(BaseManager):

    def __init__(self, upload_manager, build_manager, *args, **kwargs):
        super(InspectorManager,self).__init__(*args, **kwargs)
        self.upload_manager = upload_manager
        self.build_manager = build_manager

    def inspect(self, data_provider, mode="type", **kwargs):
        """
        Inspect given data provider:
        - backend definition, see bt.hub.dababuild.create_backend for
          supported format), eg "merged_collection" or ("src","clinvar")
        - ("uploader",src_name), will use the src_name's uploader output
          not the actual collection data
        - or callable yielding documents
        Mode:
        - "type": will inspect and report type map found in data
        - "mapping": will inspect and return a map compatible for later
          ElasticSearch mapping generation (see bt.utils.es.generate_es_mapping)
        - "stats": will inspect and report types + different counts found in
          data, giving a detailed overview of the volumetry of each fields and sub-fields
        """
        # /!\ attention: this piece of code is critical and not easy to understand...
        # Depending on the source of data to inspect, this method will create an
        # uploader or a builder. These objects don't be behave the same with they
        # pass through pickle: uploader needs to be "unprepare()"ed so it can be
        # pickled (remove some db connection, socket), while builder must *not* be
        # unprepare() because it would reset the underlying target_name (the actual
        # target collection). Also, the way results and statuses are registered is
        # different for uploader and builder...
        # So, there are lots of "if", be careful if you want to modify that code.

        data_provider_type = None # where to register results (if possible to do so)
        registerer_obj = None # who should register result
        if callable(data_provider):
            raise NotImplementedError("data_provider as callable untested...")
            yielder_provider = data_provider
        else:
            if data_provider[0] == "uploader" or data_provider[0] == "src":
                data_provider_type = "source"
                # find src_dump doc
                src_name = data_provider[1].split(".")[0]
                doc = get_src_dump().find_one({"_id":src_name})
                if not doc:
                    raise InspectorError("Can't find document associated to '%s'" % src_name)
                if not doc.get("data_folder"):
                    raise InspectorError("Can't find data folder for '%s'" % src_name)

                # get an uploader instance (used to get the data if type is "uploader"
                # but also used to update status of the datasource via register_status()
                ups = self.upload_manager[data_provider[1]]
                # TODO: if dealing with a jobs list later (job_manager), we can handle this easily
                assert len(ups) == 1, "More than one uploader found for '%s', not supported (yet)" % data_provider[1]
                # create uploader
                registerer_obj = self.upload_manager.create_instance(ups[0])
                if isinstance(registerer_obj,ParallelizedSourceUploader):
                    raise InspectorError("ParallelizedSourceUploader based uploaders aren't supported (yet)")

                # just wrap functions returnning data so they're called in new process
                # when submitted by job_manager
                def provider_uploader(uploader,data_folder):
                    return uploader.load_data(data_folder)
                def provider_source():
                    col = create_backend(data_provider).target_collection
                    return col.find() # cursor

                # data providers are different
                if data_provider[0] == "uploader":
                    data_folder = doc["data_folder"]
                    # in this case, registerer_obj is also the object used to get the data
                    yielder_provider = partial(provider_uploader,registerer_obj,data_folder)
                else:
                    yielder_provider = partial(provider_source)
            else:
                try:
                    data_provider_type = "build"
                    def provider_build():
                        col = create_backend(data_provider).target_collection
                        return col.find() # cursor
                    registerer_obj = self.build_manager.get_builder(data_provider)
                    yielder_provider = provider_build
                except Exception as e:
                    raise InspectorError("Unable to create backend from '%s': %s" % (repr(data_provider),e))

        # commong function used to call inspector
        def inspect_data(yielder_provider,mode):
            yielder = yielder_provider()
            if callable(yielder):
                data = yielder()
            else:
                data = yielder
            return btinspect.inspect_docs(data,mode=mode)

        inspected = None
        got_error = None
        try:
            @asyncio.coroutine
            def do():
                yield from asyncio.sleep(0.0)
                pinfo = {"category" : INSPECTOR_CATEGORY,
                        "source" : "%s" % repr(data_provider),
                        "step" : "",
                        "description" : ""}
                # register begin of inspection (differ slightly depending on type)
                if data_provider_type == "source":
                    registerer_obj.register_status("inspecting")
                elif data_provider_type == "build":
                    registerer_obj.register_status("inspecting",transient=True,init=True,job={"step":"inspect"})

                logging.info("Running inspector on %s (type:%s,data_provider:%s)" % \
                        (repr(data_provider),data_provider_type,yielder_provider))
                # make it pickleable
                if data_provider_type == "source":
                    # because register_obj is also used to fetch data, it has to be unprepare() for pickling
                    registerer_obj.unprepare()
                else:
                    # NOTE: do not unprepare() the builder, we'll loose the target name
                    # (it's be randomly generated again) and we won't be able to register results
                    pass
                job = yield from self.job_manager.defer_to_process(pinfo,
                        partial(inspect_data,yielder_provider,mode=mode))
                def inspected(f):
                    nonlocal inspected
                    nonlocal got_error
                    try:
                        # keys can be types, we need to convert keys to strings
                        res = f.result()
                        _map = dict_walk(res,lambda k: str(k))
                        _map["data_provider"] = repr(data_provider)
                        # register begin of inspection (differ slightly depending on type)
                        if data_provider_type == "source":
                            registerer_obj.register_status("success",inspect=_map)
                        elif data_provider_type == "build":
                            registerer_obj.register_status("success",job={"step":"inspect"},
                                                                        build={"inspect":_map})
                    except Exception as e:
                        logging.exception("Error while inspecting data: %s" % e)
                        got_error = e
                job.add_done_callback(inspected)
                yield from job
                if got_error:
                    raise got_error
                if data_provider_type is None:
                    return inspected
            task = asyncio.ensure_future(do())
            return task
        except Exception as e:
            logging.error("Error while inspecting '%s': %s" % (repr(data_provider),e))
            raise

