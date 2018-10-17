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
from biothings.utils.mongo import id_feeder, doc_feeder
from biothings.utils.loggers import get_logger
from biothings.hub import INSPECTOR_CATEGORY
from biothings.hub.databuild.backend import create_backend
import biothings.utils.inspect as btinspect
import biothings.utils.es as es

from biothings.utils.manager import BaseManager


class InspectorError(Exception):
    pass


# commong function used to call inspector
def inspect_data(backend_provider,ids,mode,pre_mapping,**kwargs):
    col = create_backend(backend_provider).target_collection
    cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}}) 
    return btinspect.inspect_docs(cur,mode=mode,pre_mapping=pre_mapping,metadata=False,**kwargs)


class InspectorManager(BaseManager):

    def __init__(self, upload_manager, build_manager, *args, **kwargs):
        super(InspectorManager,self).__init__(*args, **kwargs)
        self.upload_manager = upload_manager
        self.build_manager = build_manager
        self.logfile = None
        self.setup_log()

    def setup_log(self):
        """Setup and return a logger instance"""
        self.logger, self.logfile = get_logger('inspect')

    def inspect(self, data_provider, mode="type", batch_size=10000,**kwargs):
        """
        Inspect given data provider:
        - backend definition, see bt.hub.dababuild.create_backend for
          supported format), eg "merged_collection" or ("src","clinvar")
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
        # uploader or a builder. These objects don't be behave the same while they
        # pass through pickle: uploader needs to be "unprepare()"ed so it can be
        # pickled (remove some db connection, socket), while builder must *not* be
        # unprepare() because it would reset the underlying target_name (the actual
        # target collection). Also, the way results and statuses are registered is
        # different for uploader and builder...
        # So, there are lots of "if", be careful if you want to modify that code.

        data_provider_type = None # where to register results (if possible to do so)
        registerer_obj = None # who should register result
        t0 = time.time()
        started_at = datetime.now()
        self.logger.info("Inspecting data with mode %s and data_provider %s" % (repr(mode),repr(data_provider)))
        if callable(data_provider):
            raise NotImplementedError("data_provider as callable untested...")
        else:
            if data_provider[0] == "src":
                data_provider_type = "source"
                # find src_dump doc
                src_name = data_provider[1].split(".")[0]
                doc = get_src_dump().find_one({"_id":src_name})
                if not doc:
                    raise InspectorError("Can't find document associated to '%s'" % src_name)

                # get an uploader instance (used to get the data if type is "uploader"
                # but also used to update status of the datasource via register_status()
                ups = self.upload_manager[data_provider[1]]
                # TODO: if dealing with a jobs list later (job_manager), we can handle this easily
                assert len(ups) == 1, "More than one uploader found for '%s', not supported (yet)" % data_provider[1]
                # create uploader
                registerer_obj = self.upload_manager.create_instance(ups[0])
                backend_provider = data_provider
            else:
                try:
                    data_provider_type = "build"
                    registerer_obj = self.build_manager.get_builder(data_provider)
                    backend_provider = data_provider
                except Exception as e:
                    raise InspectorError("Unable to create backend from '%s': %s" % (repr(data_provider),e))

        got_error = None
        try:
            @asyncio.coroutine
            def do():
                yield from asyncio.sleep(0.0)
                nonlocal mode

                pinfo = {"category" : INSPECTOR_CATEGORY,
                        "source" : "%s" % repr(data_provider),
                        "step" : "",
                        "description" : ""}
                # register begin of inspection (differ slightly depending on type)
                if data_provider_type == "source":
                    registerer_obj.register_status("inspecting",subkey="inspect")
                elif data_provider_type == "build":
                    registerer_obj.register_status("inspecting",transient=True,init=True,job={"step":"inspect"})

                self.logger.info("Running inspector on %s (type:%s,data_provider:%s)" % \
                        (repr(data_provider),data_provider_type,backend_provider))
                # make it pickleable
                if data_provider_type == "source":
                    # because register_obj is also used to fetch data, it has to be unprepare() for pickling
                    registerer_obj.unprepare()
                else:
                    # NOTE: do not unprepare() the builder, we'll loose the target name
                    # (it's be randomly generated again) and we won't be able to register results
                    pass

                cnt = 0
                jobs = []
                # normalize mode param and prepare global results
                if type(mode) == str:
                    mode = [mode]
                inspected = {}
                for m in mode:
                    inspected.setdefault(m,{})

                backend = create_backend(backend_provider).target_collection
                for ids in id_feeder(backend,batch_size=batch_size):
                    cnt += 1
                    pinfo["description"] = "batch #%s" % cnt
                    def batch_inspected(bnum,i,f):
                        nonlocal inspected
                        nonlocal got_error
                        nonlocal mode
                        try:
                            res = f.result()
                            for m in mode:
                                inspected[m] = btinspect.merge_record(inspected[m],res[m],m)
                        except Exception as e:
                            got_error = e
                            self.logger.error("Error while inspecting data from batch #%s: %s" % (bnum,e))
                            raise
                    pre_mapping ="mapping" in mode  # we want to generate intermediate mapping so we can merge
                                                    # all maps later and then generate the ES mapping from there
                    self.logger.info("Creating inspect worker for batch #%s" % cnt)
                    job = yield from self.job_manager.defer_to_process(pinfo,
                            partial(inspect_data,backend_provider,ids,mode=mode,pre_mapping=pre_mapping,**kwargs))
                    job.add_done_callback(partial(batch_inspected,cnt,ids))
                    jobs.append(job)

                yield from asyncio.gather(*jobs)

                # compute metadata (they were skipped before)
                for m in mode:
                    if m == "mapping":
                        try:
                            inspected["mapping"] = es.generate_es_mapping(inspected["mapping"])
                            # metadata for mapping only once generated
                            inspected = btinspect.compute_metadata(inspected,m)
                        except es.MappingError as e:
                            inspected["mapping"] = {"pre-mapping" : inspected["mapping"], "errors" : e.args[1]}
                    else:
                        inspected = btinspect.compute_metadata(inspected,m)

                def fully_inspected(res):
                    nonlocal got_error
                    try:
                        def bsoncompat(val):
                            if type(val) == type:
                                return val.__name__ # prevent having dots in the field (not storable in mongo)
                            else:
                                return str(val)
                        _map = {"results" : dict_walk(res,bsoncompat)}
                        _map["data_provider"] = repr(data_provider)
                        _map["started_at"] = started_at
                        _map["duration"] = timesofar(t0)
                        # register begin of inspection (differ slightly depending on type)
                        if "mapping" in mode and "errors" in res["mapping"] and "pre-mapping" in res["mapping"]:
                            registerer_obj.register_status("failed",subkey="inspect",inspect=_map)
                            got_error = InspectorError(res["mapping"]["errors"])
                        else:
                            if data_provider_type == "source":
                                registerer_obj.register_status("success",subkey="inspect",inspect=_map)
                            elif data_provider_type == "build":
                                registerer_obj.register_status("success",job={"step":"inspect"},
                                                                        build={"inspect":_map})
                    except Exception as e:
                        self.logger.exception("Error while inspecting data: %s" % e)
                        got_error = e
                        if data_provider_type == "source":
                            registerer_obj.register_status("failed",subkey="inspect",err=repr(e))
                        elif data_provider_type == "build":
                            registerer_obj.register_status("failed",job={"err":repr(e)})
                fully_inspected(inspected)
                if data_provider_type is None:
                    return
                if got_error:
                    raise got_error
            task = asyncio.ensure_future(do())
            return task
        except Exception as e:
            self.logger.error("Error while inspecting '%s': %s" % (repr(data_provider),e))
            raise

