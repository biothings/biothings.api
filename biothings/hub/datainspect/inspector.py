import time, copy
import os, pprint
from datetime import datetime
import asyncio
from functools import partial
import inspect
import subprocess
import random
import math

from biothings.utils.hub_db import get_src_dump, get_src_build, get_source_fullname
from biothings.utils.common import timesofar
from biothings.utils.dataload import dict_walk, dict_traverse
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
    import logging
    col = create_backend(backend_provider).target_collection
    cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}}) 
    res = btinspect.inspect_docs(cur,mode=mode,pre_mapping=pre_mapping,
                                  metadata=False,auto_convert=False,**kwargs)
    return res


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

    def inspect(self, data_provider, mode="type", batch_size=10000,
                limit=None, sample=None, **kwargs):
        """
        Inspect given data provider:
        - backend definition, see bt.hub.dababuild.create_backend for
          supported format), eg "merged_collection" or ("src","clinvar")
        - or callable yielding documents
        Mode:
        - "type": will inspect and report type map found in data (internal/non-standard format)
        - "mapping": will inspect and return a map compatible for later
          ElasticSearch mapping generation (see bt.utils.es.generate_es_mapping)
        - "stats": will inspect and report types + different counts found in
          data, giving a detailed overview of the volumetry of each fields and sub-fields
        - "jsonschema", same as "type" but result is formatted as json-schema standard
        - limit: when set to an integer, will inspect only x documents.
        - sample: combined with limit, for each document, if random.random() <= sample (float), 
          the document is inspected. This option allows to inspect only a sample of data.
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
                # is it a full source name (dot notation) ?
                fullname = get_source_fullname(data_provider[1])
                if fullname:
                    # it's a dot-notation
                    src_name = fullname.split(".")[0]
                else:
                    # no subsource, full source name is the passed name
                    src_name = data_provider[1]
                    fullname = src_name
                doc = get_src_dump().find_one({"_id":src_name}) # query by main source
                if not doc:
                    raise InspectorError("Can't find document associated to '%s'" % src_name)
                # get an uploader instance (used to get the data if type is "uploader"
                # but also used to update status of the datasource via register_status()
                ups = self.upload_manager[fullname] # potentially using dot notation
                if len(ups) > 1:
                    # recursively call inspect(), collect and return corresponding tasks
                    self.logger.debug("Multiple uploaders found, running inspector for each of them: %s" % ups)
                    res = []
                    for up in ups:
                        r = self.inspect((data_provider[0],"%s" % up.name),mode=mode, batch_size=batch_size,
                                limit=limit, sample=sample, **kwargs)
                        res.append(r)
                    return res

                assert len(ups) == 1, "More than one uploader found for '%s', not supported (yet), use main_source.source notation" % data_provider[1]
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
                if not sample is None:
                    self.logger.info("Sample set to %s, inspect only a subset of data",sample)
                if limit is None:
                    self.logger.info("Inspecting all the documents")
                else:
                    nonlocal batch_size
                    # adjust batch_size so we inspect only "limit" docs if batch is smaller than the limit
                    if batch_size > limit:
                        batch_size = limit
                    self.logger.info("Inspecting only %s documents",limit)
                # make it pickleable
                if data_provider_type == "source":
                    # because register_obj is also used to fetch data, it has to be unprepare() for pickling
                    registerer_obj.unprepare()
                else:
                    # NOTE: do not unprepare() the builder, we'll loose the target name
                    # (it's be randomly generated again) and we won't be able to register results
                    pass

                cnt = 0
                doccnt = 0
                jobs = []
                # normalize mode param and prepare global results
                if type(mode) == str:
                    mode = [mode]

                converters,mode = btinspect.get_converters(mode)

                inspected = {}
                for m in mode:
                    inspected.setdefault(m,{})

                backend = create_backend(backend_provider).target_collection
                for ids in id_feeder(backend,batch_size=batch_size):
                    if not sample is None:
                        if random.random() > sample:
                            continue
                    cnt += 1
                    doccnt += batch_size
                    if limit and doccnt > limit:
                        break
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

                # just potential converters
                btinspect.run_converters(inspected,converters)

                def fully_inspected(res):
                    nonlocal got_error
                    try:
                        res = btinspect.stringify_inspect_doc(res)
                        _map = {"results" : res}
                        _map["data_provider"] = repr(data_provider)
                        _map["started_at"] = started_at
                        _map["duration"] = timesofar(t0)
                        # when inspecting with "stats" mode, we can get huge number but mongo
                        # can't store more than 2^64, make sure to get rid of big nums there
                        def clean_big_nums(k,v):
                            # TODO: same with float/double? seems mongo handles more there ?
                            if isinstance(v,int) and v > 2**64:
                                return k,math.nan
                            else:
                                return k,v
                        dict_traverse(_map,clean_big_nums)
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

