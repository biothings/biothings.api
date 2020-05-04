import sys, re, os, time, math, glob, copy
from datetime import datetime
from dateutil.parser import parse as dtparse
import pickle, json
from pprint import pformat
import asyncio
from functools import partial
from elasticsearch import Elasticsearch

import biothings.utils.mongo as mongo
from biothings.utils.hub_db import get_src_build
import biothings.utils.aws as aws
from biothings.utils.common import timesofar, get_random_string, iter_n, \
                                   get_class_from_classpath, get_dotfield_value
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.es import ESIndexer, IndexerException as ESIndexerException
from biothings.utils.backend import DocESBackend
from biothings import config as btconfig
from biothings.utils.mongo import doc_feeder, id_feeder
from config import LOG_FOLDER, logger as logging
from biothings.utils.hub import publish_data_version
from biothings.hub.databuild.backend import generate_folder, create_backend, \
                                            merge_src_build_metadata
from biothings.hub import INDEXER_CATEGORY, INDEXMANAGER_CATEGORY


def new_index_worker(col_name,ids,pindexer,batch_num):
        col = create_backend(col_name).target_collection
        idxer = pindexer()
        cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}})
        cnt = idxer.index_bulk(cur)
        return cnt


def merge_index_worker(col_name,ids,pindexer,batch_num):
        col = create_backend(col_name).target_collection
        idxer = pindexer()
        upd_cnt = 0
        new_cnt = 0
        cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}})
        docs = [d for d in cur]
        [d.pop("_timestamp",None) for d in docs]
        dids = dict([(d["_id"],d) for d in docs])
        dexistings = dict([(d["_id"],d) for d in idxer.get_docs([k for k in dids.keys()])])
        for _id in dexistings:
            d = dexistings[_id]
            # update in-place
            d.update(dids[_id])
            # mark as processed/updated
            dids.pop(_id)
        # updated docs (those existing in col *and* index)
        upd_cnt = idxer.index_bulk(dexistings.values(),len(dexistings))
        logging.debug("%s documents updated in index" % repr(upd_cnt))
        # new docs (only in col, *not* in index)
        new_cnt = idxer.index_bulk(dids.values(),len(dids))
        logging.debug("%s new documents in index" % repr(new_cnt))
        # need to return one: tuple(cnt,list)
        ret = (upd_cnt[0] + new_cnt[0], upd_cnt[1] + new_cnt[1])
        return ret


def indexer_worker(col_name,ids,pindexer,batch_num,mode="index",
                   worker=new_index_worker):
    try:
        if mode in ["index","merge"]:
            return worker(col_name,ids,pindexer,batch_num)
        elif mode == "resume":
            idxr = pindexer()
            es_ids = idxr.mexists(ids)
            missing_ids = [e[0] for e in es_ids if e[1] == False]
            if missing_ids:
                return worker(col_name,missing_ids,pindexer,batch_num)
            else:
                # fake indexer results, it has to be a tuple, first elem is num of indexed docs
                return (0,None)
    except Exception as e:
        logger_name = "index_%s_%s_batch_%s" % (pindexer.keywords.get("index","index"),col_name,batch_num)
        logger,_ = get_logger(logger_name, btconfig.LOG_FOLDER)
        logger.exception(e)
        exc_fn = os.path.join(btconfig.LOG_FOLDER,"%s.pick" % logger_name)
        pickle.dump({"exc":e,"ids":ids},open(exc_fn,"wb"))
        logger.info("Exception and IDs were dumped in pickle file '%s'" % exc_fn)
        raise


class IndexerException(Exception):
    pass


class Indexer(object):
    """
    Basic indexer, reading documents from a mongo collection (target_name)
    and sending documents to ES.
    """

    def __init__(self, es_host, target_name=None, **kwargs):
        self.host = es_host
        self.env = None
        self.log_folder = LOG_FOLDER
        self.timestamp = datetime.now()
        self.conf_name = None
        self.build_doc = None
        self.target_name = None
        self.index_name = None
        self.doc_type = None
        self.num_shards = None
        self.num_replicas = None
        self.kwargs = kwargs
        self.ti = time.time()

    def get_predicates(self):
        return []

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        return {"category" : INDEXER_CATEGORY,
                "source" : "%s:%s" % (self.conf_name,self.index_name),
                "step" : "",
                "description" : ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    @asyncio.coroutine
    def index(self, target_name, index_name, job_manager, steps=["index","post"],
              batch_size=10000, ids=None, mode="index", worker=None):
        """
        Build an index named "index_name" with data from collection "target_collection".

        "ids" can be passed to selectively index documents.

        "mode" can have the following values:
            - 'purge': will delete index if it exists
            - 'resume': will use existing index and add documents. "ids" can be passed as a list of missing IDs,
                    or, if not pass, ES will be queried to identify which IDs are missing for each batch in
                    order to complete the index.
            - 'merge': will merge data with existing index' documents, used when populated several distinct times (cold/hot merge for instance)
            - None (default): will create a new index, assuming it doesn't already exist
        """
        assert job_manager
        # check what to do
        if type(steps) == str:
            steps = [steps]
        self.target_name = target_name
        self.index_name = index_name
        self.load_build()
        self.setup_log()
        # select proper index worker according to mode:
        if worker is None: # none specified, choose correct one
            if mode == "merge":
                worker = merge_index_worker
            else:
                worker = new_index_worker

        got_error = False
        cnt = 0

        if "index" in steps:
            self.register_status("indexing",transient=True,init=True,job={"step":"index"})
            assert self.build_doc.get("backend_url")
            target_collection = create_backend(self.build_doc["backend_url"]).target_collection
            backend_url = self.build_doc["backend_url"]
            _mapping = self.get_mapping()
            _extra = self.get_index_creation_settings()
            _meta = {}
            # partially instantiated indexer instance for process workers
            partial_idxer = partial(ESIndexer,doc_type=self.doc_type,
                                 index=index_name,
                                 es_host=self.host,
                                 step=batch_size,
                                 number_of_shards=self.num_shards,
                                 number_of_replicas=self.num_replicas,
                                 **self.kwargs)
            # instantiate one here for index creation
            es_idxer = partial_idxer()
            if es_idxer.exists_index():
                if mode == "purge":
                    es_idxer.delete_index()
                elif not mode in ["resume","merge"]:
                    msg = "Index already '%s' exists, (use mode='purge' to auto-delete it or mode='resume' to add more documents)" % index_name
                    self.register_status("failed",job={"err": msg})
                    raise IndexerException(msg)

            if not mode in ["resume","merge"]:
                try:
                    es_idxer.create_index({self.doc_type:_mapping},_extra)
                except Exception as e:
                    self.logger.exception("Failed to create index")
                    self.register_status("failed",job={"err": repr(e)})
                    raise

            def clean_ids(ids):
                # can't use a generator, it's going to be pickled
                cleaned = []
                for _id in ids:
                    if type(_id) != str:
                        self.logger.warning("_id '%s' has invalid type (!str), skipped" % repr(_id))
                        continue
                    if len(_id) > 512: # this is an ES6 limitation
                        self.logger.warning("_id is too long: '%s'" % _id)
                        continue
                    cleaned.append(_id)
                return cleaned

            jobs = []
            total = target_collection.count()
            btotal = math.ceil(total/batch_size)
            bnum = 1
            if ids:
                self.logger.info("Indexing from '%s' with specific list of _ids, create indexer job with batch_size=%d" % (target_name, batch_size))
                id_provider = iter_n(ids,batch_size)
            else:
                self.logger.info("Fetch _ids from '%s', and create indexer job with batch_size=%d" % (target_name, batch_size))
                id_provider = id_feeder(target_collection, batch_size=batch_size,logger=self.logger)
            for ids in id_provider:
                yield from asyncio.sleep(0.0)
                origcnt = len(ids)
                ids = clean_ids(ids)
                newcnt = len(ids)
                if origcnt != newcnt:
                    self.logger.warning("%d document(s) can't be indexed and " % (origcnt-newcnt) + \
                                        "will be skipped (invalid _id)")
                # progress count
                cnt += len(ids)
                pinfo = self.get_pinfo()
                pinfo["step"] = self.target_name
                try:
                    descprogress = cnt/total*100
                except ZeroDivisionError:
                    descprogress = 0.0
                pinfo["description"] = "#%d/%d (%.1f%%)" % (bnum,btotal,descprogress)
                self.logger.info("Creating indexer job #%d/%d, to index '%s' %d/%d (%.1f%%)" % \
                        (bnum,btotal,backend_url,cnt,total,descprogress))
                job = yield from job_manager.defer_to_process(
                        pinfo,
                        partial(indexer_worker,
                            backend_url,
                            ids,
                            partial_idxer,
                            bnum,
                            mode,
                            worker))
                def batch_indexed(f,batch_num):
                    nonlocal got_error
                    try:
                        res = f.result()
                        if type(res) != tuple or type(res[0]) != int:
                            got_error = Exception("Batch #%s failed while indexing collection '%s' [result:%s]" % \
                                    (batch_num,self.target_name,repr(res)))
                    except Exception as e:
                        got_error = e
                        self.logger.exception("Batch indexed error %s" % e)
                        return
                job.add_done_callback(partial(batch_indexed,batch_num=bnum))
                jobs.append(job)
                bnum += 1
                # raise error as soon as we know
                if got_error:
                    self.register_status("failed",job={"err": repr(got_error)})
                    raise got_error
            self.logger.info("%d jobs created for indexing step" % len(jobs))
            tasks = asyncio.gather(*jobs)
            def done(f):
                nonlocal got_error
                if None in f.result():
                    got_error = None#Exception("Some batches failed")
                    return
                # compute overall inserted/updated records
                # returned values looks like [(num,[]),(num,[]),...]
                cnt = sum([val[0] for val in f.result()])
                self.register_status("success",job={"step":"index"},index={"count":cnt})
                if total != cnt:
                    # raise error if counts don't match, but index is still created,
                    # fully registered in case we want to use it anyways
                    err = "Merged collection has %d documents but %d have been indexed (check logs for more)" % (total,cnt)
                    raise IndexerException(err)
                self.logger.info("Index '%s' successfully created using merged collection %s" % (index_name,target_name),extra={"notify":True})
            tasks.add_done_callback(done)
            yield from tasks

        if "post" in steps:
            self.logger.info("Running post-index process for index '%s'" % index_name)
            self.register_status("indexing",transient=True,init=True,job={"step":"post-index"})
            pinfo = self.get_pinfo()
            pinfo["step"] = "post_index"
            # for some reason (like maintaining object's state between pickling).
            # we can't use process there. Need to use thread to maintain that state without
            # building an unmaintainable monster
            job = yield from job_manager.defer_to_thread(pinfo, partial(self.post_index, target_name, index_name,
                    job_manager, steps=steps, batch_size=batch_size, ids=ids, mode=mode))
            def posted(f):
                nonlocal got_error
                try:
                    res = f.result()
                    self.logger.info("Post-index process done for index '%s': %s" % (index_name,res))
                    self.register_status("indexing",job={"step":"post-index"})
                except Exception as e:
                    got_error = e
                    self.logger.error("Post-index process failed for index '%s': %s" % (index_name,e),extra={"notify":True})
                    return
            job.add_done_callback(posted)
            yield from job # consume future

        if got_error:
            self.register_status("failed",job={"err": repr(got_error)})
            raise got_error
        else:
            self.register_status("success")
            return {"%s" % self.index_name : cnt}

    def register_status(self,status,transient=False,init=False,**extra):
        assert self.build_doc
        src_build = get_src_build()
        job_info = {
                'status': status,
                'step_started_at': datetime.now(),
                'logfile': self.logfile,
                }
        index_info = {
                "index": {
                    self.index_name : {
                        'host' : self.host,
                        'environment' : self.env,
                        'conf_name' : self.conf_name,
                        'target_name' : self.target_name,
                        'index_name' : self.index_name,
                        'doc_type' : self.doc_type,
                        'num_shards' : self.num_shards,
                        'num_replicas' : self.num_replicas
                        }
                    }
                }
        if transient:
            # record some "in-progress" information
            job_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            job_info["time"] = timesofar(self.ti)
            t1 = round(time.time() - self.ti, 0)
            job_info["time_in_s"] = t1
            index_info["index"][self.index_name]["created_at"] = datetime.now()
        if "index" in extra:
            index_info["index"][self.index_name].update(extra["index"])
        if "job" in extra:
            job_info.update(extra["job"])
        # since the base is the merged collection, we register info there
        build = src_build.find_one({'_id': self.target_name})
        assert build, "Can't find build document '%s'" % self.target_name
        if init:
            # init timer for this step
            self.ti = time.time()
            src_build.update({'_id': self.target_name}, {"$push": {'jobs': job_info}})
            # now refresh/sync
            build = src_build.find_one({'_id': self.target_name})
        else:
            # merge extra at root level
            # (to keep building data...) and update the last one
            # (it's been properly created before when init=True)
            build["jobs"] and build["jobs"][-1].update(job_info)
            def merge_index_info(target,d):
                if "__REPLACE__" in d.keys():
                    d.pop("__REPLACE__")
                    target = d
                else:
                    for k,v in d.items():
                        if type(v) == dict:
                            if k in target:
                                target[k] = merge_index_info(target[k],v)
                            else:
                                v.pop("__REPLACE__",None)
                                # merge v with "nothing" just to make sure to remove any "__REPLACE__"
                                v = merge_index_info({},v)
                                target[k] = v
                        else:
                            target[k] = v
                return target
            build = merge_index_info(build,index_info)
            src_build.replace_one({"_id" : build["_id"]}, build)

    def post_index(self, target_name, index_name, job_manager, steps=["index","post"], batch_size=10000, ids=None, mode=None):
        """
        Override in sub-class to add a post-index process. Method's signature is the same as index() to get
        the full context. This method will run in a thread (using job_manager.defer_to_thread())
        """
        pass

    def setup_log(self):
        self.logger, self.logfile = get_logger('index_%s' % self.index_name,self.log_folder)

    def get_index_creation_settings(self):
        """
        Override to return a dict containing some extra settings
        for index creation. Dict will be merged with mandatory settings,
        see biothings.utils.es.ESIndexer.create_index for more.
        """
        return {
                # as of ES6, include_in_all was removed, we need to create our own "all" field
                "query": {"default_field": "all"},
                "codec" : "best_compression",
                # as of ES6, analysers/tokenizers must be defined in index settings, during creation
                "analysis": {
                    "analyzer": {
                        # soon deprecated in favor of keyword_lowercase_normalizer
                        "string_lowercase": {
                            "tokenizer": "keyword",
                            "filter": "lowercase"
                            },
                        "whitespace_lowercase": {
                            "tokenizer": "whitespace",
                            "filter": "lowercase"
                            },
                        },
                    "normalizer": {
                        "keyword_lowercase_normalizer": {
                            "filter": [
                                "lowercase"
                                ],
                            "type": "custom",
                            "char_filter": []
                            },
                        }
                    },
                }

    def enrich_final_mapping(self, final_mapping):
        """
        final_mapping is the ES mapping ready to be sent,
        (with "dynamic" and "all" at its root for instance)
        this method gives opportunity to add more mapping definitions
        not directly related to datasources, such as other root keys
        """
        return final_mapping

    def get_mapping(self):
        '''collect mapping data from data sources.
        '''
        mapping = self.build_doc.get("mapping",{})
        # default "all" field to replace include_in_all field in older versions of ES
        mapping["all"] = {'type': 'text'}
        final_mapping = {"properties": mapping, "dynamic": "false"}
        final_mapping = self.enrich_final_mapping(final_mapping)
        final_mapping["_meta"] = self.get_metadata()

        return final_mapping

    def get_metadata(self):
        return self.build_doc.get("_meta",{})

    def get_build(self,target_name=None):
        target_name = target_name or self.target_name
        assert target_name, "target_name must be defined first before searching for builds"
        builds = [b for b in self.build_config["build"] if b == target_name]
        assert len(builds) == 1, "Can't find build for config '%s' and target_name '%s'" % (self.conf_name,self.target_name)
        return self.build_config["build"][builds[0]]

    def get_src_versions(self):
        build = self.get_build()
        return build["src_version"]

    def get_stats(self):
        build = self.get_build()
        return build["stats"]

    def get_timestamp(self):
        build = self.get_build()
        return build["build_date"]

    def get_build_version(self):
        build = self.get_build()
        return build["build_version"]

    def load_build(self, target_name=None):
        '''Load build info from src_build collection.'''
        target_name = target_name or self.target_name
        src_build = get_src_build()
        self.build_doc = src_build.find_one({'_id': target_name})
        assert self.build_doc, "Can't find build document associated to '%s'" % target_name
        _cfg = self.build_doc.get("build_config")
        if _cfg:
            self.build_config = _cfg
            #if not "doc_type" in _cfg:
            #    raise ValueError("Missing 'doc_type' in build config")
            self.doc_type = _cfg.get("doc_type")
            self.num_shards = _cfg.get("num_shards",10) # optional
            self.num_shards = self.num_shards and int(self.num_shards) or self.num_shards
            self.num_replicas = _cfg.get("num_replicas",0) # optional
            self.num_replicas = self.num_replicas and int(self.num_replicas) or self.num_replicas
            self.conf_name = _cfg["name"]
        else:
            raise ValueError("Cannot find build config associated to '%s'" % target_name)
        return _cfg


class ColdHotIndexer(Indexer):
    """
    This indexer works with 2 mongo collections to create a single index.
    - one premerge collection contains "cold" data, which never changes (not updated)
    - another collection contains "hot" data, regularly updated
    Index is created fetching the premerge documents. Then, documents from the hot collection
    are merged by fetching docs from the index, updating them, and putting them back in the index.
    """

    def __init__(self, *args, **kwargs):
        super(ColdHotIndexer,self).__init__(*args, **kwargs)
        self.hot_target_name = None
        self.cold_target_name = None
        self.cold_build_doc = None
        self.hot_build_doc = None
        self.cold_cfg = None
        self.hot_cfg = None

    @asyncio.coroutine
    def index(self, hot_name, index_name, job_manager, steps=["index","post"], batch_size=10000, ids=None, mode="index"):
        """
        Same as Indexer.index method but works with a cold/hot collections strategy: first index the cold collection then
        complete the index with hot collection (adding docs or merging them in existing docs within the index)
        """
        assert job_manager
        # check what to do
        if type(steps) == str:
            steps = [steps]
        self.hot_target_name = hot_name
        self.setup_log()
        self.load_build()
        if type(index_name) == list:
            # values are coming from target names, use the cold
            self.index_name = self.hot_target_name
        else:
            self.index_name = index_name
        got_error = False
        cnt = 0
        if "index" in steps:
            # selectively index cold then hot collections, using default index method
            # but specifically 'index' step to prevent any post-process before end of
            # index creation
            # Note: copy backend values as there are some references values between cold/hot and build_doc
            hot_backend_url = self.hot_build_doc["backend_url"]
            cold_backend_url = self.cold_build_doc["backend_url"]
            # target collection is taken from backend_url field, temporarily override.
            self.build_doc["backend_url"] = cold_backend_url
            cold_task = super(ColdHotIndexer,self).index(self.cold_target_name,
                                                         self.index_name,steps="index",
                                                         job_manager=job_manager,
                                                         batch_size=batch_size,ids=ids,mode=mode)
            # wait until cold is fully indexed
            yield from cold_task
            # use updating indexer worker for hot to merge in index
            # back to hot collection
            self.build_doc["backend_url"] = hot_backend_url
            hot_task = super(ColdHotIndexer,self).index(self.hot_target_name,
                                                         self.index_name,steps="index",
                                                         job_manager=job_manager,
                                                         batch_size=batch_size,ids=ids,mode="merge")
            task = asyncio.ensure_future(hot_task)
            def done(f):
                nonlocal got_error
                nonlocal cnt
                try:
                    res = f.result()
                    # compute overall inserted/updated records
                    cnt = sum(res.values())
                    self.register_status("success",job={"step":"index"},index={"count":cnt})
                    self.logger.info("index '%s' successfully created" % index_name,extra={"notify":True})
                except Exception as e:
                    logging.exception("failed indexing cold/hot collections: %s" % e)
                    got_error = e
                    raise
            task.add_done_callback(done)
            yield from task
            if got_error:
                raise got_error
        if "post" in steps:
            # use super index but this time only on hot collection (this is the entry point, cold collection
            # remains hidden from outside)
            hot_task = super(ColdHotIndexer,self).index(self.hot_target_name,
                                                         self.index_name,steps="post",
                                                         job_manager=job_manager,
                                                         batch_size=batch_size,ids=ids,mode=mode)
            task = asyncio.ensure_future(hot_task)
            def posted(f):
                nonlocal got_error
                try:
                    res = f.result()
                    # no need to process the return value more, it's been done in super
                except exception as e:
                    self.logger.error("Post-index process failed for index '%s': %s" % (self.index_name,e),extra={"notify":True})
                    got_error = e
                    raise
            task.add_done_callback(posted)
            yield from task
            if got_error:
                raise got_error

        return {self.index_name:cnt}

    # by default, build_doc is considered to be the hot one
    # (mainly used so we can call super methods as parent)
    @property
    def build_doc(self):
        return self.hot_build_doc
    @build_doc.setter
    def build_doc(self,val):
        self.hot_build_doc = val

    def get_mapping(self):
        final_mapping = super(ColdHotIndexer,self).get_mapping()
        cold_mapping = self.cold_build_doc.get("mapping",{})
        final_mapping["properties"].update(cold_mapping) # mix cold&hot
        return final_mapping

    def get_metadata(self):
        meta = merge_src_build_metadata([self.cold_build_doc,self.hot_build_doc])
        return meta

    def get_src_versions(self):
        _meta = self.get_metadata()
        return _meta["src_version"]

    def get_stats(self):
        _meta = self.get_metadata()
        return _meta["stats"]

    def get_timestamp(self):
        _meta = self.get_metadata()
        return _meta["build_date"]

    def get_build_version(self):
        _meta = self.get_metadata()
        return _meta["build_version"]

    def load_build(self):
        """
        Load cold and hot build documents.
        Index settings are the one declared in the hot build doc.
        """
        src_build = get_src_build()
        # we don't want to reload build docs if they are already loaded
        # so we can temporarily override values when dealing with cold/hot collection
        # (kind of a hack, not really clean, but...)
        if self.hot_build_doc and self.cold_build_doc and self.build_doc:
            self.logger.debug("Build documents already loaded")
            return
        self.hot_build_doc = src_build.find_one({'_id': self.hot_target_name})
        # search the cold collection definition
        assert "build_config" in self.hot_build_doc and "cold_collection" in self.hot_build_doc["build_config"], \
                "Can't find cold_collection field in build_config"
        self.cold_target_name = self.hot_build_doc["build_config"]["cold_collection"]
        self.cold_build_doc = src_build.find_one({'_id': self.cold_target_name})
        # we'll register everything (status) on the hot one
        self.build_doc = self.hot_build_doc
        assert self.cold_build_doc, "Can't find build document associated to '%s'" % self.cold_target_name
        assert self.hot_build_doc, "Can't find build document associated to '%s'" % self.hot_target_name
        self.cold_cfg = self.cold_build_doc.get("build_config")
        self.hot_cfg = self.hot_build_doc.get("build_config")
        if self.hot_cfg or not self.cold_cfg:
            self.build_config = self.hot_cfg
            if not "doc_type" in self.hot_cfg:
                raise ValueError("Missing 'doc_type' in build config")
            self.doc_type = self.hot_cfg["doc_type"]
            self.num_shards = self.hot_cfg.get("num_shards",10) # optional
            self.num_shards = self.num_shards and int(self.num_shards) or self.num_shards
            self.num_replicas = self.hot_cfg.get("num_replicas",0) # optional
            self.num_replicas = self.num_replicas and int(self.num_replicas) or self.num_replicas
            self.conf_name = self.hot_cfg["name"]
        else:
            raise ValueError("Cannot find build config associated to '%s' or '%s'" % (self.hot_target_name,self.cold_target_name))
        return (self.cold_cfg,self.hot_cfg)


class IndexManager(BaseManager):

    DEFAULT_INDEXER = Indexer

    def __init__(self, *args, **kwargs):
        super(IndexManager,self).__init__(*args, **kwargs)
        self.src_build = get_src_build()
        self.indexers = {}
        self.es_config = {}
        self.t0 = time.time()
        self.prepared = False
        self.log_folder = LOG_FOLDER
        self.timestamp = datetime.now()
        self.setup()

    def clean_stale_status(self):
        src_build = get_src_build()
        for build in src_build.find():
            dirty = False
            for job in build.get("jobs",[]):
                if job.get("status") == "indexing":
                    logging.warning("Found stale build '%s', marking index status as 'canceled'" % build["_id"])
                    job["status"] = "canceled"
                    dirty = True
            if dirty:
                src_build.replace_one({"_id":build["_id"]},build)

    def setup(self):
        self.setup_log()

    def setup_log(self):
        self.logger, self.logfile = get_logger('indexmanager',self.log_folder)

    def get_predicates(self):
        def no_other_indexmanager_step_running(job_manager):
            """IndexManager deals with snapshot, publishing,
            none of them should run more than one at a time"""
            return len([j for j in job_manager.jobs.values() if j["category"] == INDEXMANAGER_CATEGORY]) == 0
        return [no_other_indexmanager_step_running]

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {"category" : INDEXMANAGER_CATEGORY,
                "source" : "",
                "step" : "",
                "description" : ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def __getitem__(self,conf_name):
        """
        Return an instance of an indexer for the build configuration named 'conf_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        kwargs = BaseManager.__getitem__(self,conf_name)
        return kwargs

    def configure_from_list(self,indexers_kwargs):
        for dindex in indexers_kwargs:
            assert len(dindex) == 1, "Invalid indexer registration data: %s" % dindex
            env,idxkwargs = list(dindex.items())[0]
            self.register[env] = idxkwargs

    def configure_from_dict(self,confdict):
        self.es_config = copy.deepcopy(confdict)
        self.indexers.update(confdict.get("indexer_select",{}))
        indexers_kwargs = []
        for env,conf in confdict["env"].items():
            idxkwargs = dict(**conf["indexer"]["args"])
            # propagate ES host to indexer's kwargs
            idxkwargs["es_host"] = self.es_config["env"][env]["host"]
            indexers_kwargs.append({env:idxkwargs})
        self.configure_from_list(indexers_kwargs)

    def configure(self,indexer_defs):
        """
        Register indexers with:
        - a list of dict as:
            [{"indexer_type_name": partial},{....}]
        - a dict containing all indexer definitions:
            {"env" : {
                "env1" : {
                    "host": "localhost:9200",
                    "timeout": ..., "retry":...,
                    "indexer" : "path.to.ClassIndexer",
                },
                ...
            }
        Partial is used to instantiate an indexer, without args
        """
        if type(indexer_defs) == list:
            self.configure_from_list(indexer_defs)
        elif type(indexer_defs) == dict:
            self.configure_from_dict(indexer_defs)
        else:
            raise ValueError("Unknown indexer definitions type (expecting a list or a dict")
        self.logger.info(self.indexers)
        self.logger.info(self.register)

    def find_indexer(self, target_name):
        """
        Return indexer class required to index target_name.
        Rules depend on what's inside the corresponding src_build doc
        and the indexers definitions
        """
        doc = self.src_build.find_one({"_id":target_name})
        if not self.indexers or not doc:
            return self.__class__.DEFAULT_INDEXER
        klass = None
        for path_in_doc in self.indexers:
            if klass is None and (
                    path_in_doc is None or \
                    path_in_doc == "default" or \
                    path_in_doc == ""):
                # couldn't find a klass yet and we found a default declated, keep it
                strklass = self.indexers[path_in_doc]
                klass = get_class_from_classpath(strklass)
            else:
                try:
                    val = get_dotfield_value(path_in_doc,doc)
                    strklass = self.indexers[path_in_doc]
                    klass = get_class_from_classpath(strklass)
                    self.logger.info("Found special indexer '%s' required to index '%s'" % (klass,target_name))
                    # the first to match wins
                    break
                except KeyError:
                    pass
        if klass is None:
            self.logger.debug("Using default indexer")
            return self.__class__.DEFAULT_INDEXER
        else:
            # either we return a default declared in config or
            # a specific one found according to the doc
            self.logger.debug("Using custom indexer %s" % klass)
            return klass

    def index(self, indexer_env, target_name=None, index_name=None, ids=None, **kwargs):
        """
        Trigger an index creation to index the collection target_name and create an
        index named index_name (or target_name if None). Optional list of IDs can be
        passed to index specific documents.
        """
        t0 = time.time()
        def indexed(f):
            try:
                res = f.result()
                self.logger.info("Done indexing target '%s' to index '%s': %s" % (target_name,index_name,res))
            except Exception as e:
                self.logger.exception("Error while running index job, %s" % e)
                raise
        idxklass = self.find_indexer(target_name)
        idxkwargs = self[indexer_env]
        idx = idxklass(**idxkwargs)
        idx.env = indexer_env
        idx.target_name = target_name
        index_name = index_name or target_name
        job = idx.index(target_name, index_name, ids=ids, job_manager=self.job_manager, **kwargs)
        job = asyncio.ensure_future(job)
        job.add_done_callback(indexed)

        return job

    def update_metadata(self, indexer_env, index_name, build_name=None,_meta=None):
        """
        Update _meta for index_name, based on build_name (_meta directly
        taken from the src_build document) or _meta
        """
        idxkwargs = self[indexer_env]
        # 1st pass we get the doc_type (don't want to ask that on the signature...)
        indexer = create_backend((idxkwargs["es_host"],index_name,None)).target_esidxer
        m = indexer._es.indices.get_mapping(index_name)
        assert len(m[index_name]["mappings"]) == 1, "Found more than one doc_type: " + \
                    "%s" % m[index_name]["mappings"].keys()
        doc_type = list(m[index_name]["mappings"].keys())[0]
        # 2nd pass to re-create correct indexer
        indexer = create_backend((idxkwargs["es_host"],index_name,doc_type)).target_esidxer
        if build_name:
            build = get_src_build().find_one({"_id":build_name})
            assert build, "No such build named '%s'" % build_name
            _meta = build.get("_meta")
        assert not _meta is None, "No _meta found"
        return indexer.update_mapping_meta({"_meta" : _meta})

    def index_info(self, env=None, remote=False):
        res = copy.deepcopy(self.es_config)
        for kenv in self.es_config["env"]:
            if env and env != kenv:
                continue
            if remote:
                # lost all indices, remotely
                try:
                    cl = Elasticsearch(res["env"][kenv]["host"],timeout=1,max_retries=0)
                    indices = [{"index":k,
                        "doc_type":list(v["mappings"].keys())[0],
                        "aliases":list(v["aliases"].keys())}
                        for k,v in cl.indices.get("*").items()]
                    # init index key if not done in config var
                    # (a default index name can be specified in config...)
                    if not "index" in res["env"][kenv]:
                        res["env"][kenv]["index"] = []
                    assert type(res["env"][kenv]["index"]) == list
                    res["env"][kenv]["index"].extend(indices)
                except Exception as e:
                    self.logger.exception("Can't load remote indices: %s" % e)
                    continue
        return res

    def validate_mapping(self, mapping, env):
        idxkwargs = self[env]
        # just get the default indexer (target_name doesn't exist, return default one)
        idxklass = self.find_indexer(target_name="__placeholder_name__%s" % get_random_string())
        idxr_obj = idxklass(**idxkwargs)
        settings = idxr_obj.get_index_creation_settings()
        # generate a random index, it'll be deleted at the end
        index_name = ("hub_tmp_%s" % get_random_string()).lower()
        idxr = ESIndexer(index=index_name,es_host=idxr_obj.host,doc_type=None)
        self.logger.info("Testing mapping by creating index '%s' on host '%s' (settings: %s)" % \
                (index_name,idxr_obj.host,settings))
        try:
            res = idxr.create_index(mapping,settings)
            return res
        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            try:
                idxr.delete_index()
            except Exception as e:
                pass

