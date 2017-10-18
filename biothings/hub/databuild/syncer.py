import sys, re, math
import os, glob, json
import time
import copy
import importlib
import pickle
from datetime import datetime
from pprint import pformat
import asyncio
from functools import partial
from elasticsearch.helpers import BulkIndexError
from elasticsearch.exceptions import NotFoundError, ConflictError

from biothings.utils.common import timesofar, iter_n, loadobj, dump
from biothings.utils.mongo import doc_feeder, get_target_db, invalidate_cache
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger, HipchatHandler
from biothings import config as btconfig
from biothings.utils.manager import BaseManager, ManagerError
from .backend import create_backend
from ..dataload.storage import UpsertStorage
import biothings.utils.jsonpatch as jsonpatch
from biothings.hub import SYNCER_CATEGORY

logging = btconfig.logger

class SyncerException(Exception):
    pass


class BaseSyncer(object):

    # diff type name, identifying the diff algorithm
    # must be set in sub-class
    diff_type = None

    # backend used to sync data (mongo / es)
    # must be set in sub-class
    target_backend = None

    def __init__(self, job_manager, log_folder):
        self.log_folder = log_folder
        self.job_manager = job_manager
        self.timestamp = datetime.now()
        self.synced_cols = None # str representation of synced cols (internal usage)
        self.setup_log()

    def setup_log(self):
        # TODO: use bt.utils.loggers.get_logger
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, 'sync_%s.log' % time.strftime("%Y%m%d",self.timestamp.timetuple()))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("sync")
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not nh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(nh)
        return self.logger

    def get_predicates(self, running_jobs={}):
        if not running_jobs:
            return None
        #def no_same_syncer_running():
        #    """Avoid syncers collision"""
        #    return len([j for j in running_jobs.values() if \
        #            j["source"] == self.synced_cols and j["category"] == SYNCER_CATEGORY]) == 0
        return []

    def get_pinfo(self, job_manager=None):
        pinfo = {"category" : SYNCER_CATEGORY,
                 "step" : "",
                 "description" : ""}
        preds = self.get_predicates(job_manager.jobs)
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    @asyncio.coroutine
    def sync_cols(self, diff_folder, batch_size=10000, mode=None, force=False,
            target_backend=None,steps=["mapping","content","meta"]):
        """
        Sync a collection with diff files located in diff_folder. This folder contains a metadata.json file which
        describes the different involved collection: "old" is the collection/index to be synced, "new" is the collecion
        that should be obtained once all diff files are applied (not used, just informative).
        If target_backend (bt.databbuild.backend.create_backend() notation), then it will replace "old" (that is, the one
        being synced)
        """
        got_error = False
        cnt = 0
        jobs = []
        meta = json.load(open(os.path.join(diff_folder,"metadata.json")))
        diff_type = self.diff_type
        selfcontained = "selfcontained" in meta["diff"]["type"]
        # first try to use what's been passed explicitely
        # then default to what's in config (tuple will be used for create_backend() call)
        # or use what we have in the diff metadata
        old_db_col_names = target_backend or \
            (btconfig.ES_HOST,btconfig.ES_INDEX_NAME,btconfig.ES_DOC_TYPE) or \
            meta["old"]["backend"]
        new_db_col_names = meta["new"]["backend"]
        diff_mapping_file = meta["diff"]["mapping_file"]
        pinfo = self.get_pinfo()
        self.synced_cols = "%s -> %s" % (old_db_col_names,new_db_col_names)
        pinfo["source"] = self.synced_cols
        summary = {}
        if "mapping" in steps and self.target_backend == "es":
            if diff_mapping_file:
                # old_db_col_names is actually the index name in that case
                index_name = old_db_col_names[1]
                doc_type = meta["build_config"]["doc_type"]
                indexer = create_backend(old_db_col_names).target_esidxer
                pinfo["step"] = "mapping"
                pinfo["description"] = diff_mapping_file

                def update_mapping():
                    diffm = os.path.join(diff_folder,diff_mapping_file["name"])
                    ops = loadobj(diffm)
                    mapping = indexer.get_mapping()
                    # we should have the same doc type declared in the mapping
                    mapping[doc_type]["properties"] = jsonpatch.apply_patch(mapping[doc_type]["properties"],ops)
                    res = indexer.update_mapping(mapping)
                    return res

                job = yield from self.job_manager.defer_to_thread(pinfo, partial(update_mapping))

                def updated(f):
                    try:
                        res = f.result()
                        self.logger.info("Mapping updated on index '%s'" % index_name)
                        summary["mapping_updated"] = True
                    except Exception as e:
                        self.logger.error("Failed to update mapping on index '%s': %s" % (index_name,e))
                        got_error = e

                job.add_done_callback(updated)
                yield from job

            if got_error:
                self.logger.error("Failed to update mapping on index '%s': %s" % \
                    (old_db_col_names, got_error),extra={"notify":True})
                raise got_error

        if "content" in steps:
            if selfcontained:
                # selfconained is a worker param, isolate diff format
                diff_type = diff_type.replace("-selfcontained","")
            diff_files = [os.path.join(diff_folder,e["name"]) for e in meta["diff"]["files"]]
            total = len(diff_files)
            self.logger.info("Syncing %s to %s using diff files in '%s'" % (old_db_col_names,new_db_col_names,diff_folder))
            pinfo["step"] = "content"
            for diff_file in diff_files:
                cnt += 1
                pinfo["description"] = "file %s (%s/%s)" % (diff_file,cnt,total)
                worker = getattr(sys.modules[self.__class__.__module__],"sync_%s_%s_worker" % \
                        (self.target_backend,diff_type))
                self.logger.info("Creating sync worker %s for file %s (%s/%s)" % (worker.__name__,diff_file,cnt,total))
                job = yield from self.job_manager.defer_to_process(pinfo,
                        partial(worker, diff_file, old_db_col_names, new_db_col_names, batch_size, cnt,
                            force, selfcontained, meta))
                jobs.append(job)
            def synced(f):
                try:
                    for d in f.result():
                        for k in d:
                            summary.setdefault(k,0)
                            summary[k] += d[k] 
                except Exception as e:
                    got_error = e
                    raise
            tasks = asyncio.gather(*jobs)
            tasks.add_done_callback(synced)
            yield from tasks
            if got_error:
                self.logger.error("Failed to sync collection from %s to %s using diff files in '%s': %s" % \
                    (old_db_col_names, new_db_col_names, diff_folder, got_error),extra={"notify":True})
                raise got_error

        if "meta" in steps and self.target_backend == "es":
            # old_db_col_names is actually the index name in that case
            index_name = old_db_col_names[1]
            doc_type = meta["build_config"]["doc_type"]
            indexer = create_backend(old_db_col_names).target_esidxer
            new_meta = meta["_meta"]
            pinfo["step"] = "metadata"

            def update_metadata():
                res = indexer.update_mapping_meta({"_meta" : new_meta})
                return res

            job = yield from self.job_manager.defer_to_thread(pinfo, partial(update_metadata))

            def updated(f):
                try:
                    res = f.result()
                    self.logger.info("Metadata updated on index '%s': %s" % (index_name,res))
                    summary["metadata_updated"] = True
                except Exception as e:
                    self.logger.error("Failed to update metadata on index '%s': %s" % (index_name,e))
                    got_error = e

            job.add_done_callback(updated)
            yield from job

            if got_error:
                self.logger.error("Failed to update metadata on index '%s': %s" % \
                    (old_db_col_names, got_error),extra={"notify":True})
                raise got_error

        self.logger.info("Succesfully synced index %s to reach collection %s using diff files in '%s': %s" % \
                (old_db_col_names, new_db_col_names, diff_folder,summary),extra={"notify":True})

        return summary

    def sync(self, diff_folder=None, batch_size=10000, mode=None, target_backend=None,
            steps=["mapping","content","meta"]):
        """wrapper over sync_cols() coroutine, return a task"""
        job = asyncio.ensure_future(self.sync_cols(
                    diff_folder=diff_folder, batch_size=batch_size, mode=mode,
                    target_backend=target_backend,steps=steps))
        return job


class ThrottlerSyncer(BaseSyncer):

    def __init__(self, max_sync_workers, *args, **kwargs):
        super(ThrottlerSyncer,self).__init__(*args, **kwargs)
        self.max_sync_workers = max_sync_workers

    def get_predicates(self, running_jobs={}):
        preds = super(ThrottlerSyncer,self).get_predicates(running_jobs)
        if preds is None:
            preds = []
        def not_too_much_syncers():
            """
            Limit number of syncers accordingly (this is useful when live-updating
            the prod,we usually need to reduce the number of sync workers as they
            would kill the ES server otherwise... (or at least produces timeout errors)
            """
            return len([j for j in running_jobs.values() if \
                    j["category"] == "sync"]) < self.max_sync_workers
        preds.append(not_too_much_syncers)
        return preds


class MongoJsonDiffSyncer(BaseSyncer):
    diff_type = "jsondiff"
    target_backend = "mongo"

class MongoJsonDiffSelfContainedSyncer(BaseSyncer):
    diff_type = "jsondiff-selfcontained"
    target_backend = "mongo"

class ESJsonDiffSyncer(BaseSyncer):
    diff_type = "jsondiff"
    target_backend = "es"

class ESJsonDiffSelfContainedSyncer(BaseSyncer):
    diff_type = "jsondiff-selfcontained"
    target_backend = "es"

class ThrottledESJsonDiffSyncer(ThrottlerSyncer,ESJsonDiffSyncer): pass
class ThrottledESJsonDiffSelfContainedSyncer(ThrottlerSyncer,ESJsonDiffSelfContainedSyncer): pass


# TODO: refactor workers (see sync_es_...)
def sync_mongo_jsondiff_worker(diff_file, old_db_col_names, new_db_col_names, batch_size, cnt,
        force=False, selfcontained=False, metadata={}):
    """Worker to sync data between a new and an old mongo collection"""
    # check if diff files was already synced
    res = {"added": 0, "updated": 0, "deleted": 0, "skipped": 0}
    synced_file = "%s.synced" % diff_file
    if os.path.exists(synced_file):
        logging.info("Diff file '%s' already synced, skip it" % os.path.basename(diff_file))
        diff = loadobj(synced_file)
        res["skipped"] += len(diff["add"]) + len(diff["delete"]) + len(diff["update"])
        return res
    new = create_backend(new_db_col_names)
    old = create_backend(old_db_col_names)
    storage = UpsertStorage(get_target_db(),old.target_collection.name,logging)
    diff = loadobj(diff_file)
    assert new.target_collection.name == diff["source"], "Source is different in diff file '%s': %s" % (diff_file,diff["source"])

    # add: get ids from "new" 
    if selfcontained:
        # diff["add"] contains all documents, not mongo needed
        for docs in iter_n(diff["add"],batch_size):
            res["added"] += storage.process((d for d in docs),batch_size)
    else:
        cur = doc_feeder(new.target_collection, step=batch_size, inbatch=False, query={'_id': {'$in': diff["add"]}})
        for docs in iter_n(cur,batch_size):
            # use generator otherwise process/doc_iterator will require a dict (that's bad...)
            res["added"] += storage.process((d for d in docs),batch_size)

    # update: get doc from "old" and apply diff
    batch = []
    for patch_info in diff["update"]:
        doc = old.get_from_id(patch_info["_id"])
        try:
            doc = jsonpatch.apply_patch(doc,patch_info["patch"])
            batch.append(doc)
        except jsonpatch.JsonPatchConflict:
            # assuming already applieda
            res["skipped"] += 1
            continue
        if len(batch) >= batch_size:
            res["updated"] += storage.process((d for d in batch),batch_size)
            batch = []
    if batch:
        res["updated"] += storage.process((d for d in batch),batch_size)

    # delete: remove from "old"
    for ids in iter_n(diff["delete"],batch_size):
        res["deleted"] += old.remove_from_ids(ids)

    # we potentially modified the "old" collection so invalidate cache just to make sure
    invalidate_cache(old.target_collection.name,"target")
    logging.info("Done applying diff from file '%s': %s" % (diff_file,res))
    # mark as synced
    os.rename(diff_file,synced_file)
    return res


def sync_es_jsondiff_worker(diff_file, es_config, new_db_col_names, batch_size, cnt,
        force=False, selfcontained=False, metadata={}):
    """Worker to sync data between a new mongo collection and an elasticsearch index"""
    res = {"added": 0, "updated": 0, "deleted": 0, "skipped": 0}
    # check if diff files was already synced
    synced_file = "%s.synced" % diff_file
    if os.path.exists(synced_file):
        logging.info("Diff file '%s' already synced, skip it" % os.path.basename(diff_file))
        diff = loadobj(synced_file)
        res["skipped"] += len(diff["add"]) + len(diff["delete"]) + len(diff["update"])
        return res
    eskwargs = {}
    # pass optional ES Indexer args
    if hasattr(btconfig,"ES_TIMEOUT"):
        eskwargs["timeout"] = btconfig.ES_TIMEOUT
    if hasattr(btconfig,"ES_MAX_RETRY"):
        eskwargs["max_retries"] = btconfig.ES_MAX_RETRY
    if hasattr(btconfig,"ES_RETRY"):
        eskwargs["retry_on_timeout"] = btconfig.ES_RETRY
    logging.debug("Create ES backend with args: (%s,%s)" % (es_config,eskwargs))
    bckend = create_backend(es_config,**eskwargs)
    indexer = bckend.target_esidxer
    diff = loadobj(diff_file)
    errors = []
    # add: get ids from "new" 
    if selfcontained:
        # diff["add"] contains all documents, no mongo needed
        cur = diff["add"]
    else:
        new = create_backend(new_db_col_names) # mongo collection to sync from
        assert new.target_collection.name == diff["source"], "Source is different in diff file '%s': %s" % (diff_file,diff["source"])
        cur = doc_feeder(new.target_collection, step=batch_size, inbatch=False, query={'_id': {'$in': diff["add"]}})
    for docs in iter_n(cur,batch_size):
        try:
            res["added"] += indexer.index_bulk(docs,batch_size,action="create")[0]
        except BulkIndexError:
            for doc in docs:
                _id = doc.pop("_id")
                try:
                     # force action=create to spot docs already added
                     indexer.index(doc,_id,action="create")
                     res["added"] += 1
                except ConflictError:
                    # already added
                    res["skipped"] += 1
                    continue
                except Exception as e:
                    errors.append({"_id":_id,"file":diff_file,"error":e})
                    import pickle
                    pickle.dump(errors,open("errors","wb"))
                    raise

    # update: get doc from indexer and apply diff
    batch = []
    ids = [p["_id"] for p in diff["update"]]
    for i,doc in enumerate(indexer.get_docs(ids)):
        try:
            patch_info = diff["update"][i] # same order as what's return by get_doc()...
            assert patch_info["_id"] == doc["_id"],"%s != %s" % (patch_info["_id"],doc["_id"]) # ... but just make sure
            newdoc = jsonpatch.apply_patch(doc,patch_info["patch"])
            if newdoc == doc:
                # already applied
                res["skipped"] += 1
                continue
            batch.append(newdoc)
        except jsonpatch.JsonPatchConflict:
            # assuming already applieda
            res["skipped"] += 1
            continue
        if len(batch) >= batch_size:
            res["updated"] += indexer.index_bulk(batch,batch_size)[0]
            batch = []
    if batch:
        res["updated"] += indexer.index_bulk(batch,batch_size)[0]

    # delete: remove from "old"
    for ids in iter_n(diff["delete"],batch_size):
        del_skip = indexer.delete_docs(ids)
        res["deleted"] += del_skip[0]
        res["skipped"] += del_skip[1]

    logging.info("Done applying diff from file '%s': %s" % (diff_file,res))
    # mark as synced
    os.rename(diff_file,synced_file)
    return res


class SyncerManager(BaseManager):

    def __init__(self, *args,**kwargs):
        """
        SyncerManager deals with the different syncer objects used to synchronize
        different collections or indices using diff files
        """
        super(SyncerManager,self).__init__(*args,**kwargs)
        self.setup_log()

    def register_syncer(self,klass):
        if type(klass) == partial:
            assert type(klass.func) == type, "%s is not a class" % klass.func
            diff_type,target_backend = klass.func.diff_type,klass.func.target_backend
        else:
            diff_type,target_backend = klass.diff_type,klass.target_backend

        self.register[(diff_type,target_backend)] = partial(klass, log_folder=btconfig.LOG_FOLDER,
                                           job_manager=self.job_manager)

    def configure(self,klasses=None):
        """
        Register default syncers (if klasses is None) or given klasses.
        klasses is a list of class, or a list of partial'ly initialized classes.
        """
        klasses = klasses or [MongoJsonDiffSyncer,ESJsonDiffSyncer,
                MongoJsonDiffSelfContainedSyncer,ESJsonDiffSelfContainedSyncer]
        for klass in klasses:
            self.register_syncer(klass)

    def setup_log(self):
        self.logger = btconfig.logger

    def __getitem__(self,diff_target):
        """
        Return an instance of a builder for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self,diff_target)
        return pclass()

    def sync(self, backend_type, diff_folder, batch_size=100000, mode=None, target_backend=None,
            steps=["mapping","content","meta"]):
        if not os.path.exists(diff_folder):
            raise FileNotFoundError("Directory '%s' does not exist, run a diff first" % diff_folder)
        # load metadata to know collections that have been diffed in diff_func protocol
        try:
            meta = json.load(open(os.path.join(diff_folder,"metadata.json")))
        except FileNotFoundError as e:
            self.logger.error("Can't find metadata file in diff folder '%s'" % diff_folder)
            raise

        self.logger.info("Found metadata information: %s" % pformat(meta))
        try:
            diff_type = meta["diff"]["type"]
        except KeyError as e:
            msg = "Can't find diff_type in metadata file located in '%s'" % diff_folder
            raise SyncerException(msg)

        try:
            syncer = self[(diff_type,backend_type)]
            job = syncer.sync(diff_folder,batch_size=batch_size,mode=mode,
                    target_backend=target_backend)
            return job
        except KeyError as e:
            raise SyncerException("No such syncer (%s,%s) (error: %s)" % (diff_type,backend_type,e))

