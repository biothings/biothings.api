import sys, re, math
import os, glob
import time
import copy
import importlib
import pickle
from datetime import datetime
from pprint import pformat
import asyncio
from functools import partial

from biothings.utils.common import timesofar, iter_n, loadobj
from biothings.utils.mongo import doc_feeder, get_target_db, invalidate_cache
from biothings.utils.loggers import get_logger, HipchatHandler
from biothings import config as btconfig
from biothings.utils.manager import BaseManager, ManagerError
from biothings.databuild.backend import create_backend
from biothings.dataload.storage import UpsertStorage
import biothings.utils.jsonpatch as jsonpatch
from biothings.utils.diff import generate_diff_folder

logging = btconfig.logger

class SyncerException(Exception):
    pass


class BaseSyncer(object):

    # diff type name, identifying the diff algorithm
    # must be set in sub-class
    diff_type = None

    def __init__(self, job_manager, log_folder):
        self.log_folder = log_folder
        self.job_manager = job_manager
        self.timestamp = datetime.now()
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

    @asyncio.coroutine
    def sync_cols(self,old_db_col_names, new_db_col_names, batch_size=10000, mode=None):
        new = create_backend(new_db_col_names)
        old = create_backend(old_db_col_names)
        diff_folder = generate_diff_folder(old_db_col_names,new_db_col_names)

        got_error = False
        cnt = 0
        jobs = []
        pinfo = {"category" : "sync",
                 "source" : "%s -> %s" % (old.target_collection.name,new.target_collection.name),
                 "description" : ""}

        diff_files = glob.glob(os.path.join(diff_folder,"*.pyobj"))
        total = len(diff_files)
        summary = {}
        self.logger.info("Syncing from %s to %s using diff files in '%s'" % (old.target_collection.name, \
                new.target_collection.name, diff_folder))
        for diff_file in diff_files:
            cnt += 1
            pinfo["description"] = "file %s (%s/%s)" % (diff_file,cnt,total)
            self.logger.info("Creating sync worker for file %s (%s/%s)" % (diff_file,cnt,total))
            job = yield from self.job_manager.defer_to_process(pinfo,
                    partial(sync_jsondiff_worker, diff_file, old_db_col_names, new_db_col_names, batch_size, cnt))
            jobs.append(job)
        def synced(f):
            try:
                for d in f.result():
                    for k in d:
                        summary.setdefault(k,0)
                        summary[k] += d[k] 
                # we potentially modified the "old" collection so invalidate cache just to make sure
                invalidate_cache(old.target_collection.name,"target")
            except Exception as e:
                got_error = e
                raise
        tasks = asyncio.gather(*jobs)
        tasks.add_done_callback(synced)
        yield from tasks
        if got_error:
            raise got_error
        self.logger.info("Succesfully synced collection %s from collection %s using diff files in '%s': %s" % \
                (old.target_collection.name,new.target_collection.name,diff_folder,summary),extra={"notify":True})
        return summary

    def sync(self, old_db_col_names, new_db_col_names, batch_size=10000, mode=None):
        """wrapper over sync_cols() coroutine, return a task"""
        job = asyncio.ensure_future(self.sync_cols(old_db_col_names, new_db_col_names, batch_size, mode))
        return job


class JsonDiffSyncer(BaseSyncer):
    diff_type = "jsondiff"


def sync_jsondiff_worker(diff_file, old_db_col_names, new_db_col_names, batch_size, cnt):
    new = create_backend(new_db_col_names)
    old = create_backend(old_db_col_names)
    storage = UpsertStorage(get_target_db(),old.target_collection.name,logging)
    diff = loadobj(diff_file)
    assert new.target_collection.name == diff["source"], "Source is different in diff file '%s': %s" % (diff_file,diff["source"])
    added = 0
    updated = 0
    deleted = 0
    # add: get ids from "new" 
    cur = doc_feeder(new.target_collection, step=batch_size, inbatch=False, query={'_id': {'$in': diff["add"]}})
    for docs in iter_n(cur,batch_size):
        # use generator otherwise process/doc_iterator will require a dict (that's bad...)
        added += storage.process((d for d in docs),batch_size)
    # update: get doc from "old" and apply diff
    batch = []
    for patch_info in diff["update"]:
        doc = old.get_from_id(patch_info["_id"])
        doc = jsonpatch.apply_patch(doc,patch_info["patch"])
        batch.append(doc)
        if len(batch) >= batch_size:
            updated += storage.process((d for d in batch),batch_size)
            batch = []
    if batch:
        updated += storage.process((d for d in batch),batch_size)
    # delete: remove from "old"
    for ids in iter_n(diff["delete"],batch_size):
        deleted += old.remove_from_ids(ids)

    return {"added": added, "updated": updated, "deleted": deleted}


class SyncerManager(BaseManager):

    def __init__(self, *args,**kwargs):
        """
        SyncerManager deals with the different syncer objects used to synchronize
        different collections or indices using diff files
        """
        super(SyncerManager,self).__init__(*args,**kwargs)
        self.setup_log()

    def register_syncer(self,klass):
        self.register[klass.diff_type] = partial(klass,log_folder=btconfig.LOG_FOLDER,
                                           job_manager=self.job_manager)

    def configure(self):
        for klass in [JsonDiffSyncer]: # TODO: make it dynamic...
            self.register_syncer(klass)

    def setup_log(self):
        self.logger = btconfig.logger

    def __getitem__(self,diff_type):
        """
        Return an instance of a builder for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self,diff_type)
        return pclass()

    def sync(self, old_db_col_names, new_db_col_names, batch_size=100000, mode=None):
        diff_folder = generate_diff_folder(old_db_col_names,new_db_col_names)
        if not os.path.exists(diff_folder):
            raise FileNotFoundError("Directory '%s' does not exist, run a diff first" % diff_folder)

        # load metadata to know collections that have been diffed in diff_func protocol
        try:
            meta = loadobj(os.path.join(diff_folder,"metadata.pick"),"rb")
        except FileNotFoundError as e:
            self.logger.error("Can't find metadata file in diff folder '%s'" % diff_folder)
            raise

        self.logger.info("Found metadata information: %s" % meta)
        try:
            diff_type = meta["diff_type"]
        except KeyError as e:
            msg = "Can't find diff_type in metadata file located in '%s'" % diff_folder
            raise SyncerException(msg)

        try:
            syncer = self[diff_type]
            job = syncer.sync(old_db_col_names, new_db_col_names,
                              batch_size=batch_size,
                              mode=mode)
            return job
        except KeyError as e:
            raise DifferException("No such syncer '%s' (error: %s)" % (diff_type,e))

