import asyncio
import copy
import json
import os
import pickle
import sys
import time
from datetime import datetime
from functools import partial
from pprint import pformat

from elasticsearch.exceptions import ConflictError
from elasticsearch.helpers import BulkIndexError

import biothings.utils.jsonpatch as jsonpatch
from biothings import config as btconfig
from biothings.hub import SYNCER_CATEGORY
from biothings.hub.manager import BaseManager
from biothings.utils.common import iter_n, loadobj, timesofar
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from biothings.utils.mongo import doc_feeder, get_target_db, invalidate_cache
from biothings.utils.storage import UpsertStorage

from .backend import create_backend, generate_folder

logging = btconfig.logger


class SyncerException(Exception):
    pass


class BaseSyncer(object):
    # diff type name, identifying the diff algorithm
    # must be set in sub-class
    diff_type = None

    # backend used to sync data (mongo / es)
    # must be set in sub-class
    target_backend_type = None

    def __init__(self, job_manager, log_folder):
        self.log_folder = log_folder
        self.job_manager = job_manager
        self.timestamp = datetime.now()
        self.ti = time.time()
        self.synced_cols = None  # str representation of synced cols (internal usage)
        self.setup_log()
        # set by manager during instanciation
        self.old = None
        self.new = None
        self.target_backend = None
        self._meta = None

    def setup_log(self, build_name=None):
        log_folder = None
        if build_name:
            log_folder = os.path.join(btconfig.LOG_FOLDER, "build", build_name) if btconfig.LOG_FOLDER else None
        self.logger, self.logfile = get_logger("sync", log_folder=log_folder, force=True)

    def get_predicates(self):
        # def no_same_syncer_running(job_manager):
        #    """Avoid syncers collision"""
        #    return len([j for j in job_manager.jobs.values() if \
        #            j["source"] == self.synced_cols and j["category"] == SYNCER_CATEGORY]) == 0
        return []

    def get_pinfo(self):
        pinfo = {"category": SYNCER_CATEGORY, "step": "", "description": ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def register_status(self, status, transient=False, init=False, **extra):
        src_build = get_src_build()
        job_info = {
            "status": status,
            "step_started_at": datetime.now().astimezone(),
            "logfile": self.logfile,
        }
        # to select correct diff sub-record (1 collection can be diffed with multiple others)
        diff_key = "%s" % self.old.target_name
        # once in diff, select correct sync sub-record (1 diff can be applied to different backend)
        # replace dots as hostname can have dots which could be interpreted as dotted field by mongo
        # also remove doc_type (which can be sometimes None if hub deals with multiple APIs,
        # and is not useful in distinguishing where the diff was applid since there's only one
        # doc type allowed now since ES6 (last element in self.target_backend is doc_type)
        sync_key = "-".join(self.target_backend[:-1]).replace(".", "-")
        sync_info = {sync_key: {}}
        if transient:
            # record some "in-progress" information
            job_info["pid"] = os.getpid()
        else:
            # only register time when it's a final state
            job_info["time"] = timesofar(self.ti)
            t1 = round(time.time() - self.ti, 0)
            job_info["time_in_s"] = t1
            sync_info[sync_key]["created_at"] = datetime.now().astimezone()
        if "sync" in extra:
            sync_info[sync_key].update(extra["sync"])
        if "job" in extra:
            job_info.update(extra["job"])
        # since the base is the merged collection, we register info there
        # as the new collection (diff results are associated to the most recent colleciton)
        build = src_build.find_one({"_id": self.new.target_name})
        if not build:
            self.logger.info("Can't find build document '%s', no status to register" % self.new.target_name)
            return
        assert "diff" in build and diff_key in build["diff"], "Missing previous diff information in build document"
        if init:
            # init timer for this step
            self.ti = time.time()
            src_build.update({"_id": self.new.target_name}, {"$push": {"jobs": job_info}})
            # now refresh/sync
            build = src_build.find_one({"_id": self.new.target_name})
        else:
            # merge extra at root level
            # (to keep building data...) and update the last one
            # (it's been properly created before when init=True)
            build["jobs"] and build["jobs"][-1].update(job_info)

            def merge_info(target, d):
                if "__REPLACE__" in d.keys():
                    d.pop("__REPLACE__")
                    target = d
                else:
                    for k, v in d.items():
                        if isinstance(v, dict):
                            if k in target:
                                target[k] = merge_info(target[k], v)
                            else:
                                v.pop("__REPLACE__", None)
                                # merge v with "nothing" just to make sure to remove any "__REPLACE__"
                                v = merge_info({}, v)
                                target[k] = v
                        else:
                            target[k] = v
                return target

            sync_info = {"sync": merge_info(build["diff"][diff_key].get("sync", {}), sync_info)}
            build["diff"][diff_key].update(sync_info)
            # src_build.update({'_id': build["_id"]}, {"$set": index_info})
            src_build.replace_one({"_id": build["_id"]}, build)

    def load_metadata(self, diff_folder):
        self._meta = json.load(open(os.path.join(diff_folder, "metadata.json")))

    def get_target_backend(self):
        # first try to use what's been passed explicitely
        # then default to what's in config (tuple will be used for create_backend() call)
        # or use what we have in the diff metadata
        old_db_col_names = (
            self.target_backend
            or (btconfig.ES_HOST, btconfig.ES_INDEX_NAME, btconfig.ES_DOC_TYPE)
            or self._meta["old"]["backend"]
        )
        return old_db_col_names

    async def sync_cols(
        self,
        diff_folder,
        batch_size=10000,
        mode=None,
        force=False,
        target_backend=None,
        steps=("mapping", "content", "meta", "post"),
        debug=False,
    ):
        """
        Sync a collection with diff files located in diff_folder. This folder contains a metadata.json file which
        describes the different involved collection: "old" is the collection/index to be synced, "new" is the collecion
        that should be obtained once all diff files are applied (not used, just informative).
        If target_backend (bt.databbuild.backend.create_backend() notation), then it will replace "old" (that is, the one
        being synced)
        """
        if isinstance(steps, tuple):
            steps = list(steps)  # may not be necessary, but previous steps default is a list, so let's be consistent
        elif isinstance(steps, str):
            steps = [steps]
        assert self.old and self.new, "'self.old' and 'self.new' must be set to old/new collections"
        self.target_backend = target_backend
        got_error = False
        cnt = 0
        jobs = []
        self.load_metadata(diff_folder)
        meta = self._meta
        diff_type = self.diff_type
        selfcontained = "selfcontained" in self._meta["diff"]["type"]
        old_db_col_names = self.get_target_backend()
        new_db_col_names = self._meta["new"]["backend"]

        self.setup_log(new_db_col_names)

        diff_mapping_file = self._meta["diff"]["mapping_file"]
        pinfo = self.get_pinfo()
        self.synced_cols = "%s -> %s" % (old_db_col_names, new_db_col_names)
        pinfo["source"] = self.synced_cols
        summary = {}
        if "mapping" in steps and self.target_backend_type == "es":
            if diff_mapping_file:
                # old_db_col_names is actually the index name in that case
                index_name = old_db_col_names[1]
                doc_type = self._meta["build_config"]["doc_type"]
                indexer = create_backend(old_db_col_names).target_esidxer
                pinfo["step"] = "mapping"
                pinfo["description"] = diff_mapping_file

                def update_mapping():
                    diffm = os.path.join(diff_folder, diff_mapping_file["name"])
                    ops = loadobj(diffm)
                    mapping = indexer.get_mapping()
                    # we should have the same doc type declared in the mapping
                    mapping[doc_type]["properties"] = jsonpatch.apply_patch(mapping[doc_type]["properties"], ops)
                    res = indexer.update_mapping(mapping)
                    return res

                self.register_status("syncing", transient=True, init=True, job={"step": "sync-mapping"})
                job = await self.job_manager.defer_to_thread(pinfo, partial(update_mapping))

                def updated(f):
                    try:
                        _ = f.result()
                        self.logger.info("Mapping updated on index '%s'" % index_name)
                        summary["mapping_updated"] = True
                        self.register_status("success", job={"step": "sync-mapping"}, sync=summary)
                    except Exception as e:
                        nonlocal got_error
                        self.logger.error("Failed to update mapping on index '%s': %s" % (index_name, e))
                        self.register_status("failed", job={"err": repr(e)})
                        got_error = e

                job.add_done_callback(updated)
                await job

            if got_error:
                self.logger.error(
                    "Failed to update mapping on index '%s': %s" % (old_db_col_names, got_error), extra={"notify": True}
                )
                raise got_error

        if "content" in steps:
            if selfcontained:
                # selfconained is a worker param, isolate diff format
                diff_type = diff_type.replace("-selfcontained", "").replace("-", "_")
            diff_files = [
                (os.path.join(diff_folder, e["name"]), e.get("worker_args", {})) for e in self._meta["diff"]["files"]
            ]
            total = len(diff_files)
            self.logger.info(
                "Syncing %s to %s using diff files in '%s'" % (old_db_col_names, new_db_col_names, diff_folder)
            )
            pinfo["step"] = "content"
            self.register_status("syncing", transient=True, init=True, job={"step": "sync-content"})
            for diff_file, worker_args in diff_files:
                cnt += 1
                pinfo["description"] = "file %s (%s/%s)" % (diff_file, cnt, total)
                worker = getattr(
                    sys.modules["biothings.hub.databuild.syncer"],
                    "sync_%s_%s_worker" % (self.target_backend_type, diff_type),
                )
                strwargs = worker_args and " using specific worker args %s" % repr(worker_args) or ""
                self.logger.info(
                    "Creating sync worker %s for file %s (%s/%s)%s" % (worker.__name__, diff_file, cnt, total, strwargs)
                )
                # deepcopy to make we don't embed "self" with unpickleable stuff
                meta = copy.deepcopy(self._meta)
                job = await self.job_manager.defer_to_process(
                    pinfo,
                    partial(
                        worker,
                        diff_file,
                        old_db_col_names,
                        new_db_col_names,
                        worker_args.get("batch_size") or batch_size,
                        cnt,
                        force,
                        selfcontained,
                        meta,
                        debug,
                    ),
                )
                jobs.append(job)

            def synced(f):
                try:
                    res = f.result()
                    for d in res:
                        for k in d:
                            summary.setdefault(k, 0)
                            summary[k] += d[k]
                except Exception as e:
                    nonlocal got_error
                    got_error = e
                    self.register_status("failed", job={"err": repr(e)})
                    raise

            tasks = asyncio.gather(*jobs)
            tasks.add_done_callback(synced)
            await tasks
            if got_error:
                self.logger.error(
                    "Failed to sync collection from %s to %s using diff files in '%s': %s"
                    % (old_db_col_names, new_db_col_names, diff_folder, got_error),
                    extra={"notify": True},
                )
                raise got_error
            self.register_status("success", job={"step": "sync-content"}, sync=summary)

        if "meta" in steps and self.target_backend_type == "es":
            # old_db_col_names is actually the index name in that case
            index_name = old_db_col_names[1]
            doc_type = self._meta["build_config"]["doc_type"]
            indexer = create_backend(old_db_col_names).target_esidxer
            new_meta = self._meta["_meta"]
            pinfo["step"] = "metadata"

            def update_metadata():
                res = indexer.update_mapping_meta({"_meta": new_meta})
                return res

            job = await self.job_manager.defer_to_thread(pinfo, partial(update_metadata))

            def updated(f):
                try:
                    res = f.result()
                    self.logger.info("Metadata updated on index '%s': %s", index_name, res)
                    summary["metadata_updated"] = True
                    self.register_status("success", job={"step": "sync-meta"}, sync=summary)
                except Exception as e:
                    nonlocal got_error
                    self.logger.error("Failed to update metadata on index '%s': %s", index_name, e)
                    self.register_status("failed", job={"err": repr(e)})
                    got_error = e

            self.register_status("syncing", transient=True, init=True, job={"step": "sync-meta"})
            job.add_done_callback(updated)
            await job

            if got_error:
                self.logger.error(
                    "Failed to update metadata on index '%s': %s" % (old_db_col_names, got_error),
                    extra={"notify": True},
                )
                raise got_error

        if "post" in steps:
            pinfo["step"] = "post"
            job = await self.job_manager.defer_to_thread(
                pinfo,
                partial(
                    self.post_sync_cols,
                    diff_folder=diff_folder,
                    batch_size=batch_size,
                    mode=mode,
                    force=force,
                    target_backend=target_backend,
                    steps=steps,
                ),
            )

            def posted(f):
                try:
                    res = f.result()
                    self.logger.info("Post-sync process done on index '%s': %s", repr(old_db_col_names), res)
                    summary["post-sync"] = True
                    self.register_status("success", job={"step": "sync-post"}, sync=summary)
                except Exception as e:
                    nonlocal got_error
                    self.logger.error("Failed to run post-sync process on index '%s': %s", repr(old_db_col_names), e)
                    self.register_status("failed", job={"err": repr(e)})
                    got_error = e

            self.register_status("syncing", transient=True, init=True, job={"step": "sync-post"})
            job.add_done_callback(posted)
            await job

            if got_error:
                self.logger.error(
                    "Failed to run post-sync process on index '%s': %s",
                    repr(old_db_col_names),
                    got_error,
                    extra={"notify": True},
                )
                raise got_error

        self.logger.info(
            "Succesfully synced index %s to reach collection %s using diff files in '%s': %s",
            old_db_col_names,
            new_db_col_names,
            diff_folder,
            summary,
            extra={"notify": True},
        )

        return summary

    def post_sync_cols(self, diff_folder, batch_size, mode, force, target_backend, steps):
        """Post-sync hook, can be implemented in sub-class"""
        return

    def sync(
        self,
        diff_folder=None,
        batch_size=10000,
        mode=None,
        target_backend=None,
        steps=("mapping", "content", "meta", "post"),
        debug=False,
    ):
        """wrapper over sync_cols() coroutine, return a task"""
        job = asyncio.ensure_future(
            self.sync_cols(
                diff_folder=diff_folder,
                batch_size=batch_size,
                mode=mode,
                target_backend=target_backend,
                steps=steps,
                debug=debug,
            )
        )
        return job


class ThrottlerSyncer(BaseSyncer):
    def __init__(self, max_sync_workers, *args, **kwargs):
        super(ThrottlerSyncer, self).__init__(*args, **kwargs)
        self.max_sync_workers = max_sync_workers

    def get_predicates(self):
        preds = super(ThrottlerSyncer, self).get_predicates()
        if preds is None:
            preds = []

        def not_too_much_syncers(job_manager):
            """
            Limit number of syncers accordingly (this is useful when live-updating
            the prod,we usually need to reduce the number of sync workers as they
            would kill the ES server otherwise... (or at least produces timeout errors)
            """
            return (
                len([j for j in job_manager.jobs.values() if j["category"] == SYNCER_CATEGORY]) < self.max_sync_workers
            )

        preds.append(not_too_much_syncers)
        return preds


class MongoJsonDiffSyncer(BaseSyncer):
    diff_type = "jsondiff"
    target_backend_type = "mongo"


class MongoJsonDiffSelfContainedSyncer(BaseSyncer):
    diff_type = "jsondiff-selfcontained"
    target_backend_type = "mongo"


class ESJsonDiffSyncer(BaseSyncer):
    diff_type = "jsondiff"
    target_backend_type = "es"


class ESJsonDiffSelfContainedSyncer(BaseSyncer):
    diff_type = "jsondiff-selfcontained"
    target_backend_type = "es"


class ESColdHotJsonDiffSyncer(BaseSyncer):
    diff_type = "coldhot-jsondiff"
    target_backend_type = "es"


class ESColdHotJsonDiffSelfContainedSyncer(BaseSyncer):
    diff_type = "coldhot-jsondiff-selfcontained"
    target_backend_type = "es"


class ThrottledESJsonDiffSyncer(ThrottlerSyncer, ESJsonDiffSyncer):
    pass


class ThrottledESJsonDiffSelfContainedSyncer(ThrottlerSyncer, ESJsonDiffSelfContainedSyncer):
    pass


class ThrottledESColdHotJsonDiffSyncer(ThrottlerSyncer, ESColdHotJsonDiffSyncer):
    pass


class ThrottledESColdHotJsonDiffSelfContainedSyncer(ThrottlerSyncer, ESColdHotJsonDiffSelfContainedSyncer):
    pass


# TODO: refactor workers (see sync_es_...)
def sync_mongo_jsondiff_worker(
    diff_file,
    old_db_col_names,
    new_db_col_names,
    batch_size,
    cnt,
    force=False,
    selfcontained=False,
    metadata=None,
    debug=False,
):
    """Worker to sync data between a new and an old mongo collection"""
    metadata = metadata or {}
    # check if diff files was already synced
    res = {"added": 0, "updated": 0, "deleted": 0, "skipped": 0}
    synced_file = "%s.synced" % diff_file
    if os.path.exists(synced_file):
        logging.info("Diff file '%s' already synced, skip it", os.path.basename(diff_file))
        diff = loadobj(synced_file)
        res["skipped"] += len(diff["add"]) + len(diff["delete"]) + len(diff["update"])
        return res
    new = create_backend(new_db_col_names)
    old = create_backend(old_db_col_names)
    storage = UpsertStorage(get_target_db(), old.target_collection.name, logging)
    diff = loadobj(diff_file)
    assert new.target_collection.name == diff["source"], "Source is different in diff file '%s': %s" % (
        diff_file,
        diff["source"],
    )

    # add: get ids from "new"
    if selfcontained:
        # diff["add"] contains all documents, not mongo needed
        for docs in iter_n(diff["add"], batch_size):
            res["added"] += storage.process((d for d in docs), batch_size)
    else:
        cur = doc_feeder(new.target_collection, step=batch_size, inbatch=False, query={"_id": {"$in": diff["add"]}})
        for docs in iter_n(cur, batch_size):
            # use generator otherwise process/doc_iterator will require a dict (that's bad...)
            res["added"] += storage.process((d for d in docs), batch_size)

    # update: get doc from "old" and apply diff
    batch = []
    for patch_info in diff["update"]:
        doc = old.get_from_id(patch_info["_id"])
        try:
            doc = jsonpatch.apply_patch(doc, patch_info["patch"])
            batch.append(doc)
        except jsonpatch.JsonPatchConflict:
            # assuming already applieda
            res["skipped"] += 1
            continue
        if len(batch) >= batch_size:
            res["updated"] += storage.process((d for d in batch), batch_size)
            batch = []
    if batch:
        res["updated"] += storage.process((d for d in batch), batch_size)

    # delete: remove from "old"
    for ids in iter_n(diff["delete"], batch_size):
        res["deleted"] += old.remove_from_ids(ids)

    # we potentially modified the "old" collection so invalidate cache just to make sure
    invalidate_cache(old.target_collection.name, "target")
    logging.info("Done applying diff from file '%s': %s" % (diff_file, res))
    # mark as synced
    os.rename(diff_file, synced_file)
    return res


def sync_es_jsondiff_worker(
    diff_file,
    es_config,
    new_db_col_names,
    batch_size,
    cnt,
    force=False,
    selfcontained=False,
    metadata=None,
    debug=False,
):
    """Worker to sync data between a new mongo collection and an elasticsearch index"""
    metadata = metadata or {}
    res = {"added": 0, "updated": 0, "deleted": 0, "skipped": 0}
    # check if diff files was already synced
    synced_file = "%s.synced" % diff_file
    if os.path.exists(synced_file):
        logging.info("Diff file '%s' already synced, skip it", os.path.basename(diff_file))
        diff = loadobj(synced_file)
        res["skipped"] += len(diff["add"]) + len(diff["delete"]) + len(diff["update"])
        return res
    eskwargs = {}
    # pass optional ES Indexer args
    if hasattr(btconfig, "ES_TIMEOUT"):
        eskwargs["request_timeout"] = btconfig.ES_TIMEOUT
    if hasattr(btconfig, "ES_MAX_RETRY"):
        eskwargs["max_retries"] = btconfig.ES_MAX_RETRY
    if hasattr(btconfig, "ES_RETRY"):
        eskwargs["retry_on_timeout"] = btconfig.ES_RETRY
    logging.debug("Create ES backend with args: (%s,%s)", es_config, eskwargs)
    bckend = create_backend(es_config, **eskwargs)
    indexer = bckend.target_esidxer
    diff = loadobj(diff_file)
    errors = []
    # add: get ids from "new"
    if selfcontained:
        # diff["add"] contains all documents, no mongo needed
        cur = diff["add"]
    else:
        new = create_backend(new_db_col_names)  # mongo collection to sync from
        assert new.target_collection.name == diff["source"], "Source is different in diff file '%s': %s" % (
            diff_file,
            diff["source"],
        )
        cur = doc_feeder(
            new.target_collection,
            step=batch_size,
            inbatch=False,
            query={"_id": {"$in": diff["add"]}},
        )
    for docs in iter_n(cur, batch_size):
        # remove potenial existing _timestamp from document
        # (not allowed within an ES document (_source))
        [d.pop("_timestamp", None) for d in docs]
        try:
            res["added"] += indexer.index_bulk(docs, batch_size, action="create")[0]
        except BulkIndexError:
            for doc in docs:
                _id = doc.pop("_id")
                try:
                    # force action=create to spot docs already added
                    indexer.index(doc, _id, action="create")
                    res["added"] += 1
                except ConflictError:
                    # already added
                    logging.warning("_id '%s' already added" % _id)
                    res["skipped"] += 1
                    continue
                except Exception as e:
                    errors.append({"_id": _id, "file": diff_file, "error": e})
                    import pickle

                    pickle.dump(errors, open("errors", "wb"))
                    raise
        except Exception as e:
            if debug:
                logging.error(
                    "From diff file '%s', following IDs couldn't be synced because: %s\n%s",
                    diff_file,
                    e,
                    [d.get("_id") for d in docs],
                )
                pickfile = "batch_%s_%s.pickle" % (cnt, os.path.basename(diff_file))
                logging.error("Documents pickled in '%s'" % pickfile)
                pickle.dump(docs, open(pickfile, "wb"))
            raise

    # update: get doc from indexer and apply diff
    sync_es_for_update(diff_file, indexer, diff["update"], batch_size, res, debug)

    # delete: remove from "old"
    for ids in iter_n(diff["delete"], batch_size):
        # FIXME: bulk delete can fail
        del_skip = indexer.delete_docs(ids)
        res["deleted"] += del_skip[0]
        res["skipped"] += del_skip[1]

    logging.info("Done applying diff from file '%s': %s" % (diff_file, res))
    # mark as synced
    os.rename(diff_file, synced_file)
    return res


def sync_es_coldhot_jsondiff_worker(
    diff_file,
    es_config,
    new_db_col_names,
    batch_size,
    cnt,
    force=False,
    selfcontained=False,
    metadata=None,
    debug=False,
):
    metadata = metadata or {}
    res = {"added": 0, "updated": 0, "deleted": 0, "skipped": 0}
    # check if diff files was already synced
    synced_file = "%s.synced" % diff_file
    if os.path.exists(synced_file):
        logging.info("Diff file '%s' already synced, skip it", os.path.basename(diff_file))
        diff = loadobj(synced_file)
        res["skipped"] += len(diff["add"]) + len(diff["delete"]) + len(diff["update"])
        return res
    eskwargs = {}
    # pass optional ES Indexer args
    if hasattr(btconfig, "ES_TIMEOUT"):
        eskwargs["request_timeout"] = btconfig.ES_TIMEOUT
    if hasattr(btconfig, "ES_MAX_RETRY"):
        eskwargs["max_retries"] = btconfig.ES_MAX_RETRY
    if hasattr(btconfig, "ES_RETRY"):
        eskwargs["retry_on_timeout"] = btconfig.ES_RETRY
    logging.debug("Create ES backend with args: (%s,%s)", es_config, eskwargs)
    bckend = create_backend(es_config, **eskwargs)
    indexer = bckend.target_esidxer
    diff = loadobj(diff_file)

    # add: diff between hot collections showed we have new documents but it's
    # possible some of those docs already exist in premerge/cold collection.
    # if so, they should be treated as dict.update() where the hot document content
    # has precedence over the cold content for fields in common
    if selfcontained:
        # diff["add"] contains all documents, no mongo needed
        cur = diff["add"]
    else:
        new = create_backend(new_db_col_names)  # mongo collection to sync from
        assert new.target_collection.name == diff["source"], "Source is different in diff file '%s': %s" % (
            diff_file,
            diff["source"],
        )
        cur = doc_feeder(
            new.target_collection,
            step=batch_size,
            inbatch=False,
            query={"_id": {"$in": diff["add"]}},
        )
    for docs in iter_n(cur, batch_size):
        # remove potenial existing _timestamp from document
        # (not allowed within an ES document (_source))
        [d.pop("_timestamp", None) for d in docs]
        # check which docs already exist in existing index (meaning they exist in cold collection)
        dids = dict([(d["_id"], d) for d in docs])
        dexistings = dict([(d["_id"], d) for d in indexer.get_docs([k for k in dids.keys()])])
        logging.debug("From current batch, %d already exist" % len(dexistings))
        # remove existing docs from "add" so the rest of the dict will be treated
        # as "real" added documents while update existing ones with new content
        toremove = []
        for _id, d in dexistings.items():
            # update in-place
            if d == dids[d["_id"]]:
                logging.debug("%s was already added, skip it" % d["_id"])
                toremove.append(d["_id"])
                res["skipped"] += 1
            else:
                newd = copy.deepcopy(d)
                d.update(dids[d["_id"]])
                if d == newd:
                    logging.debug("%s was already updated, skip it" % d["_id"])
                    toremove.append(d["_id"])
                    res["skipped"] += 1
            dids.pop(d["_id"])
        for _id in toremove:
            dexistings.pop(_id)
        logging.info(
            "Syncing 'add' documents (%s in total) from cold/hot merge: " % len(docs)
            + "%d documents will be updated as they already exist in the index, " % len(dexistings)
            + "%d documents will be added (%d skipped as already processed)" % (len(dids), len(toremove))
        )
        # treat real "added" documents
        # Note: no need to check for "already exists" errors, as we already checked that before
        # in order to know what to do
        try:
            res["added"] += indexer.index_bulk(dids.values(), batch_size, action="create")[0]
        except BulkIndexError:
            logging.error("Error while adding documents %s" % [k for k in dids.keys()])
        # update already existing docs in cold collection
        # treat real "added" documents
        try:
            res["updated"] += indexer.index_bulk(dexistings.values(), batch_size)[0]
        except BulkIndexError as e:
            logging.error("Error while updating (via new hot detected docs) documents: %s" % e)

    # update: get doc from indexer and apply diff
    # note: it's the same process as for non-coldhot
    sync_es_for_update(diff_file, indexer, diff["update"], batch_size, res, debug)

    # delete: remove from "old"
    for ids in iter_n(diff["delete"], batch_size):
        del_skip = indexer.delete_docs(ids)
        res["deleted"] += del_skip[0]
        res["skipped"] += del_skip[1]

    logging.info("Done applying diff from file '%s': %s" % (diff_file, res))
    # mark as synced
    os.rename(diff_file, synced_file)
    return res


def sync_es_for_update(diff_file, indexer, diffupdates, batch_size, res, debug):
    batch = []
    ids = [p["_id"] for p in diffupdates]
    iterids_bcnt = iter_n(ids, batch_size, True)
    for batchids, bcnt in iterids_bcnt:
        try:
            for i, doc in enumerate(indexer.get_docs(batchids)):
                # recompute correct index in diff["update"], since we split it in batches
                diffidx = i + bcnt - len(batchids)  # len(batchids) is not == batch_size for the last one...
                try:
                    patch_info = diffupdates[diffidx]  # same order as what's return by get_doc()...
                    assert patch_info["_id"] == doc["_id"], "%s != %s" % (
                        patch_info["_id"],
                        doc["_id"],
                    )  # ... but just make sure
                    newdoc = jsonpatch.apply_patch(doc, patch_info["patch"])
                    if newdoc == doc:
                        # already applied
                        logging.warning("_id '%s' already synced" % doc["_id"])
                        res["skipped"] += 1
                        continue
                    batch.append(newdoc)
                except jsonpatch.JsonPatchConflict as e:
                    # assuming already applied
                    logging.warning("_id '%s' already synced ? JsonPatchError: %s", doc["_id"], e)
                    res["skipped"] += 1
                    continue
                if len(batch) >= batch_size:
                    res["updated"] += indexer.index_bulk(batch, batch_size)[0]
                    batch = []
            if batch:
                res["updated"] += indexer.index_bulk(batch, batch_size)[0]
        except Exception as e:
            if debug:
                logging.error(
                    "From diff file '%s', %d IDs couldn't be synced because: %s\n%s", diff_file, e, len(batchids)
                )
                pickfile = "batch_sync_updater_%s_%s.pickle" % (bcnt, os.path.basename(diff_file))
                logging.error("IDs pickled in '%s'" % pickfile)
                pickle.dump(batchids, open(pickfile, "wb"))
            raise


class SyncerManager(BaseManager):
    def __init__(self, *args, **kwargs):
        """
        SyncerManager deals with the different syncer objects used to synchronize
        different collections or indices using diff files
        """
        super(SyncerManager, self).__init__(*args, **kwargs)
        self.setup_log()

    def clean_stale_status(self):
        src_build = get_src_build()
        for build in src_build.find():
            dirty = False
            for job in build.get("jobs", []):
                if job.get("status") == "syncing":
                    logging.warning("Found stale build '%s', marking sync status as 'canceled'", build["_id"])
                    job["status"] = "canceled"
                    dirty = True
            if dirty:
                src_build.replace_one({"_id": build["_id"]}, build)

    def register_syncer(self, klass):
        if isinstance(klass, partial):
            assert isinstance(klass.func, type), "%s is not a class" % klass.func
            diff_type, target_backend_type = klass.func.diff_type, klass.func.target_backend_type
        else:
            diff_type, target_backend_type = klass.diff_type, klass.target_backend_type

        self.register[(diff_type, target_backend_type)] = partial(
            klass, log_folder=btconfig.LOG_FOLDER, job_manager=self.job_manager
        )

    def configure(self, klasses=None):
        """
        Register default syncers (if klasses is None) or given klasses.
        klasses is a list of class, or a list of partial'ly initialized classes.
        """
        klasses = klasses or [
            MongoJsonDiffSyncer,
            ESJsonDiffSyncer,
            MongoJsonDiffSelfContainedSyncer,
            ESJsonDiffSelfContainedSyncer,
        ]
        for klass in klasses:
            self.register_syncer(klass)

    def setup_log(self):
        self.logger, self.logfile = get_logger("syncmanager")

    def __getitem__(self, diff_target):
        """
        Return an instance of a builder for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self, diff_target)
        return pclass()

    def sync(
        self,
        backend_type,
        old_db_col_names,
        new_db_col_names,
        diff_folder=None,
        batch_size=10000,
        mode=None,
        target_backend=None,
        steps=("mapping", "content", "meta", "post"),
        debug=False,
    ):
        if isinstance(steps, tuple):
            steps = list(steps)  # may not be necessary, but previous steps default is a list, so let's be consistent
        elif isinstance(steps, str):
            steps = [steps]
        if hasattr(btconfig, "SYNC_BATCH_SIZE"):
            batch_size = btconfig.SYNC_BATCH_SIZE
            self.logger.debug("Overriding sync batch_size default to %s" % batch_size)
        if diff_folder is None:
            diff_folder = generate_folder(btconfig.DIFF_PATH, old_db_col_names, new_db_col_names)
        if not os.path.exists(diff_folder):
            raise FileNotFoundError(f"Directory '{diff_folder}' does not exist, run a diff first")
        # load metadata to know collections that have been diffed in diff_func protocol
        try:
            meta = json.load(open(os.path.join(diff_folder, "metadata.json")))
        except FileNotFoundError:
            self.logger.error("Can't find metadata file in diff folder '%s'", diff_folder)
            raise

        self.logger.info("Found metadata information: %s" % pformat(meta))
        try:
            diff_type = meta["diff"]["type"]
        except KeyError:
            msg = "Can't find diff_type in metadata file located in '%s'" % diff_folder
            raise SyncerException(msg)

        try:
            syncer = self[(diff_type, backend_type)]
            self.logger.info("Selected syncer: %s" % syncer)
            syncer.old = create_backend(old_db_col_names)
            syncer.new = create_backend(new_db_col_names)
            job = syncer.sync(
                diff_folder,
                batch_size=batch_size,
                mode=mode,
                target_backend=target_backend,
                steps=steps,
                debug=debug,
            )
            return job
        except KeyError as e:
            raise SyncerException("No such syncer (%s,%s) (error: %s)" % (diff_type, backend_type, e))
