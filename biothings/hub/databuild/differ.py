import os
import shutil
import time
import json
from datetime import datetime
import asyncio
from functools import partial
import glob
import random

from biothings.utils.common import timesofar, get_timestamp, \
    dump, rmdashfr, loadobj, md5sum
from biothings.utils.mongo import id_feeder, get_target_db, get_previous_collection
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from biothings.utils.diff import diff_docs_jsonpatch
from biothings.hub.databuild.backend import generate_folder
from biothings import config as btconfig
from biothings.utils.manager import BaseManager
from .backend import create_backend, merge_src_build_metadata
from biothings.utils.backend import DocMongoBackend
from biothings.utils.jsondiff import make as jsondiff
from biothings.hub import DIFFER_CATEGORY, DIFFMANAGER_CATEGORY
from biothings.hub.datarelease import set_pending_to_release_note

logging = btconfig.logger


class DifferException(Exception):
    pass


class BaseDiffer(object):

    # diff type name, identifying the diff algorithm
    # must be set in sub-class
    diff_type = None

    def __init__(self, diff_func, job_manager, log_folder):
        self.old = None
        self.new = None
        self.log_folder = log_folder
        self.job_manager = job_manager
        self.diff_func = diff_func
        self.timestamp = datetime.now()
        self.logfile = None
        self.setup_log()
        self.ti = time.time()
        self.metadata = {}  # diff metadata
        self.metadata_filename = None

    def setup_log(self):
        self.logger, self.logfile = get_logger(
            'diff_%s' % self.__class__.diff_type, self.log_folder)

    def get_predicates(self):
        return []

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": DIFFER_CATEGORY,
            "source": "",
            "step": "",
            "description": ""
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def register_status(self, status, transient=False, init=False, **extra):
        src_build = get_src_build()
        job_info = {
            'status': status,
            'step_started_at': datetime.now().astimezone(),
            'logfile': self.logfile,
        }
        diff_key = self.old.target_name
        diff_info = {"diff": {diff_key: {}}}
        if transient:
            # record some "in-progress" information
            job_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            job_info["time"] = timesofar(self.ti)
            t1 = round(time.time() - self.ti, 0)
            job_info["time_in_s"] = t1
            diff_info["diff"][diff_key]["created_at"] = datetime.now().astimezone()
        if "diff" in extra:
            diff_info["diff"][diff_key].update(extra["diff"])
        if "job" in extra:
            job_info.update(extra["job"])
        # since the base is the merged collection, we register info there
        # as the new collection (diff results are associated to the most recent colleciton)
        build = src_build.find_one({'_id': self.new.target_name})
        if not build:
            self.logger.info(
                "Can't find build document '%s', no status to register" %
                self.new.target_name)
            return
        if init:
            # init timer for this step
            self.ti = time.time()
            src_build.update({'_id': self.new.target_name},
                             {"$push": {
                                 'jobs': job_info
                             }})
            # now refresh/sync
            build = src_build.find_one({'_id': self.new.target_name})
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
                        if type(v) == dict:
                            # only merge if both are dict (otherwise replace/set with d)
                            if k in target and type(target[k]) == dict:
                                target[k] = merge_info(target[k], v)
                            else:
                                v.pop("__REPLACE__", None)
                                # merge v with "nothing" just to make sure to remove any "__REPLACE__"
                                v = merge_info({}, v)
                                target[k] = v
                        else:
                            target[k] = v
                return target

            build = merge_info(build, diff_info)
            src_build.replace_one({"_id": build["_id"]}, build)

    async def diff_cols(self,
                  old_db_col_names,
                  new_db_col_names,
                  batch_size,
                  steps,
                  mode=None,
                  exclude=[]):
        """
        Compare new with old collections and produce diff files. Root keys can be excluded from
        comparison with "exclude" parameter

        `*_db_col_names` can be:
          1. a colleciton name (as a string) asusming they are
             in the target database.
          2. tuple with 2 elements, the first one is then either "source" or "target"
             to respectively specify src or target database, and the second element is
             the collection name.
          3. tuple with 3 elements (URI,db,collection), looking like:
             ("mongodb://user:pass@host","dbname","collection"), allowing to specify
             any connection on any server

        steps: - 'content' will perform diff on actual content.
               - 'mapping' will perform diff on ES mappings (if target collection involved)
               - 'reduce' will merge diff files, trying to avoid having many small files
               - 'post' is a hook to do stuff once everything is merged (override method post_diff_cols)

        mode: 'purge' will remove any existing files for this comparison while 'resume' will happily ignore
              existing data and to whatever it's requested (like running steps="post" on existing diff folder...)
        """
        # these ones are used to point to the build doc, not the underlying backned
        # (ie. if link builder has been used, it refers a colleciton in src_db, but
        # we need the metadata from the build doc too)
        self.new = create_backend(new_db_col_names, follow_ref=False)
        self.old = create_backend(old_db_col_names, follow_ref=False)
        # these point to the actual collection containing data
        content_new = create_backend(new_db_col_names, follow_ref=True)
        content_old = create_backend(old_db_col_names, follow_ref=True)
        # check what to do
        if type(steps) == str:
            steps = [steps]

        diff_folder = generate_folder(btconfig.DIFF_PATH, old_db_col_names,
                                      new_db_col_names)

        if mode != "force" and os.path.exists(
                diff_folder) and "content" in steps:
            if mode == "purge" and os.path.exists(diff_folder):
                rmdashfr(diff_folder)
            elif mode != "resume":
                raise FileExistsError(
                    "Found existing files in '%s', use mode='purge'" %
                    diff_folder)
        if not os.path.exists(diff_folder):
            os.makedirs(diff_folder)

        # create metadata file storing info about how we created the diff
        # and some summary data
        diff_stats = {
            "update": 0,
            "add": 0,
            "delete": 0,
            "mapping_changed": False
        }
        self.metadata_filename = os.path.join(diff_folder, "metadata.json")
        if os.path.exists(self.metadata_filename):
            # load previous metadata in case we try to update/complete diff
            self.metadata = json.load(open(self.metadata_filename))
        else:
            assert self.old.version, "Version for 'old' collection not defined"
            self.metadata = {
                "diff": {
                    "type": self.diff_type,
                    "func": self.diff_func.__name__,
                    "version": "%s.%s" % (self.old.version, self.new.version),
                    "stats": diff_stats,  # ref to diff_stats
                    "files": [],
                    # when "new" is a target collection:
                    "mapping_file": None,
                    "info": {
                        "generated_on": str(datetime.now().astimezone()),
                        "exclude": exclude,
                        "steps": steps,
                        "mode": mode,
                        "batch_size": batch_size
                    }
                },
                "old": {
                    "backend": old_db_col_names,
                    "version": self.old.version
                },
                "new": {
                    "backend": new_db_col_names,
                    "version": self.new.version
                },
                # when "new" is a mongodb target collection:
                "_meta": {},
                "build_config": {},
            }
        if isinstance(
                self.new, DocMongoBackend
        ) and self.new.target_collection.database.name == btconfig.DATA_TARGET_DATABASE:
            new_doc = get_src_build().find_one(
                {"_id": self.new.target_collection.name})
            if not new_doc:
                raise DifferException("Collection '%s' has no corresponding build document" %
                                      self.new.target_collection.name)
            self.metadata["_meta"] = self.get_metadata()
            self.metadata["build_config"] = new_doc.get("build_config")

        got_error = False
        if "mapping" in steps:

            def diff_mapping(old, new, diff_folder):
                summary = {}
                old_build = get_src_build().find_one(
                    {"_id": old.target_collection.name})
                new_build = get_src_build().find_one(
                    {"_id": new.target_collection.name})
                if old_build and new_build:
                    # mapping diff always in jsondiff
                    mapping_diff = jsondiff(old_build["mapping"],
                                            new_build["mapping"])
                    if mapping_diff:
                        file_name = os.path.join(diff_folder, "mapping.pyobj")
                        dump(mapping_diff, file_name)
                        md5 = md5sum(file_name)
                        summary["mapping_file"] = {
                            "name": os.path.basename(file_name),
                            "md5sum": md5,
                            "size": os.stat(file_name).st_size
                        }
                else:
                    self.logger.info("Neither '%s' nor '%s' have mappings associated to them, skip" %
                                     (old.target_collection.name, new.target_collection.name))
                return summary

            def mapping_diffed(f):
                res = f.result()
                self.register_status("success", job={"step": "diff-mapping"})
                if res.get("mapping_file"):
                    nonlocal got_error
                    # check mapping differences: only "add" ops are allowed, as any others actions would be
                    # ingored by ES once applied (you can't update/delete elements of an existing mapping)
                    mf = os.path.join(diff_folder, res["mapping_file"]["name"])
                    ops = loadobj(mf)
                    for op in ops:
                        if op["op"] != "add":
                            err = DifferException("Found diff operation '%s' in mapping file, " % op["op"]
                                                  + " only 'add' operations are allowed. You can still produce the "
                                                  + "diff by removing 'mapping' from 'steps' arguments. "
                                                  + "Ex: steps=['content']. Diff operation was: %s" % op)
                            got_error = err
                    self.metadata["diff"]["mapping_file"] = res["mapping_file"]
                    diff_stats["mapping_changed"] = True
                    json.dump(self.metadata,
                              open(self.metadata_filename, "w"),
                              indent=True)

                self.logger.info(
                    "Diff file containing mapping differences generated: %s" %
                    res.get("mapping_file"))

            pinfo = self.get_pinfo()
            pinfo["source"] = "%s vs %s" % (self.new.target_name,
                                            self.old.target_name)
            pinfo["step"] = "mapping: old vs new"
            self.register_status("diffing",
                                 transient=True,
                                 init=True,
                                 job={"step": "diff-mapping"})
            job = await self.job_manager.defer_to_thread(
                pinfo, partial(diff_mapping, self.old, self.new, diff_folder))
            job.add_done_callback(mapping_diffed)
            await job
            if got_error:
                raise got_error

        if content_old == content_new:
            self.logger.info(
                "Old and new collections are the same, skipping 'content' step"
            )
        elif "content" in steps:
            cnt = 0
            jobs = []
            pinfo = self.get_pinfo()
            pinfo["source"] = "%s vs %s" % (content_new.target_name,
                                            content_old.target_name)
            pinfo["step"] = "content: new vs old"
            data_new = id_feeder(content_new, batch_size=batch_size)
            selfcontained = "selfcontained" in self.diff_type
            self.register_status("diffing",
                                 transient=True,
                                 init=True,
                                 job={"step": "diff-content"})
            for id_list_new in data_new:
                cnt += 1
                pinfo["description"] = "batch #%s" % cnt

                def diffed(f):
                    res = f.result()
                    diff_stats["update"] += res["update"]
                    diff_stats["add"] += res["add"]
                    if res.get("diff_file"):
                        self.metadata["diff"]["files"].append(res["diff_file"])
                    self.logger.info("(Updated: {}, Added: {})".format(
                        res["update"], res["add"]))
                    self.register_status("success",
                                         job={"step": "diff-content"})

                self.logger.info("Creating diff worker for batch #%s" % cnt)
                job = await self.job_manager.defer_to_process(
                    pinfo,
                    partial(diff_worker_new_vs_old, id_list_new,
                            old_db_col_names, new_db_col_names, cnt,
                            diff_folder, self.diff_func, exclude,
                            selfcontained))
                job.add_done_callback(diffed)
                jobs.append(job)
            await asyncio.gather(*jobs)
            self.logger.info(
                "Finished calculating diff for the new collection. Total number of docs updated: {}, added: {}"
                .format(diff_stats["update"], diff_stats["add"]))

            data_old = id_feeder(content_old, batch_size=batch_size)
            jobs = []
            pinfo = self.get_pinfo()
            pinfo["source"] = "%s vs %s" % (content_old.target_name,
                                            content_new.target_name)
            pinfo["step"] = "content: old vs new"
            for id_list_old in data_old:
                cnt += 1
                pinfo["description"] = "batch #%s" % cnt

                def diffed(f):
                    res = f.result()
                    diff_stats["delete"] += res["delete"]
                    if res.get("diff_file"):
                        self.metadata["diff"]["files"].append(res["diff_file"])
                    self.logger.info("(Deleted: {})".format(res["delete"]))

                self.logger.info("Creating diff worker for batch #%s" % cnt)
                job = await self.job_manager.defer_to_process(
                    pinfo,
                    partial(diff_worker_old_vs_new, id_list_old,
                            new_db_col_names, cnt, diff_folder))
                job.add_done_callback(diffed)
                jobs.append(job)
            await asyncio.gather(*jobs)
            self.logger.info(
                "Finished calculating diff for the old collection. Total number of docs deleted: {}"
                .format(diff_stats["delete"]))
            json.dump(self.metadata,
                      open(self.metadata_filename, "w"),
                      indent=True)

        self.logger.info(
            "Summary: (Updated: {}, Added: {}, Deleted: {}, Mapping changed: {})"
            .format(diff_stats["update"], diff_stats["add"],
                    diff_stats["delete"], diff_stats["mapping_changed"]))

        if "reduce" in steps:
            async def merge_diff():
                self.logger.info("Reduce/merge diff files")
                max_diff_size = getattr(btconfig, "MAX_DIFF_SIZE",
                                        10 * 1024**2)
                current_size = 0
                cnt = 0
                final_res = []
                tomerge = []
                # .done contains original diff files
                done_folder = os.path.join(diff_folder, ".done")
                try:
                    os.mkdir(done_folder)
                except FileExistsError:
                    pass

                def merged(f, cnt):
                    nonlocal got_error
                    nonlocal final_res
                    try:
                        res = f.result()
                        final_res.extend(res)
                        self.logger.info("Diff file #%s created" % cnt)
                    except Exception as e:
                        got_error = e

                diff_files = [f for f in glob.glob(os.path.join(diff_folder, "*.pyobj"))
                              if not os.path.basename(f).startswith("mapping")]
                self.logger.info("%d diff files to process in total" %
                                 len(diff_files))
                jobs = []
                while diff_files:
                    if len(diff_files) % 100 == 0:
                        self.logger.info("%d diff files to process" %
                                         len(diff_files))
                    if current_size > max_diff_size:
                        job = await self.job_manager.defer_to_process(
                            pinfo,
                            partial(reduce_diffs, tomerge, cnt, diff_folder,
                                    done_folder))
                        job.add_done_callback(partial(merged, cnt=cnt))
                        jobs.append(job)
                        current_size = 0
                        cnt += 1
                        tomerge = []
                    else:
                        diff_file = diff_files.pop()
                        current_size += os.stat(diff_file).st_size
                        tomerge.append(diff_file)

                assert not diff_files

                if tomerge:
                    job = await self.job_manager.defer_to_process(
                        pinfo,
                        partial(reduce_diffs, tomerge, cnt, diff_folder,
                                done_folder))
                    job.add_done_callback(partial(merged, cnt=cnt))
                    jobs.append(job)
                    await job

                await asyncio.gather(*jobs)

                return final_res

            pinfo = self.get_pinfo()
            pinfo["source"] = "diff_folder"
            pinfo["step"] = "reduce"
            #job = await self.job_manager.defer_to_thread(pinfo,merge_diff)
            self.register_status("diffing",
                                 transient=True,
                                 init=True,
                                 job={"step": "diff-reduce"})
            res = await merge_diff()
            self.metadata["diff"]["files"] = res
            json.dump(self.metadata,
                      open(self.metadata_filename, "w"),
                      indent=True)
            if got_error:
                self.logger.exception("Failed to reduce diff files: %s" %
                                      got_error,
                                      extra={"notify": True})
                raise got_error
            self.register_status("success", job={"step": "diff-reduce"})

        if "post" in steps:
            pinfo = self.get_pinfo()
            pinfo["source"] = "diff_folder"
            pinfo["step"] = "post"
            self.register_status("diffing",
                                 transient=True,
                                 init=True,
                                 job={"step": "diff-post"})
            job = await self.job_manager.defer_to_thread(
                pinfo,
                partial(self.post_diff_cols,
                        old_db_col_names,
                        new_db_col_names,
                        batch_size,
                        steps,
                        mode=mode,
                        exclude=exclude))

            def posted(f):
                nonlocal got_error
                try:
                    res = f.result()
                    self.register_status("success",
                                         job={"step": "diff-post"},
                                         diff={"post": res})
                    self.logger.info("Post diff process successfully run: %s" %
                                     res)
                except Exception as e:
                    got_error = e

            job.add_done_callback(posted)
            await job
            json.dump(self.metadata,
                      open(self.metadata_filename, "w"),
                      indent=True)
            if got_error:
                self.logger.exception("Failed to run post diff process: %s" %
                                      got_error,
                                      extra={"notify": True})
                raise got_error

        strargs = "[old=%s,new=%s,steps=%s,diff_stats=%s]" % (
            old_db_col_names, new_db_col_names, steps, diff_stats)
        self.logger.info("success %s" % strargs, extra={"notify": True})

        # remove some metadata key for diff registering (some are already in build doc, it would be duplication)
        self.metadata.pop("_meta", None)
        self.metadata.pop("build_config", None)
        # record diff_folder so it's available for later without re-computing it
        self.metadata["diff_folder"] = diff_folder
        self.register_status("success", diff=self.metadata)
        return diff_stats

    def diff(self,
             old_db_col_names,
             new_db_col_names,
             batch_size=100000,
             steps=["content", "mapping", "reduce", "post"],
             mode=None,
             exclude=[]):
        """wrapper over diff_cols() coroutine, return a task"""
        job = asyncio.ensure_future(
            self.diff_cols(old_db_col_names, new_db_col_names, batch_size,
                           steps, mode, exclude))
        return job

    def get_metadata(self):
        new_doc = get_src_build().find_one(
            {"_id": self.new.target_collection.name})
        if not new_doc:
            raise DifferException("Collection '%s' has no corresponding build document" %
                                  self.new.target_collection.name)
        return new_doc.get("_meta", {})

    def post_diff_cols(self,
                       old_db_col_names,
                       new_db_col_names,
                       batch_size,
                       steps,
                       mode=None,
                       exclude=[]):
        """Post diff process hook. This coroutine will in a dedicated thread"""
        return


class ColdHotDiffer(BaseDiffer):
    async def diff_cols(self, old_db_col_names, new_db_col_names, *args, **kwargs):
        self.new = create_backend(new_db_col_names)
        new_doc = get_src_build().find_one(
            {"_id": self.new.target_collection.name})
        assert "cold_collection" in new_doc.get("build_config", {}), "%s document doesn't have " % self.new.target_collection.name \
                                                                     + "a premerge collection declared. Is it really a hot merges collection ?"
        self.cold = create_backend(new_doc["build_config"]["cold_collection"])
        return super(ColdHotDiffer,
                     self).diff_cols(old_db_col_names, new_db_col_names, *args,
                                     **kwargs)

    def get_metadata(self):
        new_doc = get_src_build().find_one(
            {"_id": self.new.target_collection.name})
        cold_doc = get_src_build().find_one(
            {"_id": self.cold.target_collection.name})
        if not new_doc:
            raise DifferException("Collection '%s' has no corresponding build document" %
                                  self.new.target_collection.name)
        if not cold_doc:
            raise DifferException("Collection '%s' has no corresponding build document" %
                                  self.cold.target_collection.name)
        return merge_src_build_metadata([cold_doc, new_doc])


class ColdHotJsonDifferBase(ColdHotDiffer):
    def post_diff_cols(self,
                       old_db_col_names,
                       new_db_col_names,
                       batch_size,
                       steps,
                       mode=None,
                       exclude=[]):
        """
        Post-process the diff files by adjusting some jsondiff operation. Here's the process.
        For updated documents, some operations might illegal in the context of cold/hot merged collections.
        Case #1: "remove" op in an update
            from a cold/premerge collection, we have that doc:
                coldd = {"_id":1, "A":"123", "B":"456", "C":True}

            from the previous hot merge we have this doc:
                prevd = {"_id":1, "D":"789", "C":True, "E":"abc"}

            At that point, the final document, fully merged and indexed is:
                finald = {"_id":1, "A":"123", "B":"456", "C":True, "D":"789", "E":"abc"}
            We can notice field "C" is common to coldd and prevd.

            from the new hot merge, we have:
                newd = {"_id":1, "E","abc"} # C and D don't exist anymore

            Diffing prevd vs. newd will give jssondiff operations:
                [{'op': 'remove', 'path': '/C'}, {'op': 'remove', 'path': '/D'}]

            The problem here is 'C' is removed while it was already in cold merge, it should stay because it has come
            with some resource involved in the premerge (dependent keys, eg. myvariant, "observed" key comes with certain sources)
            => the jsondiff opetation on "C" must be discarded.

            Note: If operation involved a root key (not '/a/c' for instance) and if that key is found in the premerge, then
               then remove the operation. (note we just consider root keys, if the deletion occurs deeper in the document,
               it's just a legal operation updating innder content)

        For deleted documents, the same kind of logic applies
        Case #2: "delete"
            from a cold/premerge collection, we have that doc:
                coldd = {"_id":1, "A":"123", "B":"456", "C":True}
            from the previous hot merge we have this doc:
                prevd = {"_id":1, "D":"789", "C":True}
            fully merged doc:
                finald = {"_id":1, "A":"123", "B":"456", "C":True, "D":"789"}
            from the new hot merge, we have:
                newd = {} # document doesn't exist anymore

            Diffing prevd vs. newd will mark document with _id == 1 to be deleted
            The problem is we have data for _id=1 on the premerge collection, if we delete the whole document we'd loose too much
            information.
            => the deletion must converted into specific "remove" jsondiff operations, for the root keys found in prevd on not in coldd
               (in that case: [{'op':'remove', 'path':'/D'}], and not "C" as C is in premerge)
        """
        # we should be able to find a cold_collection definition in the src_build doc
        # and it should be the same for both old and new
        old_doc = get_src_build().find_one({"_id": old_db_col_names})
        new_doc = get_src_build().find_one({"_id": new_db_col_names})
        assert "build_config" in old_doc and "cold_collection" in old_doc["build_config"], \
            "No cold collection defined in src_build for %s" % old_db_col_names
        assert "build_config" in new_doc and "cold_collection" in new_doc["build_config"], \
            "No cold collection defined in src_build for %s" % new_db_col_names
        assert old_doc["build_config"]["cold_collection"] == new_doc["build_config"]["cold_collection"], \
            "Cold collections are different in src_build docs %s and %s" % (old_db_col_names, new_db_col_names)
        coldcol = get_target_db()[new_doc["build_config"]["cold_collection"]]
        assert coldcol.count() > 0, "Cold collection is empty..."
        diff_folder = generate_folder(btconfig.DIFF_PATH, old_db_col_names,
                                      new_db_col_names)
        diff_files = glob.glob(os.path.join(diff_folder, "diff_*.pyobj"))
        fixed = 0
        for diff_file in diff_files:
            dirty = False
            self.logger.info("Post-processing diff file %s" % diff_file)
            data = loadobj(diff_file)
            # update/remove case #1
            for updt in data["update"]:
                toremove = []
                for patch in updt["patch"]:
                    pathk = patch["path"].split("/")[
                        1:]  # remove / at the beginning of the path
                    if patch["op"] == "remove" and \
                            len(pathk) == 1:
                        # let's query the premerge
                        coldd = coldcol.find_one({"_id": updt["_id"]})
                        if coldd and pathk[0] in coldd:
                            self.logger.info(
                                "Fixed a root key in cold collection that should be preserved: '%s' (for doc _id '%s')"
                                % (pathk[0], updt["_id"]))
                            toremove.append(patch)
                            fixed += 1
                            dirty = True
                for p in toremove:
                    updt["patch"].remove(p)
            # delete case #2
            toremove = []
            prevcol = get_target_db()[old_doc["target_name"]]
            for delid in data["delete"]:
                coldd = coldcol.find_one({"_id": delid})
                if not coldd:
                    # true deletion is required
                    continue
                else:
                    prevd = prevcol.find_one({"_id": delid})
                    prevs = set(prevd.keys())
                    colds = set(coldd.keys())
                    keys = prevs.difference(
                        colds
                    )  # keys exclusively in prevd that should be removed
                    patches = []
                    for k in keys:
                        patches.append({"op": "remove", "path": "/%s" % k})
                    data["update"].append({"_id": delid, "patch": patches})
                    self.logger.info(
                        "Fixed a delete document by converting to update/remove jsondiff operations for keys: %s (_id: '%s')"
                        % (keys, delid))
                    fixed += 1
                    dirty = True
                    toremove.append(delid)
            for i in toremove:
                data["delete"].remove(i)

            if dirty:
                dump(data, diff_file, compress="lzma")
                name = os.path.basename(diff_file)
                md5 = md5sum(diff_file)
                # find info to adjust md5sum
                found = False
                for i, df in enumerate(self.metadata["diff"]["files"]):
                    if df["name"] == name:
                        found = True
                        break
                assert found, "Couldn't find file information in metadata (with md5 value), try to rebuild_diff_file_list() ?"
                size = os.stat(diff_file).st_size
                self.metadata["diff"]["files"][i] = {
                    "name": name,
                    "md5sum": md5,
                    "size": size
                }
                self.logger.info(self.metadata["diff"]["files"])

        self.logger.info(
            "Post-diff process fixing jsondiff operations done: %s fixed" %
            fixed)
        return {"fixed": fixed}


class JsonDiffer(BaseDiffer):
    diff_type = "jsondiff"

    def __init__(self, diff_func=diff_docs_jsonpatch, *args, **kwargs):
        super(JsonDiffer, self).__init__(diff_func=diff_func, *args, **kwargs)


class SelfContainedJsonDiffer(JsonDiffer):
    diff_type = "jsondiff-selfcontained"


class ColdHotJsonDiffer(ColdHotJsonDifferBase, JsonDiffer):
    diff_type = "coldhot-jsondiff"


class ColdHotSelfContainedJsonDiffer(ColdHotJsonDifferBase,
                                     SelfContainedJsonDiffer):
    diff_type = "coldhot-jsondiff-selfcontained"


def diff_worker_new_vs_old(id_list_new,
                           old_db_col_names,
                           new_db_col_names,
                           batch_num,
                           diff_folder,
                           diff_func,
                           exclude=[],
                           selfcontained=False):
    new = create_backend(new_db_col_names, follow_ref=True)
    old = create_backend(old_db_col_names, follow_ref=True)
    docs_common = old.mget_from_ids(id_list_new)
    ids_common = [_doc['_id'] for _doc in docs_common]
    id_in_new = list(set(id_list_new) - set(ids_common))
    _updates = []
    if len(ids_common) > 0:
        _updates = diff_func(old, new, list(ids_common), exclude_attrs=exclude)
    file_name = os.path.join(diff_folder, "%s.pyobj" % str(batch_num))
    _result = {
        'add': id_in_new,
        'update': _updates,
        'delete': [],
        'source': new.target_name,
        'timestamp': get_timestamp()
    }
    if selfcontained:
        # consume generator as result will be pickled
        _result["add"] = [d for d in new.mget_from_ids(id_in_new)]
    summary = {"add": len(id_in_new), "update": len(_updates), "delete": 0}
    if len(_updates) != 0 or len(id_in_new) != 0:
        dump(_result, file_name)
        # compute md5 so when downloaded, users can check integreity
        md5 = md5sum(file_name)
        summary["diff_file"] = {
            "name": os.path.basename(file_name),
            "md5sum": md5,
            "size": os.stat(file_name).st_size
        }

    return summary


def diff_worker_old_vs_new(id_list_old, new_db_col_names, batch_num,
                           diff_folder):
    new = create_backend(new_db_col_names, follow_ref=True)
    docs_common = new.mget_from_ids(id_list_old)
    ids_common = [_doc['_id'] for _doc in docs_common]
    id_in_old = list(set(id_list_old) - set(ids_common))
    file_name = os.path.join(diff_folder, "%s.pyobj" % str(batch_num))
    _result = {
        'delete': id_in_old,
        'add': [],
        'update': [],
        'source': new.target_name,
        'timestamp': get_timestamp()
    }
    summary = {"add": 0, "update": 0, "delete": len(id_in_old)}
    if len(id_in_old) != 0:
        dump(_result, file_name)
        # compute md5 so when downloaded, users can check integreity
        md5 = md5sum(file_name)
        summary["diff_file"] = {
            "name": os.path.basename(file_name),
            "md5sum": md5,
            "size": os.stat(file_name).st_size
        }

    return summary


def diff_worker_count(id_list, db_col_names, batch_num):
    col = create_backend(db_col_names, follow_ref=True)
    docs = col.mget_from_ids(id_list)
    res = {}
    for doc in docs:
        for k in doc:
            res.setdefault(k, 0)
            res[k] += 1
    return res


class DiffReportRendererBase(object):
    def __init__(self,
                 max_reported_ids=None,
                 max_randomly_picked=None,
                 detailed=False):
        self.max_reported_ids = max_reported_ids or hasattr(btconfig, "MAX_REPORTED_IDS") and \
            btconfig.MAX_REPORTED_IDS or 1000
        self.max_randomly_picked = max_randomly_picked or hasattr(btconfig, "MAX_RANDOMLY_PICKED") and \
            btconfig.MAX_RANDOMLY_PICKED or 10
        self.detailed = detailed

    def save(self, report, filename):
        """
        Save report output (rendered) into filename
        """
        raise NotImplementedError("implement me")


class DiffReportTxt(DiffReportRendererBase):
    def save(self, report, filename="report.txt"):
        try:
            import prettytable
        except ImportError:
            raise ImportError(
                "Please install prettytable to use this rendered")

        def build_id_table(subreport):
            if self.detailed:
                table = prettytable.PrettyTable(["IDs", "Root keys"])
                table.align["IDs"] = "l"
                table.align["Root keys"] = "l"
            else:
                table = prettytable.PrettyTable(["IDs"])
                table.align["IDs"] = "l"
            if subreport["count"] <= self.max_reported_ids:
                ids = subreport["ids"]
            else:
                ids = [
                    random.choice(subreport["ids"])
                    for i in range(self.max_reported_ids)
                ]
            for dat in ids:
                if self.detailed:
                    # list of [_id,[keys]]
                    table.add_row([dat[0], ", ".join(dat[1])])
                else:
                    table.add_row([dat])

            return table

        txt = ""
        title = "Diff report (generated on %s)" % datetime.now()
        txt += title + "\n"
        txt += "".join(["="] * len(title)) + "\n"
        txt += "\n"
        txt += "Metadata\n"
        txt += "--------\n"
        if report.get("metadata", {}):
            txt += "Old collection: %s\n" % repr(report["metadata"].get("old"))
            txt += "New collection: %s\n" % repr(report["metadata"].get("new"))
            txt += "Batch size: %s\n" % report["metadata"]["diff"]["info"].get(
                "batch_size")
            txt += "Steps: %s\n" % report["metadata"]["diff"]["info"].get(
                "steps")
            txt += "Key(s) excluded: %s\n" % report["metadata"]["diff"][
                "info"].get("exclude")
            txt += "Diff generated on: %s\n" % report["metadata"]["diff"][
                "info"].get("generated_on")
        else:
            txt += "No metadata found in report\n"
        txt += "\n"
        txt += "Summary\n"
        txt += "-------\n"
        txt += "Added documents: %s\n" % report["added"]["count"]
        txt += "Deleted documents: %s\n" % report["deleted"]["count"]
        txt += "Updated documents: %s\n" % report["updated"]["count"]
        txt += "\n"
        root_keys = report.get("metadata",
                               {}).get("diff",
                                       {}).get("stats",
                                               {}).get("root_keys", {})
        if root_keys:
            for src in sorted(root_keys):
                txt += "%s: %s\n" % (src, root_keys[src])
        else:
            txt += "No root keys count found in report\n"
        txt += "\n"
        txt += "Added documents (%s randomly picked from report)\n" % self.max_reported_ids
        txt += "------------------------------------------------\n"
        if report["added"]["count"]:
            table = build_id_table(report["added"])
            txt += table.get_string()
            txt += "\n"
        else:
            txt += "No added document found in report\n"
        txt += "\n"
        txt += "Deleted documents (%s randomly picked from report)\n" % self.max_reported_ids
        txt += "--------------------------------------------------\n"
        if report["deleted"]["count"]:
            table = build_id_table(report["deleted"])
            txt += table.get_string()
            txt += "\n"
        else:
            txt += "No deleted document found in report\n"
        txt += "\n"
        txt += "Updated documents (%s examples randomly picked from report)\n" % self.max_randomly_picked
        txt += "-----------------------------------------------------------\n"
        txt += "\n"
        for op in sorted(report["updated"]):
            if op == "count":
                continue  # already displayed
            if report["updated"][op]:
                table = prettytable.PrettyTable([op, "Count", "Examples"])
                table.sortby = "Count"
                table.reversesort = True
                table.align[op] = "l"
                table.align["Count"] = "r"
                table.align["Examples"] = "l"
                for path in report["updated"][op]:
                    info = report["updated"][op][path]
                    row = [path, info["count"]]
                    if info["count"] <= self.max_randomly_picked:
                        row.append(", ".join(info["ids"]))
                    else:
                        row.append(", ".join([
                            random.choice(info["ids"])
                            for i in range(self.max_randomly_picked)
                        ]))
                    table.add_row(row)
                txt += table.get_string()
                txt += "\n"
            else:
                txt += "No content found for diff operation '%s'\n" % op
            txt += "\n"
        txt += "\n"

        with open(os.path.join(report["diff_folder"], filename), "w") as fout:
            fout.write(txt)

        return txt


class DifferManager(BaseManager):
    def __init__(self, poll_schedule=None, *args, **kwargs):
        """
        DifferManager deals with the different differ objects used to create and
        analyze diff between datasources.
        """
        super(DifferManager, self).__init__(*args, **kwargs)
        self.log_folder = btconfig.LOG_FOLDER
        self.timestamp = datetime.now()
        self.poll_schedule = poll_schedule
        self.setup_log()

    def clean_stale_status(self):
        src_build = get_src_build()
        for build in src_build.find():
            dirty = False
            for job in build.get("jobs", []):
                if job.get("status") == "diffing":
                    logging.warning(
                        "Found stale build '%s', marking diff status as 'canceled'"
                        % build["_id"])
                    job["status"] = "canceled"
                    dirty = True
            if dirty:
                src_build.replace_one({"_id": build["_id"]}, build)

    def register_differ(self, klass):
        if klass.diff_type is None:
            raise DifferException("diff_type must be defined in %s" % klass)
        self.register[klass.diff_type] = partial(
            klass,
            log_folder=btconfig.LOG_FOLDER,
            job_manager=self.job_manager)

    def configure(self, partial_differs=[JsonDiffer, SelfContainedJsonDiffer]):
        for pdiffer in partial_differs:
            self.register_differ(pdiffer)

    def setup_log(self):
        self.logger, self.logfile = get_logger('diffmanager')

    def get_predicates(self):
        def no_other_diffmanager_step_running(job_manager):
            """DiffManager deals with diff report, release note, publishing,
            none of them should run more than one at a time"""
            # Note: report output is part a publish_diff, release_note is impacted by diff content,
            # overall we keep things simple and don't allow more than one diff manager job to run
            # at the same time
            return len([
                j for j in job_manager.jobs.values()
                if j["category"] == DIFFMANAGER_CATEGORY
            ]) == 0

        return [no_other_diffmanager_step_running]

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": DIFFMANAGER_CATEGORY,
            "source": "",
            "step": "",
            "description": ""
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def __getitem__(self, diff_type):
        """
        Return an instance of a builder for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self, diff_type)
        return pclass()

    def diff(self,
             diff_type,
             old,
             new,
             batch_size=100000,
             steps=["content", "mapping", "reduce", "post"],
             mode=None,
             exclude=["_timestamp"]):
        """
        Run a diff to compare old vs. new collections. using differ algorithm diff_type. Results are stored in
        a diff folder.
        Steps can be passed to choose what to do:
        - count: will count root keys in new collections and stores them as statistics.
        - content: will diff the content between old and new. Results (diff files) format depends on diff_type
        """
        # Note: _timestamp is excluded by default since it's an internal field (and exists in mongo doc,
        #       but not in ES "_source" document (there's a root timestamp under control of
        #       _timestamp : {enable:true} in mapping
        try:
            differ = self[diff_type]
            old = old or get_previous_collection(new)
            job = differ.diff(old,
                              new,
                              batch_size=batch_size,
                              steps=steps,
                              mode=mode,
                              exclude=exclude)

            def diffed(f):
                try:
                    _ = f.result()
                    # after creating a build diff, indicate
                    # a release note should be auto generated
                    set_pending_to_release_note(new)
                except Exception as e:
                    self.logger.error("Error during diff: %s" % e)
                    raise

            job.add_done_callback(diffed)
            return job
        except KeyError as e:
            raise DifferException("No such differ '%s' (error: %s)" %
                                  (diff_type, e))

    def diff_report(self,
                    old_db_col_names,
                    new_db_col_names,
                    report_filename="report.txt",
                    format="txt",
                    detailed=True,
                    max_reported_ids=None,
                    max_randomly_picked=None,
                    mode=None):

        max_reported_ids = max_reported_ids or hasattr(btconfig, "MAX_REPORTED_IDS") and \
            btconfig.MAX_REPORTED_IDS or 1000
        max_randomly_picked = max_randomly_picked or hasattr(btconfig, "MAX_RANDOMLY_PICKED") and \
            btconfig.MAX_RANDOMLY_PICKED or 10

        def do():
            if mode == "purge" or not os.path.exists(reportfilepath):
                assert format == "txt", "Only 'txt' format supported for now"
                report = self.build_diff_report(diff_folder, detailed,
                                                max_reported_ids)
                render = DiffReportTxt(max_reported_ids=max_reported_ids,
                                       max_randomly_picked=max_randomly_picked,
                                       detailed=detailed)
                return render.save(report, report_filename)
            else:
                self.logger.debug("Report already generated, now using it")
                return open(reportfilepath).read()

        async def main(diff_folder):
            got_error = False
            pinfo = self.get_pinfo()
            pinfo["step"] = "report"
            pinfo["source"] = diff_folder
            pinfo["description"] = report_filename
            job = await self.job_manager.defer_to_thread(pinfo, do)

            def reported(f):
                nonlocal got_error
                try:
                    _ = f.result()
                    self.logger.info("Diff report ready, saved in %s" %
                                     reportfilepath,
                                     extra={
                                         "notify": True,
                                         "attach": reportfilepath
                                     })
                except Exception as e:
                    got_error = e

            job.add_done_callback(reported)
            await job
            if got_error:
                self.logger.exception("Failed to create diff report: %s" %
                                      got_error,
                                      extra={"notify": True})
                raise got_error

        diff_folder = generate_folder(btconfig.DIFF_PATH, old_db_col_names,
                                      new_db_col_names)
        reportfilepath = os.path.join(diff_folder, report_filename)
        job = asyncio.ensure_future(main(diff_folder))
        return job

    def build_diff_report(self,
                          diff_folder,
                          detailed=True,
                          max_reported_ids=None):
        """
        Analyze diff files in diff_folder and give a summy of changes.
        max_reported_ids is the number of IDs contained in the report for each part.
        detailed will trigger a deeper analysis, takes more time.
        """

        max_reported_ids = max_reported_ids or hasattr(btconfig, "MAX_REPORTED_IDS") and \
            btconfig.MAX_REPORTED_IDS or 1000

        update_details = {
            "add": {},  # "count": 0, "data": {} },
            "remove": {},  # "count": 0, "data": {} },
            "replace": {},  # "count": 0, "data": {} },
            "move": {},  # "count": 0, "data": {} },
            "count": 0,
        }
        adds = {"count": 0, "ids": []}
        dels = {"count": 0, "ids": []}
        sources = {}

        if os.path.isabs(diff_folder):
            data_folder = diff_folder
        else:
            data_folder = os.path.join(btconfig.DIFF_PATH, diff_folder)

        metadata = {}
        try:
            metafile = os.path.join(data_folder, "metadata.json")
            metadata = json.load(open(metafile))
        except FileNotFoundError:
            logging.warning("Not metadata found in diff folder")
            if detailed:
                raise Exception(
                    "Can't perform detailed analysis without a metadata file")

        def analyze(diff_file, detailed):
            data = loadobj(diff_file)
            sources[data["source"]] = 1
            if detailed:
                # TODO: if self-contained, no db connection needed
                new_col = create_backend(metadata["new"]["backend"],
                                         follow_ref=True)
                old_col = create_backend(metadata["old"]["backend"],
                                         follow_ref=True)
            if len(adds["ids"]) < max_reported_ids:
                if detailed:
                    # look for which root keys were added in new collection
                    for _id in data["add"]:
                        # selfcontained = dict for whole doc (see TODO above)
                        if type(_id) == dict:
                            _id = _id["_id"]
                        doc = new_col.get_from_id(_id)
                        rkeys = sorted(doc.keys())
                        adds["ids"].append([_id, rkeys])
                else:
                    if data["add"] and type(data["add"][0]) == dict:
                        adds["ids"].extend([d["_id"] for d in data["add"]])
                    else:
                        adds["ids"].extend(data["add"])
            adds["count"] += len(data["add"])
            if len(dels["ids"]) < max_reported_ids:
                if detailed:
                    # look for which root keys were deleted in old collection
                    for _id in data["delete"]:
                        doc = old_col.get_from_id(_id)
                        rkeys = sorted(doc.keys())
                        dels["ids"].append([_id, rkeys])
                else:
                    dels["ids"].extend(data["delete"])
            dels["count"] += len(data["delete"])
            for up in data["update"]:
                for patch in up["patch"]:
                    update_details[patch["op"]].setdefault(
                        patch["path"], {
                            "count": 0,
                            "ids": []
                        })
                    if len(update_details[patch["op"]][patch["path"]]
                           ["ids"]) < max_reported_ids:
                        update_details[patch["op"]][
                            patch["path"]]["ids"].append(up["_id"])
                    update_details[patch["op"]][patch["path"]]["count"] += 1
            update_details["count"] += len(data["update"])

            assert len(
                sources
            ) == 1, "Should have one datasource from diff files, got: %s" % [
                s for s in sources
            ]

        # we randomize files order b/c we randomly pick some examples from those
        # files. If files contains data in order (like chrom 1, then chrom 2)
        # we won't have a representative sample
        files = glob.glob(os.path.join(data_folder, "*.pyobj"))
        random.shuffle(files)
        total = len(files)
        for i, f in enumerate(files):
            if os.path.basename(f).startswith("mapping"):
                logging.debug("Skip mapping file")
                continue
            logging.info("Running report worker for '%s' (%d/%d)" %
                         (f, i + 1, total))
            analyze(f, detailed)
        return {
            "added": adds,
            "deleted": dels,
            "updated": update_details,
            "diff_folder": diff_folder,
            "detailed": detailed,
            "metadata": metadata
        }

    def poll(self, state, func):
        super(DifferManager, self).poll(state, func, col=get_src_build())

    def trigger_diff(self, diff_type, doc, **kwargs):
        """
        Launch a diff given a src_build document. In order to
        know the first collection to diff against, get_previous_collection()
        method is used.
        """
        new_db_col_names = doc["_id"]
        old_db_col_names = get_previous_collection(new_db_col_names)
        self.diff(diff_type, old_db_col_names, new_db_col_names, **kwargs)

    def rebuild_diff_file_list(self, diff_folder):
        diff_files = glob.glob(os.path.join(diff_folder, "*.pyobj"))
        metadata = json.load(open(os.path.join(diff_folder, "metadata.json")))
        try:
            metadata["diff"]["files"] = []
            if "mapping_file" not in metadata["diff"]:
                metadata["diff"]["mapping_file"] = None
            for diff_file in diff_files:
                name = os.path.basename(diff_file)
                md5 = md5sum(diff_file)
                info = {
                    "md5sum": md5,
                    "name": name,
                    "size": os.stat(diff_file).st_size
                }
                if "mapping" in diff_file:  # dirty...
                    metadata["diff"]["mapping"] = info
                else:
                    metadata["diff"]["files"].append(info)
            json.dump(metadata,
                      open(os.path.join(diff_folder, "metadata.json"), "w"),
                      indent=True)
            self.logger.info(
                "Successfully rebuild diff_files list with all files found in %s"
                % diff_folder)
        except KeyError as e:
            self.logging.error(
                "Metadata is too much damaged, can't rebuild diff_files list: %s"
                % e)

    def diff_info(self):
        dtypes = self.register.keys()
        res = {}
        for typ in dtypes:
            res[typ] = {}
        return res


def reduce_diffs(diffs, num, diff_folder, done_folder):
    assert diffs
    res = []
    fn = "diff_%s.pyobj" % num
    logging.info("Merging %s => %s" % ([os.path.basename(f)
                                        for f in diffs], fn))

    if len(diffs) == 1:
        # just rename
        outf = os.path.join(diff_folder, fn)
        shutil.copyfile(diffs[0], outf)
        res.append({
            "name": fn,
            "md5sum": md5sum(outf),
            "size": os.stat(outf).st_size
        })
        os.rename(diffs[0],
                  os.path.join(done_folder, os.path.basename(diffs[0])))
        return res

    merged = loadobj(diffs[0])
    os.rename(diffs[0], os.path.join(done_folder, os.path.basename(diffs[0])))
    for diff_fn in diffs[1:]:
        diff = loadobj(diff_fn)
        assert merged["source"] == diff["source"]
        for k in ["add", "delete", "update"]:
            merged[k].extend(diff[k])
        os.rename(diff_fn, os.path.join(done_folder,
                                        os.path.basename(diff_fn)))
    dump(merged, os.path.join(diff_folder, fn), compress="lzma")
    file_name = os.path.join(diff_folder, fn)
    res.append({
        "name": fn,
        "md5sum": md5sum(file_name),
        "size": os.stat(file_name).st_size
    })
    return res


def set_pending_to_diff(col_name):
    src_build = get_src_build()
    src_build.update({"_id": col_name}, {"$addToSet": {"pending": "diff"}})
