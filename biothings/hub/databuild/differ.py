import os, re
import time, hashlib
import pickle, json
from datetime import datetime
from dateutil.parser import parse as dtparse
from pprint import pformat, pprint
import asyncio
from functools import partial
import glob, random
from pympler.asizeof import asizeof

from biothings.utils.common import timesofar, iter_n, get_timestamp, \
                                   dump, rmdashfr, loadobj, md5sum
from biothings.utils.mongo import id_feeder, get_target_db
from biothings.utils.hub_db import get_src_build, get_source_fullname
from biothings.utils.loggers import get_logger, HipchatHandler
from biothings.utils.diff import diff_docs_jsonpatch
from biothings.hub.databuild.backend import generate_folder
from biothings import config as btconfig
from biothings.utils.manager import BaseManager, ManagerError
from .backend import create_backend
from .syncer import SyncerManager
from biothings.utils.backend import DocMongoBackend
import biothings.utils.aws as aws
from biothings.utils.jsondiff import make as jsondiff
from biothings.utils.hub import publish_data_version
from biothings.utils.dataload import update_dict_recur
from biothings.hub import DIFFER_CATEGORY, DIFFMANAGER_CATEGORY

logging = btconfig.logger


class DifferException(Exception):
    pass


class BaseDiffer(object):

    # diff type name, identifying the diff algorithm
    # must be set in sub-class
    diff_type = None

    def __init__(self, diff_func, job_manager, log_folder):
        self.log_folder = log_folder
        self.job_manager = job_manager
        self.diff_func = diff_func
        self.timestamp = datetime.now()
        self.setup_log()

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, 'diff_%s_%s.log' % (self.__class__.diff_type,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("diff")
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not nh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(nh)
        return self.logger

    def get_predicates(self, running_jobs={}):
        return []

    def get_pinfo(self, job_manager=None):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {"category" : DIFFER_CATEGORY,
                "source" : "",
                "step" : "",
                "description" : ""}
        preds = self.get_predicates(job_manager.jobs)
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    @asyncio.coroutine
    def diff_cols(self,old_db_col_names, new_db_col_names, batch_size, steps, mode=None, exclude=[]):
        """
        Compare new with old collections and produce diff files. Root keys can be excluded from
        comparison with "exclude" parameter
        *_db_col_names can be:
         1. a colleciton name (as a string) asusming they are
            in the target database.
         2. tuple with 2 elements, the first one is then either "source" or "target"
            to respectively specify src or target database, and the second element is
            the collection name.
         3. tuple with 3 elements (URI,db,collection), looking like:
            ("mongodb://user:pass@host","dbname","collection"), allowing to specify
            any connection on any server
        steps: - 'count' will count the root keys for every documents in new collection
                 (to check number of docs from datasources).
               - 'content' will perform diff on actual content.
               - 'mapping' will perform diff on ES mappings (if target collection involved)
               - 'post' will merge diff files, trying to avoid having many small files
        mode: 'purge' will remove any existing files for this comparison.
        """
        new = create_backend(new_db_col_names)
        old = create_backend(old_db_col_names)
        # check what to do
        if type(steps) == str:
            steps = [steps]

        diff_folder = generate_folder(btconfig.DIFF_PATH,old_db_col_names,new_db_col_names)

        if mode != "force" and os.path.exists(diff_folder) and "content" in steps:
            if mode == "purge" and os.path.exists(diff_folder):
                rmdashfr(diff_folder)
            else:
                raise FileExistsError("Found existing files in '%s', use mode='purge'" % diff_folder)
        if not os.path.exists(diff_folder):
            os.makedirs(diff_folder)

        # create metadata file storing info about how we created the diff
        # and some summary data
        diff_stats = {"update":0, "add":0, "delete":0, "mapping_changed": False}
        metadata = {
                "diff" : {
                    "type" : self.diff_type,
                    "func" : self.diff_func.__name__,
                    "version" : "%s.%s" % (old.version,new.version),
                    "stats": diff_stats, # ref to diff_stats
                    "files": [],
                    # when "new" is a target collection:
                    "mapping_file": None,
                    "info" : {
                        "generated_on": str(datetime.now()),
                        "exclude": exclude,
                        "steps": steps,
                        "mode": mode,
                        "batch_size": batch_size
                        }
                    },
                "old": {
                    "backend" : old_db_col_names,
                    "version" : old.version
                    },
                "new": {
                    "backend" : new_db_col_names,
                    "version": new.version
                    },
                # when "new" is a mongodb target collection:
                "_meta" : {},
                "build_config": {},
                }
        if isinstance(new,DocMongoBackend) and new.target_collection.database.name == btconfig.DATA_TARGET_DATABASE:
            new_doc = get_src_build().find_one({"_id":new.target_collection.name})
            if not new_doc:
                raise DifferException("Collection '%s' has no corresponding build document" % \
                        new.target_collection.name)
            metadata["_meta"] = new_doc.get("_meta",{})
            metadata["build_config"] = new_doc.get("build_config")

        # dump it here for minimum information, in case we don't go further
        json.dump(metadata,open(os.path.join(diff_folder,"metadata.json"),"w"),indent=True)

        got_error = False
        if "mapping" in steps:
            def diff_mapping(old,new,diff_folder):
                summary = {}
                old_build = get_src_build().find_one({"_id":old.target_collection.name})
                new_build = get_src_build().find_one({"_id":new.target_collection.name})
                if old_build and new_build:
                    # mapping diff always in jsondiff
                    mapping_diff = jsondiff(old_build["mapping"], new_build["mapping"])
                    if mapping_diff:
                        file_name = os.path.join(diff_folder,"mapping.pyobj")
                        dump(mapping_diff, file_name)
                        md5 = md5sum(file_name)
                        summary["mapping_file"] = {
                                "name" : os.path.basename(file_name),
                                "md5sum" : md5
                                }
                else:
                    self.logger.info("Neither '%s' nor '%s' have mappings associated to them, skip" % \
                            (old.target_collection.name,new.target_collection.name))
                return summary

            def mapping_diffed(f):
                res = f.result()
                if res.get("mapping_file"):
                    nonlocal got_error
                    # check mapping differences: only "add" ops are allowed, as any others actions would be
                    # ingored by ES once applied (you can't update/delete elements of an existing mapping)
                    mf = os.path.join(diff_folder,res["mapping_file"]["name"])
                    ops = loadobj(mf)
                    for op in ops:
                        if op["op"] != "add":
                            err = DifferException("Found diff operation '%s' in mapping file, " % op["op"] + \
                                " only 'add' operations are allowed. You can still produce the " + \
                                "diff by removing 'mapping' from 'steps' arguments. " + \
                                "Ex: steps=['count','content']. Diff operation was: %s" % op)
                            got_error = err
                    metadata["diff"]["mapping_file"] = res["mapping_file"]
                    diff_stats["mapping_changed"] = True

                self.logger.info("Diff file containing mapping differences generated: %s" % res.get("mapping_file"))

            pinfo = self.get_pinfo(self.job_manager)
            pinfo["source"] = "%s vs %s" % (new.target_name,old.target_name)
            pinfo["step"] = "mapping: old vs new"
            job = yield from self.job_manager.defer_to_thread(pinfo,
                    partial(diff_mapping, old, new, diff_folder))
            job.add_done_callback(mapping_diffed)
            yield from job
            if got_error:
                raise got_error

        if "count" in steps:
            cnt = 0
            pinfo = self.get_pinfo(self.job_manager)
            pinfo["source"] = "%s vs %s" % (new.target_name,old.target_name)
            pinfo["step"] = "count"
            self.logger.info("Counting root keys in '%s'"  % new.target_name)
            diff_stats["root_keys"] = {}
            jobs = []
            data_new = id_feeder(new, batch_size=batch_size)
            for id_list in data_new:
                cnt += 1
                pinfo["description"] = "batch #%s" % cnt
                self.logger.info("Creating diff worker for batch #%s" % cnt)
                job = yield from self.job_manager.defer_to_process(pinfo,
                        partial(diff_worker_count, id_list, new_db_col_names, cnt))
                jobs.append(job)
            def counted(f):
                root_keys = {}
                # merge the counts
                for d in f.result():
                    for k in d:
                        root_keys.setdefault(k,0)
                        root_keys[k] +=  d[k]
                self.logger.info("root keys count: %s" % root_keys)
                diff_stats["root_keys"] = root_keys
            tasks = asyncio.gather(*jobs)
            tasks.add_done_callback(counted)
            yield from tasks
            self.logger.info("Finished counting keys in the new collection: %s" % diff_stats["root_keys"])

        if "content" in steps:
            skip = 0
            cnt = 0
            jobs = []
            pinfo = self.get_pinfo(self.job_manager)
            pinfo["source"] = "%s vs %s" % (new.target_name,old.target_name)
            pinfo["step"] = "content: new vs old"
            data_new = id_feeder(new, batch_size=batch_size)
            selfcontained = "selfcontained" in self.diff_type
            for id_list_new in data_new:
                cnt += 1
                pinfo["description"] = "batch #%s" % cnt
                def diffed(f):
                    res = f.result()
                    diff_stats["update"] += res["update"]
                    diff_stats["add"] += res["add"]
                    if res.get("diff_file"):
                        metadata["diff"]["files"].append(res["diff_file"])
                    self.logger.info("(Updated: {}, Added: {})".format(res["update"], res["add"]))
                self.logger.info("Creating diff worker for batch #%s" % cnt)
                job = yield from self.job_manager.defer_to_process(pinfo,
                        partial(diff_worker_new_vs_old, id_list_new, old_db_col_names,
                                new_db_col_names, cnt , diff_folder, self.diff_func, exclude, selfcontained))
                job.add_done_callback(diffed)
                jobs.append(job)
            yield from asyncio.gather(*jobs)
            self.logger.info("Finished calculating diff for the new collection. Total number of docs updated: {}, added: {}".format(diff_stats["update"], diff_stats["add"]))

            data_old = id_feeder(old, batch_size=batch_size)
            jobs = []
            pinfo = self.get_pinfo(self.job_manager)
            pinfo["source"] = "%s vs %s" % (new.target_name,old.target_name)
            pinfo["step"] = "content: old vs new"
            for id_list_old in data_old:
                cnt += 1
                pinfo["description"] = "batch #%s" % cnt
                def diffed(f):
                    res = f.result()
                    diff_stats["delete"] += res["delete"]
                    if res.get("diff_file"):
                        metadata["diff"]["files"].append(res["diff_file"])
                    self.logger.info("(Deleted: {})".format(res["delete"]))
                self.logger.info("Creating diff worker for batch #%s" % cnt)
                job = yield from self.job_manager.defer_to_process(pinfo,
                        partial(diff_worker_old_vs_new, id_list_old, new_db_col_names, cnt , diff_folder))
                job.add_done_callback(diffed)
                jobs.append(job)
            yield from asyncio.gather(*jobs)
            self.logger.info("Finished calculating diff for the old collection. Total number of docs deleted: {}".format(diff_stats["delete"]))

        self.logger.info("Summary: (Updated: {}, Added: {}, Deleted: {}, Mapping changed: {})".format(
            diff_stats["update"], diff_stats["add"], diff_stats["delete"], diff_stats["mapping_changed"]))

        if "post" in steps:
            def merge_diff():
                self.logger.info("Reduce/merge diff files")
                max_diff_size = getattr(btconfig,"MAX_DIFF_SIZE",10*1024**2)
                current_size = 0
                diff_data = None
                cnt = 0
                res = []
                # .done contains original diff files
                done_folder = os.path.join(diff_folder,".done")
                try:
                    os.mkdir(done_folder)
                except FileExistsError:
                    pass
                diff_files = [f for f in glob.glob(os.path.join(diff_folder,"*.pyobj")) \
                        if not os.path.basename(f).startswith("mapping")]
                self.logger.info("%d diff files to process in total" % len(diff_files))
                while diff_files:
                    if len(diff_files) % 100 == 0:
                        self.logger.info("%d diff files to process" % len(diff_files))
                    if current_size > max_diff_size:
                        fn = "diff_%s.pyobj" % cnt
                        dump(diff_data,os.path.join(diff_folder,fn),compress="lzma")
                        res.append({"name":fn,"md5sum":md5sum(os.path.join(diff_folder,fn))})
                        diff_data = None
                        current_size = 0
                        cnt += 1
                    if diff_data is None:
                        diff_file = diff_files.pop()
                        diff_data = loadobj(diff_file)
                        os.rename(diff_file,os.path.join(done_folder,os.path.basename(diff_file)))
                        current_size += asizeof(diff_data)

                    # still something to merge ? It could not be, if diff files are even, 
                    # the last one will be processed just above with the diff_files.pop()
                    # leaving that diff_files empty
                    if diff_files:
                        diff_file = diff_files.pop()
                        tomerge = loadobj(diff_file)
                        os.rename(diff_file,os.path.join(done_folder,os.path.basename(diff_file)))
                        current_size += asizeof(tomerge)
                        assert diff_data["source"] == tomerge["source"]
                        for k in ["add","delete","update"]:
                            diff_data[k].extend(tomerge[k])

                if diff_data:
                    fn = "diff_%s.pyobj" % cnt
                    dump(diff_data,os.path.join(diff_folder,fn),compress="lzma")
                    res.append({"name":fn,"md5sum":md5sum(os.path.join(diff_folder,fn))})

                return res

            pinfo = self.get_pinfo(self.job_manager)
            pinfo["source"] = "diff_folder"
            pinfo["step"] = "reduce"
            job = yield from self.job_manager.defer_to_thread(pinfo,merge_diff)
            def reduced(f):
                nonlocal got_error
                try:
                    res = f.result()
                    metadata["diff"]["files"] = res
                    self.logger.info("Diff files reduced")
                except Exception as e:
                    got_error = e
            job.add_done_callback(reduced)
            yield from job
            if got_error:
                self.logger.exception("Failed to reduce diff files: %s" % got_error,extra={"notify":True})
                raise got_error

        # pickl again with potentially more information (diff_stats)
        json.dump(metadata,open(os.path.join(diff_folder,"metadata.json"),"w"),indent=True)
        strargs = "[old=%s,new=%s,steps=%s,diff_stats=%s]" % (old_db_col_names,new_db_col_names,steps,diff_stats)
        self.logger.info("success %s" % strargs,extra={"notify":True})
        return diff_stats

    def diff(self,old_db_col_names, new_db_col_names, batch_size=100000, steps=["content","mapping","post"], mode=None, exclude=[]):
        """wrapper over diff_cols() coroutine, return a task"""
        job = asyncio.ensure_future(self.diff_cols(old_db_col_names, new_db_col_names, batch_size, steps, mode, exclude))
        return job


class JsonDiffer(BaseDiffer):

    diff_type = "jsondiff"

    def __init__(self, diff_func=diff_docs_jsonpatch, *args, **kwargs):
        super(JsonDiffer,self).__init__(diff_func=diff_func,*args,**kwargs)

class SelfContainedJsonDiffer(JsonDiffer):

    diff_type = "jsondiff-selfcontained"


def diff_worker_new_vs_old(id_list_new, old_db_col_names, new_db_col_names,
                           batch_num, diff_folder, diff_func, exclude=[], selfcontained=False):
    new = create_backend(new_db_col_names)
    old = create_backend(old_db_col_names)
    docs_common = old.mget_from_ids(id_list_new)
    ids_common = [_doc['_id'] for _doc in docs_common]
    id_in_new = list(set(id_list_new) - set(ids_common))
    _updates = []
    if len(ids_common) > 0:
        _updates = diff_func(old, new, list(ids_common), exclude_attrs=exclude)
    file_name = os.path.join(diff_folder,"%s.pyobj" % str(batch_num))
    _result = {'add': id_in_new,
               'update': _updates,
               'delete': [],
               'source': new.target_name,
               'timestamp': get_timestamp()}
    if selfcontained:
        # consume generator as result will be pickled
        _result["add"] = [d for d in new.mget_from_ids(id_in_new)]
    summary = {"add" : len(id_in_new), "update" : len(_updates), "delete" : 0}
    if len(_updates) != 0 or len(id_in_new) != 0:
        dump(_result, file_name)
        # compute md5 so when downloaded, users can check integreity
        md5 = md5sum(file_name)
        summary["diff_file"] = {
                "name" : os.path.basename(file_name),
                "md5sum" : md5
                }

    return summary

def diff_worker_old_vs_new(id_list_old, new_db_col_names, batch_num, diff_folder):
    new = create_backend(new_db_col_names)
    docs_common = new.mget_from_ids(id_list_old)
    ids_common = [_doc['_id'] for _doc in docs_common]
    id_in_old = list(set(id_list_old)-set(ids_common))
    file_name = os.path.join(diff_folder,"%s.pyobj" % str(batch_num))
    _result = {'delete': id_in_old,
               'add': [],
               'update': [],
               'source': new.target_name,
               'timestamp': get_timestamp()}
    summary = {"add" : 0, "update": 0, "delete" : len(id_in_old)}
    if len(id_in_old) != 0:
        dump(_result, file_name)
        # compute md5 so when downloaded, users can check integreity
        md5 = md5sum(file_name)
        summary["diff_file"] = {
                "name" : os.path.basename(file_name),
                "md5sum" : md5
                }

    return summary


def diff_worker_count(id_list, db_col_names, batch_num):
    col = create_backend(db_col_names)
    docs = col.mget_from_ids(id_list)
    res = {}
    for doc in docs:
        for k in doc:
            res.setdefault(k,0)
            res[k] += 1
    return res


class DiffReportRendererBase(object):

    def __init__(self,
                 max_reported_ids=None,
                 max_randomly_picked=None,
                 detailed=False):
        self.max_reported_ids = max_reported_ids or hasattr(btconfig,"MAX_REPORTED_IDS") and \
                btconfig.MAX_REPORTED_IDS or 1000
        self.max_randomly_picked = max_randomly_picked or hasattr(btconfig,"MAX_RANDOMLY_PICKED") and \
                btconfig.MAX_RANDOMLY_PICKED or 10
        self.detailed = detailed

    def save(self,report,filename):
        """
        Save report output (rendered) into filename
        """
        raise NotImplementedError("implement me")


import locale
locale.setlocale(locale.LC_ALL, '')
class ReleaseNoteTxt(object):

    def __init__(self, changes):
        self.changes = changes

    def save(self, filepath):
        try:
            import prettytable
        except ImportError:
            raise ImportError("Please install prettytable to use this rendered")

        def format_number(n, sign=None):
            s = ""
            if sign:
                if n > 0:
                    s = "+"
                elif n < 0:
                    s = "-"
            try:
                n = abs(n)
                strn = "%s%s" % (s,locale.format("%d", n, grouping=True))
            except TypeError:
                # something wrong with converting, maybe we don't even have a number to format...
                strn = "N.A"
            return strn

        txt = ""
        title = "Build version: '%s'" % self.changes["new"]["_version"]
        txt += title + "\n"
        txt += "".join(["="] * len(title)) + "\n"
        dt = dtparse(self.changes["generated_on"])
        txt += "Previous build version: '%s'\n" % self.changes["old"]["_version"]
        txt += "Generated on: %s\n" % dt.strftime("%Y-%m-%d at %H:%M:%S")
        txt += "\n"

        table = prettytable.PrettyTable(["Updated datasource","prev. release","new release",
            "prev. # of docs","new # of docs"])
        table.align["Updated datasource"] = "l"
        table.align["prev. release"] = "c"
        table.align["new release"] = "c"
        table.align["prev. # of docs"] = "r"
        table.align["new # of docs"] = "r"

        for src,info in sorted(self.changes["sources"]["added"].items(),key=lambda e: e[0]):
            main_info = dict([(k,v) for k,v in info.items() if k.startswith("_")])
            sub_infos = dict([(k,v) for k,v in info.items() if not k.startswith("_")])
            if sub_infos:
                for sub,sub_info in sub_infos.items():
                    table.add_row(["%s.%s" % (src,sub),"-",main_info["_version"],"-",format_number(sub_info["_count"])]) # only _count avail there
            else:
                main_count = main_info.get("_count") and format_number(main_info["_count"]) or ""
                table.add_row([src,"-",main_info.get("_version",""),"-",main_count])
        for src,info in sorted(self.changes["sources"]["deleted"].items(),key=lambda e: e[0]):
            main_info = dict([(k,v) for k,v in info.items() if k.startswith("_")])
            sub_infos = dict([(k,v) for k,v in info.items() if not k.startswith("_")])
            if sub_infos:
                for sub,sub_info in sub_infos.items():
                    table.add_row(["%s.%s" % (src,sub),main_info.get("_version",""),"-",format_number(sub_info["_count"]),"-"]) # only _count avail there
            else:
                main_count = main_info.get("_count") and format_number(main_info["_count"]) or ""
                table.add_row([src,main_info.get("_version",""),"-",main_count,"-"])
        for src,info in sorted(self.changes["sources"]["updated"].items(),key=lambda e: e[0]):
            # extract information from main-source
            old_main_info = dict([(k,v) for k,v in info["old"].items() if k.startswith("_")])
            new_main_info = dict([(k,v) for k,v in info["new"].items() if k.startswith("_")])
            old_main_count = old_main_info.get("_count") and format_number(old_main_info["_count"]) or None
            new_main_count = new_main_info.get("_count") and format_number(new_main_info["_count"]) or None
            if old_main_count is None:
                assert new_main_count is None, "Sub-sources found for '%s', old and new count should " % src + \
                        "both be None. Info was: %s" % info
                old_sub_infos = dict([(k,v) for k,v in info["old"].items() if not k.startswith("_")])
                new_sub_infos = dict([(k,v) for k,v in info["new"].items() if not k.startswith("_")])
                # old & new sub_infos should have the same structure (same existing keys)
                # so we just use one of them to explore
                if old_sub_infos:
                    assert new_sub_infos
                    for sub,sub_info in old_sub_infos.items():
                        table.add_row(["%s.%s" % (src,sub),old_main_info.get("_version",""),new_main_info.get("_version",""),
                            format_number(sub_info["_count"]),format_number(new_sub_infos[sub]["_count"])])
            else:
                assert not new_main_count is None, "No sub-sources found, old and new count should NOT " + \
                        "both be None. Info was: %s" % info
                table.add_row([src,old_main_info.get("_version",""),new_main_info.get("_version",""),
                    old_main_count,new_main_count])

        if table._rows:
            txt += table.get_string()
            txt += "\n"
        else:
            txt += "No datasource changed.\n"

        total_count = self.changes["new"].get("_count")
        if self.changes["sources"]["added"]:
            txt += "New datasource(s): %s\n" % ", ".join(sorted(list(self.changes["sources"]["added"])))
        if self.changes["sources"]["deleted"]:
            txt += "Deleted datasource(s): %s\n" % ", ".join(sorted(list(self.changes["sources"]["deleted"])))
        if self.changes["sources"]:
            txt += "\n"

        if self.changes["new"]["_fields"]:
            new_fields = sorted(self.changes["new"]["_fields"].get("add",[]))
            deleted_fields = self.changes["new"]["_fields"].get("remove",[])
            updated_fields = self.changes["new"]["_fields"].get("replace",[])
            if new_fields:
                txt += "New field(s): %s\n" % ", ".join(new_fields)
            if deleted_fields:
                txt += "Deleted field(s): %s\n" % ", ".join(deleted_fields)
            if updated_fields:
                txt += "Updated field(s): %s\n" % ", ".join(updated_fields)
            txt += "\n"

        if total_count:
            txt += "Overall, %s documents in this release\n" % (format_number(total_count))
        if self.changes["new"]["_summary"]:
            sumups = []
            sumups.append("%s document(s) added" % format_number(self.changes["new"]["_summary"].get("add",0)))
            sumups.append("%s document(s) deleted" % format_number(self.changes["new"]["_summary"].get("delete",0)))
            sumups.append("%s document(s) updated" % format_number(self.changes["new"]["_summary"].get("update",0)))
            txt += ", ".join(sumups) + "\n"
        else:
            txt += "No information available for added/deleted/updated documents\n"

        if self.changes.get("note"):
            txt += "\n"
            txt += "Note: %s\n" % self.changes["note"]

        with open(filepath,"w") as fout:
            fout.write(txt)

        return txt


class DiffReportTxt(DiffReportRendererBase):

    def save(self, report, filename="report.txt"):
        try:
            import prettytable
        except ImportError:
            raise ImportError("Please install prettytable to use this rendered")

        def build_id_table(subreport):
            if self.detailed:
                table = prettytable.PrettyTable(["IDs","Root keys"])
                table.align["IDs"] = "l"
                table.align["Root keys"] = "l"
            else:
                table = prettytable.PrettyTable(["IDs"])
                table.align["IDs"] = "l"
            if subreport["count"] <= self.max_reported_ids:
                ids = subreport["ids"]
            else:
                ids = [random.choice(subreport["ids"]) for i in range(self.max_reported_ids)]
            for dat in ids:
                if self.detailed:
                    # list of [_id,[keys]]
                    table.add_row([dat[0],", ".join(dat[1])])
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
        if report.get("metadata",{}):
            txt += "Old collection: %s\n" % repr(report["metadata"].get("old"))
            txt += "New collection: %s\n" % repr(report["metadata"].get("new"))
            txt += "Batch size: %s\n" % report["metadata"]["diff"]["info"].get("batch_size")
            txt += "Steps: %s\n" % report["metadata"]["diff"]["info"].get("steps")
            txt += "Key(s) excluded: %s\n" % report["metadata"]["diff"]["info"].get("exclude")
            txt += "Diff generated on: %s\n" % report["metadata"]["diff"]["info"].get("generated_on")
        else:
            txt+= "No metadata found in report\n"
        txt += "\n"
        txt += "Summary\n"
        txt += "-------\n"
        txt += "Added documents: %s\n" % report["added"]["count"]
        txt += "Deleted documents: %s\n" % report["deleted"]["count"]
        txt += "Updated documents: %s\n" % report["updated"]["count"]
        txt += "\n"
        root_keys = report.get("metadata",{}).get("diff",{}).get("stats",{}).get("root_keys",{})
        if root_keys:
            for src in sorted(root_keys):
                txt += "%s: %s\n" % (src,root_keys[src])
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
                continue # already displayed
            if report["updated"][op]:
                table = prettytable.PrettyTable([op,"Count","Examples"])
                table.sortby = "Count"
                table.reversesort = True
                table.align[op] = "l"
                table.align["Count"] = "r"
                table.align["Examples"] = "l"
                for path in report["updated"][op]:
                    info = report["updated"][op][path]
                    row = [path,info["count"]]
                    if info["count"] <= self.max_randomly_picked:
                        row.append(", ".join(info["ids"]))
                    else:
                        row.append(", ".join([random.choice(info["ids"]) for i in range(self.max_randomly_picked)]))
                    table.add_row(row)
                txt += table.get_string()
                txt += "\n"
            else:
                txt += "No content found for diff operation '%s'\n" % op
            txt += "\n"
        txt += "\n"

        with open(os.path.join(btconfig.DIFF_PATH,report["diff_folder"],filename),"w") as fout:
            fout.write(txt)

        return txt


class DifferManager(BaseManager):

    def __init__(self, poll_schedule=None, *args,**kwargs):
        """
        DifferManager deals with the different differ objects used to create and
        analyze diff between datasources.
        """
        super(DifferManager,self).__init__(*args,**kwargs)
        self.log_folder = btconfig.LOG_FOLDER
        self.timestamp = datetime.now()
        self.poll_schedule = poll_schedule
        self.setup_log()

    def register_differ(self,klass):
        if klass.diff_type == None:
            raise DifferException("diff_type must be defined in %s" % klass)
        self.register[klass.diff_type] = partial(klass,log_folder=btconfig.LOG_FOLDER,
                                           job_manager=self.job_manager)

    def configure(self):
        for klass in [JsonDiffer,SelfContainedJsonDiffer]: # TODO: make it dynamic...
            self.register_differ(klass)

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, 'diffmanager_%s.log' % (time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("diffmanager")
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not nh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(nh)
        return self.logger

    def get_predicates(self, running_jobs={}):
        if not running_jobs:
            return None
        def no_other_diffmanager_step_running():
            """DiffManager deals with diff report, release note, publishing,
            none of them should run more than one at a time"""
            # Note: report output is part a publish_diff, release_note is impacted by diff content,
            # overall we keep things simple and don't allow more than one diff manager job to run
            # at the same time
            return len([j for j in running_jobs.values() if j["category"] == DIFFMANAGER_CATEGORY]) == 0
        return [no_other_diffmanager_step_running]

    def get_pinfo(self, job_manager=None):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {"category" : DIFFMANAGER_CATEGORY,
                "source" : "",
                "step" : "",
                "description" : ""}
        if job_manager:
            preds = self.get_predicates(job_manager.jobs)
            if preds:
                pinfo["__predicates__"] = preds
        return pinfo

    def __getitem__(self,diff_type):
        """
        Return an instance of a builder for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self,diff_type)
        return pclass()

    def diff(self, diff_type, old, new, batch_size=100000, steps=["content","mapping","post"],
            mode=None, exclude=["_timestamp"]):
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
            old = old or self.select_old_collection(new)
            job = differ.diff(old, new,
                              batch_size=batch_size,
                              steps=steps,
                              mode=mode,
                              exclude=exclude)
            def diffed(f):
                try:
                    res = f.result()
                    set_pending_to_release_note(new)
                except Exception as e:
                    self.logger.error("Error during diff: %s" % e)
                    raise
            job.add_done_callback(diffed)
            return job
        except KeyError as e:
            raise DifferException("No such differ '%s' (error: %s)" % (diff_type,e))


    def diff_report(self, old_db_col_names, new_db_col_names, report_filename="report.txt", format="txt", detailed=True,
                    max_reported_ids=None, max_randomly_picked=None, mode=None):

        max_reported_ids = max_reported_ids or hasattr(btconfig,"MAX_REPORTED_IDS") and \
                btconfig.MAX_REPORTED_IDS or 1000
        max_randomly_picked = max_randomly_picked or hasattr(btconfig,"MAX_RANDOMLY_PICKED") and \
                btconfig.MAX_RANDOMLY_PICKED or 10
        def do():
            if mode == "purge" or not os.path.exists(reportfilepath):
                assert format == "txt", "Only 'txt' format supported for now"
                report = self.build_diff_report(diff_folder, detailed, max_reported_ids)
                render = DiffReportTxt(max_reported_ids=max_reported_ids,
                                       max_randomly_picked=max_randomly_picked,
                                       detailed=detailed)
                return render.save(report,report_filename)
            else:
                self.logger.debug("Report already generated, now using it")
                return open(reportfilepath).read()

        @asyncio.coroutine
        def main(diff_folder):
            got_error = False
            pinfo = self.get_pinfo(self.job_manager)
            pinfo["step"] = "report"
            pinfo["source"] = diff_folder
            pinfo["description"] = report_filename
            job = yield from self.job_manager.defer_to_thread(pinfo,do)
            def reported(f):
                nonlocal got_error
                try:
                    res = f.result()
                    self.logger.info("Diff report ready, saved in %s" % reportfilepath,extra={"notify":True,"attach":reportfilepath})
                except Exception as e:
                    got_error = e
            job.add_done_callback(reported)
            yield from job
            if got_error:
                self.logger.exception("Failed to create diff report: %s" % got_error,extra={"notify":True})
                raise got_error

        diff_folder = generate_folder(btconfig.DIFF_PATH,old_db_col_names,new_db_col_names)
        reportfilepath = os.path.join(diff_folder,report_filename)
        job = asyncio.ensure_future(main(diff_folder))
        return job

    def release_note(self, old, new, filename=None, note=None, format="txt"):
        """
        Generate release note files, in TXT and JSON format, containing significant changes
        summary between target collections old and new. Output files
        are stored in a diff folder using generate_folder(old,new).

        'filename' can optionally be specified, though it's not recommended as the publishing pipeline,
        using these files, expects a filenaming convention.

        'note' is an optional free text that can be added to the release note, at the end.

        txt 'format' is the only one supported for now.
        """
        old = old or self.select_old_collection(new)
        release_folder = generate_folder(btconfig.RELEASE_PATH,old,new)
        if not os.path.exists(release_folder):
            os.makedirs(release_folder)
        filepath = None

        def do():
            changes = self.build_release_note(old,new,note=note)
            nonlocal filepath
            nonlocal filename
            assert format == "txt", "Only 'txt' format supported for now"
            filename = filename or "release_%s.%s" % (changes["new"]["_version"],format)
            filepath = os.path.join(release_folder,filename)
            render = ReleaseNoteTxt(changes)
            txt = render.save(filepath)
            filename = filename.replace(".%s" % format,".json")
            filepath = os.path.join(release_folder,filename)
            json.dump(changes,open(filepath,"w"),indent=True)
            return txt

        @asyncio.coroutine
        def main(release_folder):
            got_error = False
            pinfo = self.get_pinfo(self.job_manager)
            pinfo["step"] = "release_note"
            pinfo["source"] = release_folder
            pinfo["description"] = filename
            job = yield from self.job_manager.defer_to_thread(pinfo,do)
            def reported(f):
                nonlocal got_error
                try:
                    res = f.result()
                    assert filepath, "No filename defined for generated report, can't attach"
                    self.logger.info("Release note ready, saved in %s: %s" % (release_folder,res),extra={"notify":True})
                    set_pending_to_publish(new)
                except Exception as e:
                    got_error = e
            job.add_done_callback(reported)
            yield from job
            if got_error:
                self.logger.exception("Failed to create release note: %s" % got_error,extra={"notify":True})
                raise got_error

        job = asyncio.ensure_future(main(release_folder))
        return job

    def build_release_note(self, old_db_col_names=None, new_db_col_names=None, diff_folder=None, note=None):
        """
        Build a release note containing most significant changes between old_db_col_names and new_db_col_names
        collection (they have to be target collections, coming from a merging process). "diff_folder" can
        alternatively be passed instead of old/new_db_col_names.

        If diff_folder already contains information about a diff (metadata.json), the release note will be 
        enriched by such information. Otherwise, release note will be generated only with data coming from src_build.
        In other words, release note can still be generated without diff information.

        Return a dictionnary containing significant changes.
        """
        def get_counts(doc):
            stats = {}
            for subsrc,count in doc.get("merge_stats",{}).items():
                src_sub = get_source_fullname(subsrc).split(".")
                if len(src_sub) > 1:
                    # we have sub-sources we need to split the count
                    src,sub = src_sub
                    stats.setdefault(src,{})
                    stats[src][sub] = {"_count" : count}
                else:
                    src = src_sub[0]
                    stats[src] = {"_count" : count}
            return stats

        def get_versions(doc):
            try:
                versions = dict((k,{"_version" :v["version"]}) for k,v in \
                        doc.get("_meta",{}).get("src",{}).items() if "version" in v)
            except KeyError:
                # previous version format
                versions = dict((k,{"_version" : v}) for k,v in doc.get("_meta",{}).get("src_version",{}).items())
            return versions

        if old_db_col_names is None and new_db_col_names is None:
            assert diff_folder, "Need at least diff_folder parameter"
        else:
            diff_folder = generate_folder(btconfig.DIFF_PATH,old_db_col_names,new_db_col_names)
        try:
            metafile = os.path.join(diff_folder,"metadata.json")
            metadata = json.load(open(metafile))
            old_db_col_names = metadata["old"]["backend"]
            new_db_col_names = metadata["new"]["backend"]
            diff_stats = metadata["diff"]["stats"]
        except FileNotFoundError:
            # we're generating a release note without diff information
            self.logger.info("No metadata.json file found, this release note won't have diff stats included")
            diff_stats = {}

        new = create_backend(new_db_col_names)
        old = create_backend(old_db_col_names)
        assert isinstance(old,DocMongoBackend) and isinstance(new,DocMongoBackend), \
                "Only MongoDB backend types are allowed when generating a release note"
        assert old.target_collection.database.name == btconfig.DATA_TARGET_DATABASE and \
                new.target_collection.database.name == btconfig.DATA_TARGET_DATABASE, \
                "Target databases must match current DATA_TARGET_DATABASE setting"
        new_doc = get_src_build().find_one({"_id":new.target_collection.name})
        if not new_doc:
            raise DifferException("Collection '%s' has no corresponding build document" % \
                    new.target_collection.name)
        # old_doc doesn't have to exist (but new_doc has) in case we build a initial release note
        # compared against nothing
        old_doc = get_src_build().find_one({"_id":old.target_collection.name}) or {}
        tgt_db = get_target_db()
        old_total = tgt_db[old.target_collection.name].count()
        new_total = tgt_db[new.target_collection.name].count()
        changes = {
                "old" : {
                    "_version" : old.version,
                    "_count" : old_total,
                    },
                "new" : {
                    "_version" : new.version,
                    "_count" : new_total,
                    "_fields" : {},
                    "_summary" : diff_stats,
                    },
                "note" : note,
                "generated_on": str(datetime.now()),
                "sources" : {
                    "added" : {},
                    "deleted" : {},
                    "updated" : {},
                    }
                }
        op_map = {"replace" : "updated",
                  "add" : "added",
                  "delete" : "deleted"}
        # for later use
        new_versions = get_versions(new_doc)
        old_versions = get_versions(old_doc)
        # now deal with stats/counts. Counts are related to uploader, ie. sub-sources
        new_stats = get_counts(new_doc)
        old_stats = get_counts(old_doc)
        new_info = update_dict_recur(new_versions,new_stats)
        old_info = update_dict_recur(old_versions,old_stats)
        #print("new_info = %s" % new_info)
        #print("old_info = %s" % old_info)

        ops = jsondiff(old_info,new_info)
        for op in ops:
            # get main source
            main_src = op["path"].strip("/").split("/")[0]
            if op["op"] == "add":
                changes["sources"]["added"][main_src] = new_info[main_src]
            elif op["op"] == "remove":
                changes["sources"]["deleted"][main_src] = old_info[main_src]
            elif op["op"] == "replace":
                remains = op["path"].strip("/").split("/")[1:]
                changes["sources"]["updated"][main_src] = {
                        "new" : new_info[main_src],
                        "old" : old_info[main_src]}
            else:
                raise ValueError("Unknown operation '%s' while computing changes" % op["op"])

        # mapping diff: we re-compute them and don't use any mapping.pyobj because that file
        # only allows "add" operation as a safety rule (can't delete fields in ES mapping once indexed)
        ops = jsondiff(old_doc.get("mapping",{}),new_doc["mapping"])
        for op in ops:
            changes["new"]["_fields"].setdefault(op["op"],[]).append(op["path"].strip("/").replace("/","."))

        return changes


    def build_diff_report(self, diff_folder, detailed=True, max_reported_ids=None):
        """
        Analyze diff files in diff_folder and give a summy of changes.
        max_reported_ids is the number of IDs contained in the report for each part.
        detailed will trigger a deeper analysis, takes more time.
        """

        max_reported_ids = max_reported_ids or hasattr(btconfig,"MAX_REPORTED_IDS") and \
                btconfig.MAX_REPORTED_IDS or 1000

        update_details = {
                "add": {},# "count": 0, "data": {} },
                "remove": {}, # "count": 0, "data": {} },
                "replace": {}, # "count": 0, "data": {} },
                "move": {}, # "count": 0, "data": {} },
                "count": 0,
                }
        adds = {"count": 0, "ids": []}
        dels = {"count": 0, "ids": []}
        sources = {}

        if os.path.isabs(diff_folder):
            data_folder = diff_folder
        else:
            data_folder = os.path.join(btconfig.DIFF_PATH,diff_folder)

        metadata = {}
        try:
            metafile = os.path.join(data_folder,"metadata.json")
            metadata = json.load(open(metafile))
        except FileNotFoundError:
            logging.warning("Not metadata found in diff folder")
            if detailed:
                raise Exception("Can't perform detailed analysis without a metadata file")

        def analyze(diff_file, detailed):
            data = loadobj(diff_file)
            sources[data["source"]] = 1
            if detailed:
                # TODO: if self-contained, no db connection needed
                new_col = create_backend(metadata["new"]["backend"])
                old_col = create_backend(metadata["old"]["backend"])
            if len(adds["ids"]) < max_reported_ids:
                if detailed:
                    # look for which root keys were added in new collection
                    for _id in data["add"]:
                        # selfcontained = dict for whole doc (see TODO above)
                        if type(_id) == dict:
                            _id = _id["_id"]
                        doc = new_col.get_from_id(_id)
                        rkeys = sorted(doc.keys())
                        adds["ids"].append([_id,rkeys])
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
                        dels["ids"].append([_id,rkeys])
                else:
                    dels["ids"].extend(data["delete"])
            dels["count"] += len(data["delete"])
            for up in data["update"]:
                for patch in up["patch"]:
                    update_details[patch["op"]].setdefault(patch["path"],{"count": 0, "ids": []})
                    if len(update_details[patch["op"]][patch["path"]]["ids"]) < max_reported_ids:
                        update_details[patch["op"]][patch["path"]]["ids"].append(up["_id"])
                    update_details[patch["op"]][patch["path"]]["count"] += 1
            update_details["count"] += len(data["update"])

            assert len(sources) == 1, "Should have one datasource from diff files, got: %s" % [s for s in sources]

        # we randomize files order b/c we randomly pick some examples from those
        # files. If files contains data in order (like chrom 1, then chrom 2)
        # we won't have a representative sample
        files = glob.glob(os.path.join(data_folder,"*.pyobj"))
        random.shuffle(files)
        total = len(files)
        for i,f in enumerate(files):
            if os.path.basename(f).startswith("mapping"):
                logging.debug("Skip mapping file")
                continue
            logging.info("Running report worker for '%s' (%d/%d)" % (f,i+1,total))
            analyze(f, detailed)
        return {"added" : adds, "deleted": dels, "updated" : update_details,
                "diff_folder" : diff_folder, "detailed": detailed,
                "metadata": metadata}

    def reset_synced(self,diff_folder,backend=None):
        """
        Remove "synced" flag from any pyobj file in diff_folder
        """
        synced_files = glob.glob(os.path.join(diff_folder,"*.pyobj.synced"))
        for synced in synced_files:
            diff_file = re.sub("\.pyobj\.synced$",".pyobj",synced)
            os.rename(synced,diff_file)

    def publish_diff(self, s3_folder, old_db_col_names=None, new_db_col_names=None,
            diff_folder=None, release_folder=None, steps=["reset","upload","meta"], s3_bucket=None):
        """
        Publish diff data diff files in config.S3_DIFF_BUCKET/s3_folder and metadata, release notes, etc...
        in config.S3_RELEASE_BUCKET/s3_folder, and then register that version so it's available to auto-updating hub.
        - either pass old_db_col_names and new_db_col_names collections names, or diff_folder containing diff data.
        - same for 'release_folder'
        - steps:
          * reset: highly recommended, reset synced flag in diff files so they won't get skipped when used...
          * upload: upload diff_folder content to S3
          * meta: publish/register the version as available for auto-updating hubs
        """
        # check what to do
        if type(steps) == str:
            steps = [steps]
        if "meta" in steps:
            assert getattr(btconfig,"BIOTHINGS_ROLE","master"), "Hub must be master to publish metadata about diff release"
        if not diff_folder:
            assert old_db_col_names and new_db_col_names, "No diff_folder specified, old_db_col_names and new_db_col_names are required"
            diff_folder = generate_folder(btconfig.DIFF_PATH,old_db_col_names,new_db_col_names)
        if not release_folder:
            assert old_db_col_names and new_db_col_names, "No release_folder specified, old_db_col_names and new_db_col_names are required"
            release_folder = generate_folder(btconfig.RELEASE_PATH,old_db_col_names,new_db_col_names)
        try:
            meta = json.load(open(os.path.join(diff_folder,"metadata.json")))
        except FileNotFoundError:
            raise FileNotFoundError("metadata.json is missing")
        diff_version = meta["diff"]["version"]
        s3basedir = os.path.join(s3_folder,diff_version)
        release_note = "release_%s" % meta["new"]["version"]
        s3_bucket = s3_bucket or btconfig.S3_DIFF_BUCKET

        @asyncio.coroutine
        def do():
            jobs = []
            pinfo = self.get_pinfo(self.job_manager)
            pinfo["step"] = "upload_diff"
            pinfo["source"] = diff_folder
            pinfo["description"] = diff_version
            if "reset" in steps:
                # first we need to reset "synced" flag in diff files to make
                # sure all of them will be applied by client
                pinfo["step"] = "reset synced"
                self.logger.info("Resetting 'synced' flag in pyobj files located in folder '%s'" % diff_folder)
                job = yield from self.job_manager.defer_to_thread(pinfo,partial(self.reset_synced,diff_folder))
                yield from job
                jobs.append(job)

            if "upload" in steps:
                # then we upload all the folder content
                pinfo["step"] = "upload"
                self.logger.info("Uploading files from '%s' to s3 (%s/%s)" % (diff_folder,s3_bucket,s3basedir))
                job = yield from self.job_manager.defer_to_thread(pinfo,partial(aws.send_s3_folder,
                    diff_folder,s3basedir=s3basedir,
                    aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                    s3_bucket=s3_bucket,overwrite=True))
                yield from job
                jobs.append(job)

            if "meta" in steps:
                # finally we create a metadata json file pointing to this release
                def gen_meta():
                    pinfo["step"] = "generate meta"
                    self.logger.info("Generating JSON metadata for incremental release '%s'" % diff_version)
                    # if the same, this would create an infinite loop in autoupdate hub
                    # (X requires X, where to find X ? there, but X requires X, where to find X ?...)
                    if meta["old"]["version"] == meta["new"]["version"]:
                        raise DifferException("Required version is the same as target version " + \
                                "('%s'), prevent publishing to avoid infinite loop " % meta["new"]["version"] + \
                                "while resolving updates in auto-update hub")
                    # generate json metadata about this diff release
                    diff_meta = {
                            "type": "incremental",
                            "build_version": diff_version,
                            "require_version": meta["old"]["version"],
                            "target_version": meta["new"]["version"],
                            "release_date" : datetime.now().isoformat(),
                            "app_version": None,
                            "metadata" : {"url" : aws.get_s3_url(os.path.join(s3basedir,"metadata.json"),
                                aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=s3_bucket)},
                            }
                    # upload release notes
                    notes = glob.glob(os.path.join(release_folder,"%s.*" % release_note))
                    for note in notes:
                        if os.path.exists(note):
                            s3key = os.path.join(s3basedir,os.path.basename(note))
                            aws.send_s3_file(note,s3key,
                                    aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                                    s3_bucket=btconfig.S3_RELEASE_BUCKET,overwrite=True)

                    rel_txt_url = aws.get_s3_url(os.path.join(s3basedir,"%s.txt" % release_note),
                                    aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=btconfig.S3_RELEASE_BUCKET)
                    rel_json_url = aws.get_s3_url(os.path.join(s3basedir,"%s.json" % release_note),
                                    aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=btconfig.S3_RELEASE_BUCKET)
                    if rel_txt_url:
                        diff_meta.setdefault("changes",{})
                        diff_meta["changes"]["txt"] = {"url" : rel_txt_url}
                    if rel_json_url:
                        diff_meta.setdefault("changes",{})
                        diff_meta["changes"]["json"] = {"url" : rel_json_url}

                    diff_file = "%s.json" % diff_version
                    diff_meta_path = os.path.join(btconfig.RELEASE_PATH,diff_file)
                    json.dump(diff_meta,open(diff_meta_path,"w"),indent=True)
                    # get a timestamp from metadata to force lastdmodifed header
                    # timestamp is when the new collection was built (not when the diff
                    # was generated, as diff can be generated way after). New collection's
                    # timestamp remains a good choice as data (diff) relates to that date anyway
                    metadata = json.load(open(os.path.join(diff_folder,"metadata.json")))
                    local_ts = dtparse(diff_meta["release_date"])
                    utc_epoch = int(time.mktime(local_ts.timetuple()))
                    utc_ts = datetime.fromtimestamp(time.mktime(time.gmtime(utc_epoch)))
                    str_utc_epoch = str(utc_epoch)
                    s3key = os.path.join(s3_folder,diff_file)
                    aws.send_s3_file(diff_meta_path,s3key,
                            aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                            s3_bucket=btconfig.S3_RELEASE_BUCKET,metadata={"lastmodified":str_utc_epoch},
                             overwrite=True)
                    url = aws.get_s3_url(s3key,aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                            s3_bucket=btconfig.S3_RELEASE_BUCKET)
                    self.logger.info("Incremental release metadata published for version: '%s'" % url)
                    version_info = {"build_version":diff_meta["build_version"],
                            "require_version":diff_meta["require_version"],
                            "target_version":diff_meta["target_version"],
                            "type":diff_meta["type"],
                            "release_date":diff_meta["release_date"],
                            "url":url}
                    publish_data_version(s3_folder,version_info)
                    self.logger.info("Registered version '%s'" % (diff_version))
                job = yield from self.job_manager.defer_to_thread(pinfo,gen_meta)
                yield from job
                jobs.append(job)

            def uploaded(f):
                try:
                    res = f.result()
                    self.logger.info("Diff folder '%s' uploaded to S3: %s" % (diff_folder,res),extra={"notify":True})
                except Exception as e:
                    self.logger.error("Failed to upload diff folder '%s' uploaded to S3: %s" % (diff_folder,e),extra={"notify":True})

            yield from asyncio.wait(jobs)
            task = asyncio.gather(*jobs)
            task.add_done_callback(uploaded)
            yield from task

        return asyncio.ensure_future(do())

    def select_old_collection(self,new_id):
        """
        Given 'new_id', an _id from src_build, as the "new" collection,
        automatically select an "old" collection. By default, src_build's documents
        will be sorted according to their name (_id) and old colleciton is the one
        just before new_id.
        Note: because there can more than one build config used, the actual build config
        name is first determined using new_id collection name, then the find.sort is done
        on collections containing that build config name.
        """
        # TODO: this is not compatible with a generic hub_db backend
        # TODO: this should return a collection with status=success
        col = get_src_build()
        doc = col.find_one({"_id":new_id})
        assert doc, "No build document found for '%s'" % new_id
        assert "build_config" in doc, "No build configuration found for document '%s'" % new_id 
        assert doc["build_config"]["name"] == doc["build_config"]["_id"]
        confname = doc["build_config"]["name"]
        docs = get_src_build().find({
            "$and":[
                {"_id":{"$lte":new_id}},
                {"_id":{"$regex":"^%s.*" % confname}}
                ]},
            {"_id":1}).sort([("_id",-1)]).limit(2) 

        _ids = [d["_id"] for d in docs]
        assert len(_ids) == 2, "Expecting 2 collection _ids, got: %s" % _ids
        assert _ids[0] == new_id, "Can't find collection _id '%s'" % new_id
        return _ids[1]

    def poll(self,state,func):
        super(DifferManager,self).poll(state,func,col=get_src_build())

    def trigger_diff(self,diff_type,doc,**kwargs):
        """
        Launch a diff given a src_build document. In order to 
        know the first collection to diff against, select_old_collection()
        method is used.
        """
        new_db_col_names = doc["_id"]
        old_db_col_names = self.select_old_collection(doc)
        self.diff(diff_type, old_db_col_names, new_db_col_names, **kwargs)

    def trigger_release_note(self,doc,**kwargs):
        """
        Launch a release note generation given a src_build document. In order to 
        know the first collection to compare with, select_old_collection()
        method is used. release_note() method will get **kwargs for more optional
        parameters.
        """
        new_db_col_names = doc["_id"]
        old_db_col_names = self.select_old_collection(doc)
        self.release_note(old_db_col_names, new_db_col_names, **kwargs)


def set_pending_to_diff(col_name):
    src_build = get_src_build()
    src_build.update({"_id":col_name},{"$addToSet" : {"pending":"diff"} })

def set_pending_to_release_note(col_name):
    src_build = get_src_build()
    src_build.update({"_id":col_name},{"$addToSet" : {"pending":"release_note"} })

def set_pending_to_publish(col_name):
    src_build = get_src_build()
    src_build.update({"_id":col_name},{"$addToSet" : {"pending":"publish"} })
