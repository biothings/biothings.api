import os
import time, hashlib
import pickle, json
from datetime import datetime
from pprint import pformat
import asyncio
from functools import partial
import glob, random

from biothings.utils.common import timesofar, iter_n, get_timestamp, \
                                   dump, rmdashfr, loadobj, md5sum
from biothings.utils.mongo import id_feeder
from biothings.utils.loggers import get_logger, HipchatHandler
from biothings.utils.diff import diff_docs_jsonpatch, generate_diff_folder
from biothings import config as btconfig
from biothings.utils.manager import BaseManager, ManagerError
from biothings.databuild.backend import create_backend
import biothings.utils.aws as aws
from biothings.databuild.syncer import SyncerManager

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
        # TODO: use bt.utils.loggers.get_logger
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

    @asyncio.coroutine
    def diff_cols(self,old_db_col_names, new_db_col_names, batch_size=100000, steps=["count","content"], mode=None, exclude=[]):
        """
        Compare new with old collections and produce diff files. Root keys can be excluded from
        comparison with "exclude" parameter.
        *_db_col_names can be: 
         1. a colleciton name (as a string) asusming they are
            in the target database.
         2. tuple with 2 elements, the first one is then either "source" or "target"
            to respectively specify src or target database, and the second element is
            the collection name.
         3. tuple with 3 elements (URI,db,collection), looking like:
            ("mongodb://user:pass@host","dbname","collection"), allowing to specify
            any connection on any server
        steps: 'count' will count the root keys for every documents in new collection 
               (to check number of docs from datasources).
               'content' will perform diff on actual content.
        mode: 'purge' will remove any existing files for this comparison.
        """
        new = create_backend(new_db_col_names)
        old = create_backend(old_db_col_names)
        # check what to do
        if type(steps) == str:
            steps = [steps]

        diff_folder = generate_diff_folder(old_db_col_names,new_db_col_names)

        if os.path.exists(diff_folder):
            if mode == "purge" and os.path.exists(diff_folder):
                rmdashfr(diff_folder)
            else:
                raise FileExistsError("Found existing files in '%s', use mode='purge'" % diff_folder)
        if not os.path.exists(diff_folder):
            os.makedirs(diff_folder)


        # create metadata file storing info about how we created the diff
        # and some summary data
        stats = {"update":0, "add":0, "delete":0}
        metadata = {
                "diff_type" : self.diff_type,
                "diff_func" : self.diff_func.__name__,
                "old": old_db_col_names,
                "new": new_db_col_names,
                "old_version": old.version,
                "new_version": new.version,
                "diff_version": "%s.%s" % (old.version,new.version),
                "batch_size": batch_size,
                "steps": steps,
                "mode": mode,
                "exclude": exclude,
                "generated_on": str(datetime.now()),
                "stats": stats, # ref to stats
                "diff_files": []}
        # dump it here for minimum information, in case we don't go further
        json.dump(metadata,open(os.path.join(diff_folder,"metadata.json"),"w"))

        if "count" in steps:
            cnt = 0
            pinfo = {"category" : "diff",
                     "step" : "count",
                     "source" : "%s vs %s" % (new.target_name,old.target_name),
                     "description" : ""}

            self.logger.info("Counting root keys in '%s'"  % new.target_name)
            stats["root_keys"] = {}
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
                stats["root_keys"] = root_keys
            tasks = asyncio.gather(*jobs)
            tasks.add_done_callback(counted)
            yield from tasks
            self.logger.info("Finished counting keys in the new collection: %s" % stats["root_keys"])

        if "content" in steps:
            skip = 0
            cnt = 0
            jobs = []
            pinfo = {"category" : "diff",
                     "source" : "%s vs %s" % (new.target_name,old.target_name),
                     "step" : "content: new vs old",
                     "description" : ""}
            data_new = id_feeder(new, batch_size=batch_size)
            selfcontained = "selfcontained" in self.diff_type
            for id_list_new in data_new:
                cnt += 1
                pinfo["description"] = "batch #%s" % cnt
                def diffed(f):
                    res = f.result()
                    stats["update"] += res["update"]
                    stats["add"] += res["add"]
                    if res.get("diff_file"):
                        metadata["diff_files"].append(res["diff_file"])
                    self.logger.info("(Updated: {}, Added: {})".format(res["update"], res["add"]))
                self.logger.info("Creating diff worker for batch #%s" % cnt)
                job = yield from self.job_manager.defer_to_process(pinfo,
                        partial(diff_worker_new_vs_old, id_list_new, old_db_col_names,
                                new_db_col_names, cnt , diff_folder, self.diff_func, exclude, selfcontained))
                job.add_done_callback(diffed)
                jobs.append(job)
            yield from asyncio.gather(*jobs)
            self.logger.info("Finished calculating diff for the new collection. Total number of docs updated: {}, added: {}".format(stats["update"], stats["add"]))

            data_old = id_feeder(old, batch_size=batch_size)
            jobs = []
            pinfo["step"] = "content: old vs new"
            for id_list_old in data_old:
                cnt += 1
                pinfo["description"] = "batch #%s" % cnt
                def diffed(f):
                    res = f.result()
                    stats["delete"] += res["delete"]
                    if res.get("diff_file"):
                        metadata["diff_files"].append(res["diff_file"])
                    self.logger.info("(Deleted: {})".format(res["delete"]))
                self.logger.info("Creating diff worker for batch #%s" % cnt)
                job = yield from self.job_manager.defer_to_process(pinfo,
                        partial(diff_worker_old_vs_new, id_list_old, new_db_col_names, cnt , diff_folder))
                job.add_done_callback(diffed)
                jobs.append(job)
            yield from asyncio.gather(*jobs)
            self.logger.info("Finished calculating diff for the old collection. Total number of docs deleted: {}".format(stats["delete"]))
            self.logger.info("Summary: (Updated: {}, Added: {}, Deleted: {})".format(stats["update"], stats["add"], stats["delete"]))

        # pickle again with potentially more information (stats)
        json.dump(metadata,open(os.path.join(diff_folder,"metadata.json"),"w"))
        strargs = "[old=%s,new=%s,steps=%s,stats=%s]" % (old_db_col_names,new_db_col_names,steps,stats)
        self.logger.info("success %s" % strargs,extra={"notify":True})
        return stats

    def diff(self,old_db_col_names, new_db_col_names, batch_size=100000, steps=["count","content"], mode=None, exclude=[]):
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
        _result["add"] = new.mget_from_ids(id_in_new)
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
                 max_reported_ids=btconfig.MAX_REPORTED_IDS,
                 max_randomly_picked=btconfig.MAX_RANDOMLY_PICKED,
                 detailed=False):
        self.max_reported_ids = max_reported_ids
        self.max_randomly_picked = max_randomly_picked
        self.detailed = detailed

    def save(self,report,filename):
        """
        Save report output (rendered) into filename
        """
        raise NotImplementedError("implement me")


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
            txt += "Batch size: %s\n" % report["metadata"].get("batch_size")
            txt += "Steps: %s\n" % report["metadata"].get("steps")
            txt += "Key(s) excluded: %s\n" % report["metadata"].get("exclude")
            txt += "Diff generated on: %s\n" % report["metadata"].get("generated_on")
        else:
            txt+= "No metadata found in report\n"
        txt += "\n"
        txt += "Summary\n"
        txt += "-------\n"
        txt += "Added documents: %s\n" % report["added"]["count"]
        txt += "Deleted documents: %s\n" % report["deleted"]["count"]
        txt += "Updated documents: %s\n" % report["updated"]["count"]
        txt += "\n"
        root_keys = report.get("metadata",{}).get("stats",{}).get("root_keys",{})
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

    def __init__(self, *args,**kwargs):
        """
        DifferManager deals with the different differ objects used to create and
        analyze diff between datasources.
        """
        super(DifferManager,self).__init__(*args,**kwargs)
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
        self.logger = btconfig.logger

    def __getitem__(self,diff_type):
        """
        Return an instance of a builder for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self,diff_type)
        return pclass()

    def diff(self, diff_type, old_db_col_names, new_db_col_names, batch_size=100000, steps=["count","content"], mode=None, exclude=[]):
        """
        Run a diff to compare old vs. new collections. using differ algorithm diff_type. Results are stored in
        a diff folder.
        Steps can be passed to choose what to do:
        - count: will count root keys in new collections and stores them as statistics.
        - content: will diff the content between old and new. Results (diff files) format depends on diff_type
        """
        try:
            differ = self[diff_type]
            job = differ.diff(old_db_col_names, new_db_col_names,
                              batch_size=batch_size,
                              steps=steps,
                              mode=mode,
                              exclude=exclude)
            return job
        except KeyError as e:
            raise DifferException("No such differ '%s' (error: %s)" % (diff_type,e))


    def diff_report(self, old_db_col_names, new_db_col_names, report_filename="report.txt", format="txt", detailed=True,
                    max_reported_ids=btconfig.MAX_REPORTED_IDS, max_randomly_picked=btconfig.MAX_RANDOMLY_PICKED):

        def do():
            report = self.build_diff_report(diff_folder, detailed, max_reported_ids)
            assert format == "txt", "Only 'txt' format supported for now"
            render = DiffReportTxt(max_reported_ids=max_reported_ids,
                                   max_randomly_picked=max_randomly_picked,
                                   detailed=detailed)
            return render.save(report,report_filename)

        @asyncio.coroutine
        def main(diff_folder):
            got_error = False
            pinfo = {"category" : "diff",
                     "step" : "report",
                     "source" : diff_folder,
                     "description" : report_filename}
            job = yield from self.job_manager.defer_to_thread(pinfo,do)
            def reported(f):
                nonlocal got_error
                try:
                    res = f.result()
                    self.logger.info("Diff report ready, saved in %s:\n%s" % (os.path.join(diff_folder,report_filename),res),extra={"notify":True})
                except Exception as e:
                    got_error = e
            job.add_done_callback(reported)
            yield from job
            if got_error:
                self.logger.exception("Failed to create diff report: %s" % got_error,extra={"notify":True})
                raise got_error

        diff_folder = generate_diff_folder(old_db_col_names,new_db_col_names)
        job = asyncio.ensure_future(main(diff_folder))
        return job

    def build_diff_report(self, diff_folder, detailed=True,
                          max_reported_ids=btconfig.MAX_REPORTED_IDS):
        """
        Analyze diff files in diff_folder and give a summy of changes.
        max_reported_ids is the number of IDs contained in the report for each part.
        detailed will trigger a deeper analysis, takes more time.
        """

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
                new_col = create_backend(metadata["new"])
                old_col = create_backend(metadata["old"])
            if len(adds) < max_reported_ids:
                if detailed:
                    # look for which root keys were added in new collection
                    for _id in data["add"]:
                        doc = new_col.get_from_id(_id)
                        rkeys = sorted(doc.keys())
                        adds["ids"].append([_id,rkeys])
                else:
                    adds["ids"].extend(data["add"])
            adds["count"] += len(data["add"])
            if len(dels) < max_reported_ids:
                if detailed:
                    # look for which root keys were deleted in old collection
                    for _id in data["delete"]:
                        doc = old_col.get_from_id(_id)
                        rkeys = sorted(doc.keys())
                        dels["ids"].append([_id,rkeys])
                else:
                    dels["ids"].extend(data["add"])
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
        for f in files:
            logging.info("Running report worker for '%s'" % f)
            analyze(f, detailed)
        return {"added" : adds, "deleted": dels, "updated" : update_details,
                "diff_folder" : diff_folder, "detailed": detailed,
                "metadata": metadata}

    def upload_diff(self, old_db_col_names=None, new_db_col_names=None, diff_folder=None):
        if not diff_folder:
            assert old_db_col_names and new_db_col_names, "No diff_folder specified, old_db_col_names and new_db_col_names are required"
            diff_folder = generate_diff_folder(old_db_col_names,new_db_col_names)
        meta = json.load(open(os.path.join(diff_folder,"metadata.json")))
        diff_version = meta["diff_version"]

        @asyncio.coroutine
        def do():
            jobs = []
            pinfo = {"category" : "upload_diff",
                     "source" : diff_folder,
                     "step": None,
                     "description" : diff_version}
            # first we need to reset "synced" flag in diff files to make
            # sure all of them will be applied by client
            pinfo["step"] = "reset synced"
            self.logger.info("Resetting 'synced' flag in pyobj files located in folder '%s'" % diff_folder)
            job = yield from self.job_manager.defer_to_thread(pinfo,partial(SyncerManager.reset_synced,diff_folder))
            yield from job
            jobs.append(job)
            # then we upload all the folder content
            pinfo["step"] = "upload"
            self.logger.info("Uploading files from '%s' to s3" % diff_folder)
            s3basedir = os.path.join(btconfig.S3_DIFF_FOLDER,diff_version)
            job = yield from self.job_manager.defer_to_thread(pinfo,partial(aws.send_s3_folder,
                diff_folder,s3basedir=s3basedir,
                aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                s3_bucket=btconfig.S3_DIFF_BUCKET,
                overwrite=True,permissions="public-read"))
            yield from job
            jobs.append(job)
            # finally we create a metadata json file pointing to this release
            def gen_meta():
                pinfo["step"] = "generate meta"
                self.logger.info("Generating JSON metadata for incremental release '%s'" % diff_version)
                # generate json metadata about this diff release
                diff_meta = {
                        "type": "incremental",
                        "build_version": diff_version,
                        "app_version": None,
                        "metadata" : {"url" : aws.get_s3_url(os.path.join(s3basedir,"metadata.json"),
                            aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=btconfig.S3_DIFF_BUCKET)}
                        }
                diff_file = "%s.json" % diff_version
                diff_meta_path = os.path.join(btconfig.DIFF_PATH,diff_file)
                json.dump(diff_meta,open(diff_meta_path,"w"))
                aws.send_s3_file(diff_meta_path,os.path.join(btconfig.S3_DIFF_FOLDER,diff_file),
                        aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                        s3_bucket=btconfig.S3_DIFF_BUCKET,
                         overwrite=True,permissions="public-read")
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

