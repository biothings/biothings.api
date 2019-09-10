import sys, re, os, time, math, glob, copy
from datetime import datetime
from dateutil.parser import parse as dtparse
import pickle, json
from pprint import pformat
import asyncio
from functools import partial
import subprocess

from biothings.utils.mongo import get_previous_collection, get_target_db
from biothings.utils.hub_db import get_src_build, get_source_fullname
import biothings.utils.aws as aws
from biothings.utils.common import timesofar, get_random_string, iter_n, \
                                   get_class_from_classpath, get_dotfield_value
from biothings.utils.dataload import update_dict_recur
from biothings.utils.jsondiff import make as jsondiff
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager, BaseStatusRegisterer
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocMongoBackend
from biothings import config as btconfig
from biothings.utils.hub import publish_data_version, template_out
from biothings.hub.databuild.backend import generate_folder, create_backend, \
                                            merge_src_build_metadata
from biothings.hub.dataindex.indexer import ESIndexerException
from biothings.hub import RELEASEMANAGER_CATEGORY, SNAPSHOOTER_CATEGORY, RELEASER_CATEGORY
from biothings.hub.datarelease.releasenote import ReleaseNoteTxt
from biothings.hub.datarelease import set_pending_to_publish

# default from config
logging = btconfig.logger


class PublisherException(Exception): pass


class BasePublisher(BaseManager,BaseStatusRegisterer):

    def __init__(self, envconf, log_folder, es_backups_folder, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.envconf = envconf
        self.log_folder = log_folder
        self.es_backups_folder = es_backups_folder

    @property
    def category(self):
        raise NotImplementedError()

    @property
    def collection(self):
        return get_src_build()

    def setup(self):
        self.setup_log()
    
    def setup_log(self):
        self.logger, self.logfile = get_logger(self.category,self.log_folder)

    def get_predicates(self):
        return []
    
    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {"category" : self.category,
                "source" : "",
                "step" : "",
                "description" : ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def register_status(self, bdoc, status,transient=False,init=False,**extra):
        super().register_status(bdoc,"publish",status,transient=transient,init=init,**extra)

    def load_build(self, key_name, stage=None):
        if stage is None:
            # picke build doc searching for snapshot then diff key if not found
            return self.load_doc(key_name,"snapshot") or self.load_doc(key_name,"diff")
        else:
            return self.load_doc(key_name,stage)

    def create_bucket(self, bucket_conf, credentials):
        aws_key = credentials.get("access_key")
        aws_secret = credentials.get("secret_key")
        # create bucket if needed
        aws.create_bucket(
                name=bucket_conf["bucket"],
                region=bucket_conf["region"],
                aws_key=aws_key,aws_secret=aws_secret,
                acl=bucket_conf.get("acl"),
                ignore_already_exists=True)

    def trigger_release_note(self,doc,**kwargs):
        """
        Launch a release note generation given a src_build document. In order to 
        know the first collection to compare with, get_previous_collection()
        method is used. release_note() method will get **kwargs for more optional
        parameters.
        """
        new_db_col_names = doc["_id"]
        old_db_col_names = get_previous_collection(new_db_col_names)
        self.release_note(old_db_col_names, new_db_col_names, **kwargs)

    def run_pre_post(self, key, stage, snapshot_name, repo_conf, build_doc):
        """
        Run pre- and post- steps for given stage (eg. "snapshot", "publish").
        These steps are defined in config file.
        """
        print(build_doc.keys())
        index_meta = build_doc["_meta"]
        # start pipeline with repo config from "snapshot" step
        repo_name = list(build_doc["snapshot"][snapshot_name]["repository"].keys())[0]
        previous_result = build_doc["snapshot"][snapshot_name]["repository"][repo_name]
        steps = repo_conf[key].get(stage,[])
        assert isinstance(steps,list), "'%s' stage must be a list, got: %s" % (stage,repr(steps))
        action_done = []
        for step_conf in steps:
            try:
                action = step_conf["action"]
                self.logger.info("Processing stage '%s-%s': %s" % (stage,key,action))
                # first try user-defined methods
                # it can be named (first to match is picked):
                # see list below
                found = False
                for tpl in [
                        "step_%(stage)s_%(key)s_%(action)s",
                        "step_%(key)s_%(action)s",
                        "step_%(stage)s_%(action)s"]:
                    methname = tpl % {"stage" : stage,
                                      "key" : key,
                                      "action" : action}
                    if hasattr(self,methname):
                        found = True
                        previous_result = getattr(self,methname)(step_conf,build_doc,previous_result)
                        break
                if not found:
                    # default to generic one
                    previous_result = getattr(self,"step_%s" % action)(step_conf,build_doc,previous_result)

                action_done.append({"name" : action, "result" : previous_result})

            except AttributeError as e:
                raise ValueError("No such %s-%s step '%s'" % (stage,key,action))

        return action_done

    def step_archive(self, step_conf, build_doc, previous):
        index_meta = build_doc["_meta"]
        archive_file = os.path.join(self.es_backups_folder,template_out(step_conf["name"],index_meta))
        if step_conf["format"] == "tar.xz":
            # -J is for "xz"
            tarcmd = ["tar",
                      "cJf",
                      archive_file, # could be replaced if "split"
                      "-C",
                      self.es_backups_folder,
                      previous["settings"]["location"],
                      ]
            if step_conf.get("split"):
                part = "%s.part." % archive_file
                tarcmd[2] = "-"
                tarps = subprocess.Popen(tarcmd,stdout=subprocess.PIPE)
                splitcmd = ["split",
                            "-",
                            "-b",
                            step_conf["split"],
                            "-d",
                            part]
                ps = subprocess.Popen(splitcmd,stdin=tarps.stdin)
                ret_code = ps.wait()
                if ret_code != 0:
                    raise PublisherException("Archiving failed, code: %s" % ret_code)
                else:
                    flist = glob.glob("%s.*" % part)
                    if len(flist) == 1:
                        # no even split, go back to single archive file
                        os.rename(flist[0],archive_file)
                        self.logger.info("Tried to split archive, but only one part was produced," + \
                                         "returning single archive file: %s" % archive_file)
                        return archive_file
                    else:
                        # produce a json file with metadata about the splits
                        jsonfile = "%s.json" % outfile
                        json.dump({"filename" : outfile,"parts" : flist},
                                  open(jsonfile,"w"))
                        self.logger.info("Archive split into %d parts, metadata stored in: %s" % (len(flist),jsonfile))
                        return jsonfile
            else:
                out = subprocess.check_output(tarcmd)
                self.logger.info("Archive: %s" % archive_file)
                return archive_file
        else:
            raise ValueError("Only 'tar.xz' format supported for now, got %s" % repr(step_conf["format"]))

    def step_upload(self, step_conf, build_doc, previous):
        if step_conf["type"] == "s3":
            return self.step_upload_s3(step_conf,build_doc,previous)
        else:
            raise ValueError("Only 's3' upload type supported for now, got %s" % repr(step_conf["type"]))

    def step_upload_s3(self, step_conf, build_doc, previous):
        index_meta = build_doc["_meta"]
        aws_key=self.envconf.get("cloud",{}).get("access_key")
        aws_secret=self.envconf.get("cloud",{}).get("secret_key")
        # template out for special values
        for k in step_conf:
            v = step_conf[k]
            step_conf[k] = template_out(step_conf[k],index_meta)
        # create bucket if needed
        self.create_bucket(bucket_conf=step_conf,credentials=self.envconf.get("cloud",{}))
        if step_conf.get("file"):
            basename = template_out(step_conf["file"],index_meta)
            uploadfunc = aws.send_s3_big_file
        elif step_conf.get("folder"):
            basename = template_out(step_conf["folder"],index_meta)
            uploadfunc = aws.send_s3_folder
        else:
            raise ValueError("Can't find 'file' or 'folder' key, don't know what to upload")
        archive_path = os.path.join(self.es_backups_folder,basename)
        self.logger.info("Uploading: %s" % archive_path)
        uploadfunc(archive_path,
                os.path.join(step_conf["base_path"],basename),
                overwrite=step_conf.get("overwrite",False),
                aws_key=aws_key,
                aws_secret=aws_secret,
                s3_bucket=step_conf["bucket"])
        return {"type" : "s3",
                "key" : basename,
                "bucket" : step_conf["bucket"]}


class SnapshotPublisher(BasePublisher):

    def __init__(self, snapshot_manager, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.snapshot_manager = snapshot_manager
        self.setup()

    @property
    def category(self):
        return RELEASER_CATEGORY

    def run_pre_publish_snapshot(self, snapshot_name, repo_conf, build_doc):
        return self.run_pre_post("publish","pre",snapshot_name,repo_conf,build_doc)

    def publish(self, snapshot, build_name=None, previous_build=None, steps=["pre","meta","post"]):
        """
        Publish snapshot metadata to S3. If snapshot repository is of type "s3", data isn't actually
        uploaded/published since it's already there on s3. If type "fs", some "pre" steps can be added
        to the RELEASE_CONFIG paramater to archive and upload it to s3. Metadata about the snapshot,
        release note, etc... is then uploaded in correct buckets as defined in config, and "post"
        steps can be run afterward.

        Though snapshots don't need any previous version to be applied on, a release note with significant changes
        between current snapshot and a previous version could have been generated. By default, snapshot name is
        used to pick one single build document and from the document, get the release note information.
        """
        try:
            if build_name:
                bdoc = self.load_build(build_name)
            else:
                bdoc = self.load_build(snapshot,"snapshot")
        except Exception as e:
            self.exception("Error loading build document using snapshot named '%s': %s" % (snapshot,e))
            raise
        if isinstance(bdoc,list):
            raise PublisherException("Snapshot '%s' found in more than one builds: %s." % (snapshot,
                [d["_id"] for d in bdoc]) + " Specify which one with 'build_name'")
        if type(steps) == str:
            steps = [steps]
        if not bdoc:
            raise PublisherException("No build document found with a snapshot name '%s' associated to it" % snapshot)

        # check if a release note is associated to the build document
        release_folder = None
        if previous_build is None and "release_note" in bdoc:
            previous_build = list(bdoc["release_note"].keys())
            if len(previous_build) != 1:
                raise PublisherException("More than one release note found, " + \
                        "generated with following builds: %s" % previous_build)
            else:
                previous_build = previous_build.pop()

            assert previous_build, "Couldn't find previous build %s" % bdoc.keys()
            release_folder = generate_folder(btconfig.RELEASE_PATH,previous_build,bdoc["_id"])

        s3_release_folder = self.envconf["release"]["folder"]
        s3_release_bucket = self.envconf["release"]["bucket"]
        self.create_bucket(bucket_conf=self.envconf["release"],credentials=self.envconf.get("cloud",{}))

        @asyncio.coroutine
        def do():
            jobs = []
            pinfo = self.get_pinfo()
            pinfo["step"] = "publish"
            pinfo["source"] = snapshot
            if "pre" in steps:
                # then we upload all the folder content
                pinfo["step"] = "pre"
                self.logger.info("Running pre-publish step")
                job = yield from self.job_manager.defer_to_thread(pinfo,partial(
                                            self.pre_publish,
                                            snapshot,
                                            self.envconf,
                                            bdoc))
                yield from job
                jobs.append(job)
            if "meta" in steps:
                # TODO: this is a blocking call
                # snapshot at this point can be totally different than original
                # target_name but we still use it to potentially custom indexed
                # (anyway, it's just to access some snapshot info so default indexer 
                # will work)
                if not "_meta" in bdoc:
                    raise PublisherException("No metadata (_meta) found in build document")
                build_version = bdoc["_meta"]["build_version"]
                self.logger.info("Generating JSON metadata for full release '%s'" % build_version)
                release_note = "release_%s" % build_version
                # generate json metadata about this diff release
                assert snapshot, "Missing snapshot name information"
                if getattr(btconfig,"SKIP_CHECK_VERSIONS",None):
                    self.logger.info("SKIP_CHECK_VERSIONS %s, no version check will be performed on diff metadata" % repr(btconfig.SKIP_CHECK_VERSION))
                else:
                    assert getattr(btconfig,"BIOTHINGS_VERSION","master") != "master", "I won't publish data refering BIOTHINGS_VERSION='master'"
                    assert getattr(btconfig,"APP_VERSION","master") != "master", "I won't publish data refering APP_VERSION='master'"
                    assert getattr(btconfig,"STANDALONE_VERSION",None), "STANDALONE_VERSION not defined"
                full_meta = {
                        "type": "full",
                        "build_version": build_version,
                        "target_version": build_version,
                        "release_date" : datetime.now().isoformat(),
                        "app_version": btconfig.APP_VERSION,
                        "biothings_version": btconfig.BIOTHINGS_VERSION,
                        "standalone_version": btconfig.STANDALONE_VERSION,
                        "metadata" : {"repository" : bdoc["snapshot"][snapshot]["repository"]}
                        }
                if release_folder and os.path.exists(release_folder):
                    # ok, we have something in that folder, just pick the release note files
                    # (we can generate diff + snaphost at the same time, so there could be diff files in that folder
                    # from a diff process done before. release notes will be the same though)
                    s3basedir = os.path.join(s3_release_folder,build_version)
                    notes = glob.glob(os.path.join(release_folder,"%s.*" % release_note))
                    self.logger.info("Uploading release notes from '%s' to s3 folder '%s'" % (notes,s3basedir))
                    for note in notes:
                        if os.path.exists(note):
                            s3key = os.path.join(s3basedir,os.path.basename(note))
                            aws.send_s3_file(note,s3key,
                                    aws_key=self.envconf.get("cloud",{}).get("access_key"),
                                    aws_secret=self.envconf.get("cloud",{}).get("secret_key"),
                                    s3_bucket=s3_release_bucket,
                                    overwrite=True)
                    # specify release note URLs in metadata
                    rel_txt_url = aws.get_s3_url(os.path.join(s3basedir,"%s.txt" % release_note),
                                    aws_key=self.envconf.get("cloud",{}).get("access_key"),
                                    aws_secret=self.envconf.get("cloud",{}).get("secret_key"),
                                    s3_bucket=s3_release_bucket)
                    rel_json_url = aws.get_s3_url(os.path.join(s3basedir,"%s.json" % release_note),
                                    aws_key=self.envconf.get("cloud",{}).get("access_key"),
                                    aws_secret=self.envconf.get("cloud",{}).get("secret_key"),
                                    s3_bucket=s3_release_bucket)
                    if rel_txt_url:
                        full_meta.setdefault("changes",{})
                        full_meta["changes"]["txt"] = {"url" : rel_txt_url}
                    if rel_json_url:
                        full_meta.setdefault("changes",{})
                        full_meta["changes"]["json"] = {"url" : rel_json_url}
                else:
                    self.logger.info("No release_folder found, no release notes will be part of the publishing")
                    # yet create the folder so we can dump metadata json file in there later
                    os.makedirs(release_folder)

                # now dump that metadata
                build_info = "%s.json" % build_version
                build_info_path = os.path.join(btconfig.RELEASE_PATH,build_info)
                json.dump(full_meta,open(build_info_path,"w"))
                # override lastmodified header with our own timestamp
                local_ts = dtparse(bdoc["_meta"]["build_date"])
                utc_epoch = int(time.mktime(local_ts.timetuple()))
                utc_ts = datetime.fromtimestamp(time.mktime(time.gmtime(utc_epoch)))
                str_utc_epoch = str(utc_epoch)
                # it's a full release, but all build info metadata (full, incremental) all go
                # to the diff bucket (this is the main entry)
                s3key = os.path.join(s3_release_folder,build_info)
                aws.send_s3_file(build_info_path,s3key,
                        aws_key=self.envconf.get("cloud",{}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",{}).get("secret_key"),
                        s3_bucket=s3_release_bucket,
                        metadata={"lastmodified":str_utc_epoch},
                        overwrite=True)
                url = aws.get_s3_url(s3key,
                        aws_key=self.envconf.get("cloud",{}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",{}).get("secret_key"),
                        s3_bucket=s3_release_bucket)
                self.logger.info("Full release metadata published for version: '%s'" % url)
                full_info = {"build_version":full_meta["build_version"],
                        "require_version":None,
                        "target_version":full_meta["target_version"],
                        "type":full_meta["type"],
                        "release_date":full_meta["release_date"],
                        "url":url}
                publish_data_version(s3_release_bucket,s3_release_folder,full_info,
                        aws_key=self.envconf.get("cloud",{}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",{}).get("secret_key"))
                self.logger.info("Registered version '%s'" % (build_version))

            if "post" in steps:
                # then we upload all the folder content
                pinfo["step"] = "post"
                self.logger.info("Runnig post-publish step")
                job = yield from self.job_manager.defer_to_thread(pinfo,partial(
                                            self.post_publish,
                                            snapshot,
                                            self.envconf,
                                            bdoc))
                yield from job
                jobs.append(job)

            def published(f):
                try:
                    res = f.result()
                    self.logger.info("Snapshot '%s' uploaded to S3: %s" % (snapshot,res),extra={"notify":True})
                except Exception as e:
                    self.logger.exception("Failed to upload snapshot '%s' uploaded to S3: %s" % (snapshot,e),extra={"notify":True})

            if jobs:
                yield from asyncio.wait(jobs)
                task = asyncio.gather(*jobs)
                task.add_done_callback(published)
                yield from task

        return asyncio.ensure_future(do())

    def run_post_publish_snapshot(self, snapshot_name, repo_conf, build_doc):
        return self.run_pre_post("publish","post",snapshot_name,repo_conf,build_doc)

    def post_publish(self, snapshot_name, repo_conf, build_doc):
        """
        Post-publish hook, running steps declared in config,
        but also whatever would be defined in a sub-class
        """
        return self.run_post_publish_snapshot(snapshot_name,repo_conf,build_doc)

    def pre_publish(self, snapshot_name, repo_conf, build_doc):
        """
        Pre-publish hook, running steps declared in config,
        but also whatever would be defined in a sub-class
        """
        return self.run_pre_publish_snapshot(snapshot_name,repo_conf,build_doc)


class DiffPublisher(BasePublisher):

    def reset_synced(self,diff_folder,backend=None):
        """
        Remove "synced" flag from any pyobj file in diff_folder
        """
        synced_files = glob.glob(os.path.join(diff_folder,"*.pyobj.synced"))
        for synced in synced_files:
            diff_file = re.sub("\.pyobj\.synced$",".pyobj",synced)
            os.rename(synced,diff_file)

    def publish_diff(self, s3_folder, old_db_col_names=None, new_db_col_names=None,
            diff_folder=None, release_folder=None, steps=["reset","upload","meta","post"], s3_bucket=None):
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
            pinfo = self.get_pinfo()
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
                    if getattr(btconfig,"SKIP_CHECK_VERSIONS",None):
                        self.logger.info("SKIP_CHECK_VERSIONS %s, no version check will be performed on diff metadata" % repr(btconfig.SKIP_CHECK_VERSIONS))
                    else:
                        assert getattr(btconfig,"BIOTHINGS_VERSION","master") != "master", "I won't publish data refering BIOTHINGS_VERSION='master'"
                        assert getattr(btconfig,"APP_VERSION","master") != "master", "I won't publish data refering APP_VERSION='master'"
                        assert getattr(btconfig,"STANDALONE_VERSION",None), "STANDALONE_VERSION not defined"
                    diff_meta = {
                            "type": "incremental",
                            "build_version": diff_version,
                            "require_version": meta["old"]["version"],
                            "target_version": meta["new"]["version"],
                            "release_date" : datetime.now().isoformat(),
                            "app_version": btconfig.APP_VERSION,
                            "biothings_version": btconfig.BIOTHINGS_VERSION,
                            "standalone_version": btconfig.STANDALONE_VERSION,
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

            if "post" in steps:
                # then we upload all the folder content
                pinfo["step"] = "post"
                self.logger.info("Running post-publish step")
                job = yield from self.job_manager.defer_to_thread(pinfo,partial(self.post_publish,
                            s3_folder=s3_folder, old_db_col_names=old_db_col_names,
                            new_db_col_names=new_db_col_names, diff_folder=diff_folder,
                            release_folder=release_folder, steps=steps, s3_bucket=s3_bucket))
                yield from job
                jobs.append(job)

            def uploaded(f):
                try:
                    res = f.result()
                    self.logger.info("Diff folder '%s' uploaded to S3: %s" % (diff_folder,res),extra={"notify":True})
                except Exception as e:
                    self.logger.exception("Failed to upload diff folder '%s' uploaded to S3: %s" % (diff_folder,e),extra={"notify":True})

            yield from asyncio.wait(jobs)
            task = asyncio.gather(*jobs)
            task.add_done_callback(uploaded)
            yield from task

        return asyncio.ensure_future(do())

    def post_publish(self):#, s3_folder, old_db_col_names, new_db_col_names, diff_folder, release_folder,
                     #steps, s3_bucket, *args, **kwargs):
        """Post-publish hook, can be implemented in sub-class"""
        return


class ReleaseManager(BaseManager, BaseStatusRegisterer):

    DEFAULT_SNAPSHOT_PUBLISHER_CLASS = SnapshotPublisher
    DEFAULT_DIFF_PUBLISHER_CLASS = DiffPublisher

    def __init__(self, diff_manager, snapshot_manager, poll_schedule=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.diff_manager = diff_manager
        self.snapshot_manager = snapshot_manager
        self.t0 = time.time()
        self.log_folder = btconfig.LOG_FOLDER
        self.timestamp = datetime.now()
        self.release_config = {}
        self.poll_schedule = poll_schedule
        self.es_backups_folder = btconfig.ES_BACKUPS_FOLDER
        self.setup()

    def setup(self):
        self.setup_log()
    
    def setup_log(self):
        self.logger, self.logfile = get_logger(RELEASEMANAGER_CATEGORY,self.log_folder)
    
    def poll(self,state,func):
        super().poll(state,func,col=get_src_build())
    
    def __getitem__(self,stage_env):
        """
        Return an instance of a releaser for the release environment named "env"
        """
        stage,env = stage_env
        kwargs = copy.copy(self.register[stage_env])
        klass = kwargs.pop("class")
        return klass(**kwargs)

    def configure(self, release_confdict):
        """
        Configure manager with release "confdict". See config_hub.py in API
        for the format.
        """
        self.release_config = copy.deepcopy(release_confdict)
        for env, envconf in self.release_config.get("env",{}).items():
            try:
                if envconf.get("cloud"):
                    assert envconf["cloud"]["type"] == "aws", \
                            "Only Amazon AWS cloud is supported at the moment"
                # here we register publisher class and args passed during init
                # which are common to the manager
                self.register[("snapshot",env)] = {
                        "class" : self.DEFAULT_SNAPSHOT_PUBLISHER_CLASS,
                        "envconf" : envconf,
                        "job_manager" : self.job_manager,
                        "snapshot_manager" : self.snapshot_manager,
                        "log_folder" : self.log_folder,
                        "es_backups_folder" : self.es_backups_folder,
                        }
                self.register[("diff",env)] = {
                        "class" : self.DEFAULT_DIFF_PUBLISHER_CLASS,
                        "conf" : envconf,
                        "job_manager" : self.job_manager,
                        "diff_manager" : self.diff_manager,
                        "log_folder" : self.log_folder,
                        }
            except Exception as e:
                self.logger.exception("Couldn't setup release environment '%s' because: %s" % (env,e))

    def get_predicates(self):
        return []
    
    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {"category" : RELEASEMANAGER_CATEGORY,
                "source" : "",
                "step" : "",
                "description" : ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    @property
    def collection(self):
        return get_src_build()
    
    def register_status(self, bdoc, stage, status,transient=False,init=False,**extra):
        super().register_status(bdoc,stage,status,transient=transient,init=init,**extra)

    def load_build(self, key_name, stage=None):
        if stage is None:
            # picke build doc searching for snapshot then diff key if not found
            return self.load_doc(key_name,"snapshot") or self.load_doc(key_name,"diff")
        else:
            return self.load_doc(key_name,stage)

    def publish_diff(self, s3_folder, old_db_col_names=None, new_db_col_names=None,
            diff_folder=None, release_folder=None, steps=["reset","upload","meta","post"], s3_bucket=None):
        raise NotImplementedError("publish_diff")

    def publish_snapshot(self, publisher_env, snapshot, build_name=None, previous_build=None, steps=["pre","meta","post"]):
        if not publisher_env in self.release_config.get("env",{}):
            raise ValueError("Unknonw release environment '%s'" % publisher_env)
        publisher = self[("snapshot",publisher_env)]
        return publisher.publish(snapshot=snapshot, build_name=build_name, previous_build=previous_build, steps=steps)

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
        bdoc = self.load_build(new)
        old = old or get_previous_collection(new)
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
            return {"txt" : txt, "changes" : changes}

        @asyncio.coroutine
        def main(release_folder):
            got_error = False
            pinfo = self.get_pinfo()
            pinfo["step"] = "release_note"
            pinfo["source"] = release_folder
            pinfo["description"] = filename
            self.register_status(bdoc,"release_note","generating",transient=True,init=True,
                                 job={"step":"release_note"},release_note={old:{}})
            job = yield from self.job_manager.defer_to_thread(pinfo,do)
            def reported(f):
                nonlocal got_error
                try:
                    res = f.result()
                    self.register_status(
                            bdoc,"release_note","success",
                            job={"step":"release_note"},
                            release_note={
                                old:{"changes" : res["changes"]}
                                }
                            )
                    self.logger.info("Release note ready, saved in %s: %s" % (release_folder,res["txt"]),extra={"notify":True})
                    set_pending_to_publish(new)
                except Exception as e:
                    self.logger.exception(e)
                    got_error = e
            job.add_done_callback(reported)
            yield from job
            if got_error:
                self.logger.exception("Failed to create release note: %s" % got_error,extra={"notify":True})
                self.register_status(bdoc,"release_note","failed",job={"step":"release_note","err":str(e)},
                                     release_note={old:{}})
                raise got_error

        job = asyncio.ensure_future(main(release_folder))
        return job

    def build_release_note(self, old_colname, new_colname, note=None):
        """
        Build a release note containing most significant changes between build names "old_colname" and "new_colname".
        An optional end note can be added to bring more specific information about the release.

        Return a dictionnary containing significant changes.
        """
        def get_counts(dstats):
            stats = {}
            for subsrc,count in dstats.items():
                try:
                    src_sub = get_source_fullname(subsrc).split(".")
                except AttributeError:
                    # not a merge stats coming from a source
                    # (could be custom field stats, eg. total_* in mygene)
                    src_sub = [subsrc]
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

        diff_folder = generate_folder(btconfig.DIFF_PATH,old_colname,new_colname)
        try:
            metafile = os.path.join(diff_folder,"metadata.json")
            metadata = json.load(open(metafile))
            old_colname = metadata["old"]["backend"]
            new_colname = metadata["new"]["backend"]
            diff_stats = metadata["diff"]["stats"]
        except FileNotFoundError:
            # we're generating a release note without diff information
            self.logger.info("No metadata.json file found, this release note won't have diff stats included")
            diff_stats = {}

        new = create_backend(new_colname)
        old = create_backend(old_colname)
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
                "stats" : {
                    "added" : {},
                    "deleted" : {},
                    "updated" : {},
                    },
                "note" : note,
                "generated_on": str(datetime.now()),
                "sources" : {
                    "added" : {},
                    "deleted" : {},
                    "updated" : {},
                    }
                }
        # for later use
        new_versions = get_versions(new_doc)
        old_versions = get_versions(old_doc)
        # now deal with stats/counts. Counts are related to uploader, ie. sub-sources
        new_merge_stats = get_counts(new_doc.get("merge_stats",{}))
        old_merge_stats = get_counts(old_doc.get("merge_stats",{}))
        new_stats = get_counts(new_doc.get("_meta",{}).get("stats",{}))
        old_stats = get_counts(old_doc.get("_meta",{}).get("stats",{}))
        new_info = update_dict_recur(new_versions,new_merge_stats)
        old_info = update_dict_recur(old_versions,old_merge_stats)

        def analyze_diff(ops,destdict,old,new):
            for op in ops:
                # get main source / main field
                key = op["path"].strip("/").split("/")[0]
                if op["op"] == "add":
                    destdict["added"][key] = new[key]
                elif op["op"] == "remove":
                    destdict["deleted"][key] = old[key]
                elif op["op"] == "replace":
                    destdict["updated"][key] = {
                            "new" : new[key],
                            "old" : old[key]}
                else:
                    raise ValueError("Unknown operation '%s' while computing changes" % op["op"])

        # diff source info
        # this only works on main source information: if there's a difference in a
        # sub-source, it won't be shown but considered as if it was the main-source
        ops = jsondiff(old_info,new_info)
        analyze_diff(ops,changes["sources"],old_info,new_info)

        ops = jsondiff(old_stats,new_stats)
        analyze_diff(ops,changes["stats"],old_stats,new_stats)

        # mapping diff: we re-compute them and don't use any mapping.pyobj because that file
        # only allows "add" operation as a safety rule (can't delete fields in ES mapping once indexed)
        ops = jsondiff(old_doc.get("mapping",{}),new_doc["mapping"])
        for op in ops:
            changes["new"]["_fields"].setdefault(op["op"],[]).append(op["path"].strip("/").replace("/","."))

        return changes

