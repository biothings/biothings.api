import re
import os
import time
import glob
import copy
from datetime import datetime
from dateutil.parser import parse as dtparse
import json
import asyncio
from functools import partial
import subprocess

from biothings.utils.mongo import get_previous_collection, get_target_db
from biothings.utils.hub_db import get_src_build, get_source_fullname
import biothings.utils.aws as aws
from biothings.utils.dataload import update_dict_recur
from biothings.utils.jsondiff import make as jsondiff
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager, BaseStatusRegisterer
from biothings.utils.backend import DocMongoBackend
from biothings import config as btconfig
from biothings.utils.hub import publish_data_version, template_out
from biothings.hub.databuild.backend import generate_folder, create_backend
from biothings.hub import RELEASEMANAGER_CATEGORY, RELEASER_CATEGORY
from biothings.hub.datarelease.releasenote import ReleaseNoteTxt
from biothings.hub.datarelease import set_pending_to_publish
from biothings.hub.databuild.buildconfig import AutoBuildConfig
# default from config
logging = btconfig.logger


class PublisherException(Exception):
    pass


class BasePublisher(BaseManager, BaseStatusRegisterer):
    def __init__(self, envconf, log_folder, es_backups_folder, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.envconf = envconf
        self.log_folder = log_folder
        self.es_backups_folder = es_backups_folder
        self.ti = time.time()
        self.setup()

    def clean_stale_status(self):
        src_build = get_src_build()
        for build in src_build.find():
            dirty = False
            for job in build.get("jobs", []):
                if job.get("status", "").endswith("publishing"):
                    logging.warning(
                        "Found stale build '%s', marking publish status as 'canceled'"
                        % build["_id"])
                    job["status"] = "canceled"
                    dirty = True
            if dirty:
                src_build.replace_one({"_id": build["_id"]}, build)

    @property
    def category(self):
        return RELEASER_CATEGORY

    @property
    def collection(self):
        return get_src_build()

    def setup(self):
        self.setup_log()

    def setup_log(self, build_name=None):
        log_folder = self.log_folder
        if build_name:
            log_folder = os.path.join(btconfig.LOG_FOLDER, "build", build_name)
        self.logger, self.logfile = get_logger(self.category, log_folder, force=True)

    def get_predicates(self):
        return []

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": self.category,
            "source": "",
            "step": "",
            "description": ""
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def register_status(self,
                        bdoc,
                        status,
                        transient=False,
                        init=False,
                        **extra):
        BaseStatusRegisterer.register_status(self,
                                             bdoc,
                                             "publish",
                                             status,
                                             transient=transient,
                                             init=init,
                                             **extra)

    def load_build(self, key_name, stage=None):
        if stage is None:
            # picke build doc searching for snapshot then diff key if not found
            return self.load_doc(key_name, "snapshot") or self.load_doc(
                key_name, "diff")
        else:
            return self.load_doc(key_name, stage)

    def template_out_conf(self, build_doc):
        # don't bother exploring certain keys, convert the whole a string
        # and replace whatever we find
        try:
            strconf = template_out(json.dumps(self.envconf), build_doc)
            return json.loads(strconf)
        except Exception as e:
            self.logger.exception("Coudn't template out configuration: %s" % e)
            raise

    def create_bucket(self, bucket_conf, credentials):
        aws_key = credentials.get("access_key")
        aws_secret = credentials.get("secret_key")
        # create bucket if needed
        aws.create_bucket(name=bucket_conf["bucket"],
                          region=bucket_conf["region"],
                          aws_key=aws_key,
                          aws_secret=aws_secret,
                          acl=bucket_conf.get("acl"),
                          ignore_already_exists=True)
        if bucket_conf.get("website"):
            aws.set_static_website(bucket_conf["bucket"],
                                   aws_key=aws_key,
                                   aws_secret=aws_secret)

    def trigger_release_note(self, doc, **kwargs):
        """
        Launch a release note generation given a src_build document. In order to
        know the first collection to compare with, get_previous_collection()
        method is used. release_note() method will get **kwargs for more optional
        parameters.
        """
        new_db_col_names = doc["_id"]
        old_db_col_names = get_previous_collection(new_db_col_names)
        self.release_note(old_db_col_names, new_db_col_names, **kwargs)

    def get_pre_post_previous_result(self, build_doc, key_value):
        """
        In order to start a pre- or post- pipeline, a first previous result, fed
        all along the pipeline to the next step, has to be defined, and depends
        on the type of publisher.
        """
        raise NotImplementedError("implement me in sub-class")

    def run_pre_post(self, key, stage, key_value, repo_conf, build_doc):
        """
        Run pre- and post- publish steps (stage) for given key (eg. "snapshot", "diff").
        key_value is the value of the key inside "key" dict (such as a snapshot name or a build name)
        These steps are defined in config file.
        """
        # determine previous result as the starting point of the pipeline,
        # depending on the type of publishing (key)
        previous_result = self.get_pre_post_previous_result(
            build_doc, key_value)
        steps = repo_conf.get("publish", {}).get(stage, {}).get(
            key, [])  # publish[pre|post][snapshot|diff]
        assert isinstance(
            steps,
            list), "'%s' stage must be a list, got: %s" % (stage, repr(steps))
        action_done = []
        for step_conf in steps:
            try:
                action = step_conf["action"]
                self.logger.info("Processing stage '%s-%s-publish': %s" %
                                 (stage, key, action))
                # first try user-defined methods
                # it can be named (first to match is picked):
                # see list below
                found = False
                for tpl in [
                        "step_%(stage)s_publish_%(action)s",
                        "step_publish_%(action)s", "step_%(stage)s_%(action)s"
                ]:
                    methname = tpl % {"stage": stage, "action": action}
                    if hasattr(self, methname):
                        found = True
                        previous_result = getattr(self,
                                                  methname)(step_conf,
                                                            build_doc,
                                                            previous_result)
                        break
                if not found:
                    # default to generic one
                    previous_result = getattr(self, "step_%s" % action)(
                        step_conf, build_doc, previous_result)

                action_done.append({"name": action, "result": previous_result})

            except AttributeError:
                raise ValueError("No such %s-%s-publish step '%s'" %
                                 (stage, key, action))

        return action_done

    def step_archive(self, step_conf, build_doc, previous):
        archive_name = step_conf["name"]
        archive_file = os.path.join(self.es_backups_folder, archive_name)
        if step_conf["format"] == "tar.xz":
            # -J is for "xz"
            tarcmd = [
                "tar",
                "cJf",
                archive_file,  # could be replaced if "split"
                "-C",
                self.es_backups_folder,
                previous["settings"]["location"],
            ]
            if step_conf.get("split"):
                part = "%s.part." % archive_file
                tarcmd[2] = "-"
                tarps = subprocess.Popen(tarcmd, stdout=subprocess.PIPE)
                splitcmd = ["split", "-", "-b", step_conf["split"], "-d", part]
                ps = subprocess.Popen(splitcmd, stdin=tarps.stdin)
                ret_code = ps.wait()
                if ret_code != 0:
                    raise PublisherException("Archiving failed, code: %s" %
                                             ret_code)
                else:
                    flist = glob.glob("%s.*" % part)
                    if len(flist) == 1:
                        # no even split, go back to single archive file
                        os.rename(flist[0], archive_file)
                        self.logger.info("Tried to split archive, but only one part was produced,"
                                         + "returning single archive file: %s" % archive_file)
                        return archive_file
                    else:
                        # produce a json file with metadata about the splits
                        jsonfile = "%s.json" % outfile
                        json.dump({
                            "filename": outfile,
                            "parts": flist
                        }, open(jsonfile, "w"))
                        self.logger.info(
                            "Archive split into %d parts, metadata stored in: %s"
                            % (len(flist), jsonfile))
                        return jsonfile
            else:
                subprocess.check_output(tarcmd)
                self.logger.info("Archive: %s" % archive_file)
                return archive_file
        else:
            raise ValueError("Only 'tar.xz' format supported for now, got %s" %
                             repr(step_conf["format"]))

    def step_upload(self, step_conf, build_doc, previous):
        if step_conf["type"] == "s3":
            return self.step_upload_s3(step_conf, build_doc, previous)
        else:
            raise ValueError(
                "Only 's3' upload type supported for now, got %s" %
                repr(step_conf["type"]))

    def step_upload_s3(self, step_conf, build_doc, previous):
        aws_key = self.envconf.get("cloud", {}).get("access_key")
        aws_secret = self.envconf.get("cloud", {}).get("secret_key")
        # create bucket if needed
        self.create_bucket(bucket_conf=step_conf,
                           credentials=self.envconf.get("cloud", {}))
        if step_conf.get("file"):
            basename = step_conf["file"]
            uploadfunc = aws.send_s3_big_file
        elif step_conf.get("folder"):
            basename = step_conf["folder"]
            uploadfunc = aws.send_s3_folder
        else:
            raise ValueError(
                "Can't find 'file' or 'folder' key, don't know what to upload")
        archive_path = os.path.join(self.es_backups_folder, basename)
        self.logger.info("Uploading: %s" % archive_path)
        uploadfunc(archive_path,
                   os.path.join(step_conf["base_path"], basename),
                   overwrite=step_conf.get("overwrite", False),
                   aws_key=aws_key,
                   aws_secret=aws_secret,
                   s3_bucket=step_conf["bucket"])
        return {
            "type": "s3",
            "key": basename,
            "base_path": step_conf["base_path"],
            "bucket": step_conf["bucket"]
        }

    def get_release_note_filename(self, build_version):
        return "release_%s" % build_version

    def publish_release_notes(self,
                              release_folder,
                              build_version,
                              s3_release_folder,
                              s3_release_bucket,
                              aws_key,
                              aws_secret,
                              prefix="release_"):
        release_note = self.get_release_note_filename(build_version)
        s3basedir = os.path.join(s3_release_folder, build_version)
        notes = glob.glob(os.path.join(release_folder, "%s.*" % release_note))
        self.logger.info(
            "Uploading release notes from '%s' to s3 folder '%s'" %
            (notes, s3basedir))
        for note in notes:
            if os.path.exists(note):
                s3key = os.path.join(s3basedir, os.path.basename(note))
                aws.send_s3_file(
                    note,
                    s3key,
                    aws_key=self.envconf.get("cloud", {}).get("access_key"),
                    aws_secret=self.envconf.get("cloud", {}).get("secret_key"),
                    s3_bucket=s3_release_bucket,
                    overwrite=True)
        # specify release note URLs in metadata
        rel_txt_url = aws.get_s3_url(
            os.path.join(s3basedir, "%s.txt" % release_note),
            aws_key=self.envconf.get("cloud", {}).get("access_key"),
            aws_secret=self.envconf.get("cloud", {}).get("secret_key"),
            s3_bucket=s3_release_bucket)
        rel_json_url = aws.get_s3_url(
            os.path.join(s3basedir, "%s.json" % release_note),
            aws_key=self.envconf.get("cloud", {}).get("access_key"),
            aws_secret=self.envconf.get("cloud", {}).get("secret_key"),
            s3_bucket=s3_release_bucket)
        urls = {}
        if rel_txt_url:
            urls["txt"] = {"url": rel_txt_url}
        if rel_json_url:
            urls["json"] = {"url": rel_json_url}

        return urls


class SnapshotPublisher(BasePublisher):
    def __init__(self, snapshot_manager, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snapshot_manager = snapshot_manager
        self.setup()

    def get_pre_post_previous_result(self, build_doc, key_value):
        assert build_doc["snapshot"][key_value], "previous step not successful"
        assert build_doc["snapshot"][key_value]["created_at"]
        previous_result = build_doc["snapshot"][key_value]["conf"]["repository"]
        return previous_result

    def run_pre_publish_snapshot(self, snapshot_name, repo_conf, build_doc):
        return self.run_pre_post("snapshot", "pre", snapshot_name, repo_conf, build_doc)

    def publish(self,
                snapshot,
                build_name=None,
                previous_build=None,
                steps=["pre", "meta", "post"]):
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
                bdoc = self.load_build(snapshot, "snapshot")
        except Exception as e:
            self.exception(
                "Error loading build document using snapshot named '%s': %s" %
                (snapshot, e))
            raise

        self.setup_log(bdoc["_id"])

        if isinstance(bdoc, list):
            raise PublisherException(
                "Snapshot '%s' found in more than one builds: %s." %
                (snapshot, [d["_id"] for d in bdoc])
                + " Specify which one with 'build_name'")
        if type(steps) == str:
            steps = [steps]
        if not bdoc:
            raise PublisherException(
                "No build document found with a snapshot name '%s' associated to it"
                % snapshot)

        # instantiate publishing environment
        self.envconf = self.template_out_conf(bdoc)

        # check if a release note is associated to the build document
        release_folder = None
        if previous_build is None and "release_note" in bdoc:
            previous_build = list(bdoc["release_note"].keys())
            if len(previous_build) != 1:
                raise PublisherException("More than one release note found, "
                                         + "generated with following builds: %s" % previous_build)
            else:
                previous_build = previous_build.pop()

        assert previous_build, "Couldn't find previous build %s" % bdoc.keys()
        release_folder = generate_folder(btconfig.RELEASE_PATH, previous_build,
                                         bdoc["_id"])

        assert release_folder, "No release folder found, can't publish"

        s3_release_folder = self.envconf["release"]["folder"]
        s3_release_bucket = self.envconf["release"]["bucket"]
        self.create_bucket(bucket_conf=self.envconf["release"],
                           credentials=self.envconf.get("cloud", {}))

        # hold error/exception on each step
        got_error = None

        async def do():
            jobs = []
            pinfo = self.get_pinfo()
            pinfo["step"] = "publish"
            pinfo["source"] = snapshot

            def done(f, step):
                try:
                    res = f.result()
                    self.register_status(bdoc,
                                         "success",
                                         job={
                                             "step": step,
                                             "result": res
                                         },
                                         publish={
                                             "full": {
                                                 snapshot: {
                                                     "conf": self.envconf,
                                                     step: res
                                                 }
                                             }
                                         })
                except Exception as e:
                    nonlocal got_error
                    got_error = e
                    self.register_status(bdoc,
                                         "failed",
                                         job={
                                             "step": step,
                                             "err": str(e)
                                         },
                                         publish={
                                             "full": {
                                                 snapshot: {
                                                     "conf": self.envconf,
                                                     step: {
                                                         "err": str(e)
                                                     }
                                                 }
                                             }
                                         })
                    self.logger.exception(
                        "Error while running pre-publish: %s" % got_error)

            if "_meta" not in bdoc:
                raise PublisherException(
                    "No metadata (_meta) found in build document")

            if "pre" in steps:
                # then we upload all the folder content
                pinfo["step"] = "pre"
                self.logger.info("Running pre-publish step")
                self.register_status(bdoc,
                                     "pre",
                                     transient=True,
                                     init=True,
                                     job={"step": "pre"},
                                     publish={"full": {
                                         snapshot: {}
                                     }})
                job = await self.job_manager.defer_to_thread(
                    pinfo,
                    partial(self.pre_publish, snapshot, self.envconf, bdoc))
                job.add_done_callback(partial(done, step="pre"))
                await job
                if got_error:
                    raise got_error
                jobs.append(job)

            if "meta" in steps:
                # TODO: this is a blocking call
                # snapshot at this point can be totally different than original
                # target_name but we still use it to potentially custom indexed
                # (anyway, it's just to access some snapshot info so default indexer
                # will work)
                build_version = bdoc["_meta"]["build_version"]
                self.logger.info(
                    "Generating JSON metadata for full release '%s'" %
                    build_version)
                # generate json metadata about this diff release
                assert snapshot, "Missing snapshot name information"
                if getattr(btconfig, "SKIP_CHECK_VERSIONS", None):
                    self.logger.info(
                        "SKIP_CHECK_VERSIONS %s, no version check will be performed on full metadata"
                        % repr(btconfig.SKIP_CHECK_VERSIONS))
                else:
                    assert getattr(
                        btconfig, "BIOTHINGS_VERSION", "master"
                    ) != "master", "I won't publish data refering BIOTHINGS_VERSION='master'"
                    assert getattr(
                        btconfig, "APP_VERSION", "master"
                    ) != "master", "I won't publish data refering APP_VERSION='master'"
                    assert getattr(btconfig, "STANDALONE_VERSION",
                                   None), "STANDALONE_VERSION not defined"
                full_meta = {
                    "type": "full",
                    "build_version": build_version,
                    "target_version": build_version,
                    "release_date": datetime.now().astimezone().isoformat(),
                    "app_version": btconfig.APP_VERSION,
                    "biothings_version": btconfig.BIOTHINGS_VERSION,
                    "standalone_version": btconfig.STANDALONE_VERSION,
                    "metadata": {
                        "repository": bdoc["snapshot"][snapshot]["conf"]["repository"],
                        "snapshot_name": snapshot,
                    }
                }
                # if snapshot tyoe is "fs" (so it means it's stored locally) and we publish (so it means we want it to
                # be available remotely) it means we should have an pre-"upload" step in the publish pipeline
                # let's try to get the archive url
                if bdoc["snapshot"][snapshot]["conf"]["repository"]["type"] == "fs":
                    pre_steps = bdoc.get("publish",
                                         {}).get("full",
                                                 {}).get(snapshot,
                                                         {}).get("pre", [])
                    try:
                        assert pre_steps, "No pre-steps found, expecting pre-upload step"
                        upload_step = [
                            step for step in pre_steps
                            if step["name"] == "upload"
                        ]
                        assert len(
                            upload_step
                        ) == 1, "Expecting one pre-upload step, got %s" % repr(
                            upload_step)
                        upload_step = upload_step.pop()
                        res = upload_step["result"]
                        assert res[
                            "type"] == "s3", "Only archived uploaded to S3 are currently supported"
                        url = aws.get_s3_url(
                            s3key=os.path.join(res["base_path"], res["key"]),
                            aws_key=self.envconf.get("cloud",
                                                     {}).get("access_key"),
                            aws_secret=self.envconf.get("cloud",
                                                        {}).get("secret_key"),
                            s3_bucket=res["bucket"])
                        full_meta["metadata"]["archive_url"] = url
                    except Exception as e:
                        raise PublisherException("Repository for snapshot '%s' is type 'fs' but " % snapshot
                                                 + "coudln't determine archive URL to publish: %s" % e)

                if release_folder:
                    if os.path.exists(release_folder):
                        try:
                            self.register_status(
                                bdoc,
                                "publishing",
                                transient=True,
                                init=True,
                                job={"step": "release-note"},
                                publish={"full": {
                                    snapshot: {}
                                }})
                            # ok, we have something in that folder, just pick the release note files
                            # (we can generate diff + snaphost at the same time, so there could be diff files in that folder
                            # from a diff process done before. release notes will be the same though)
                            urls = self.publish_release_notes(
                                release_folder,
                                build_version,
                                s3_release_folder,
                                s3_release_bucket,
                                aws_key=self.envconf.get("cloud",
                                                         {}).get("access_key"),
                                aws_secret=self.envconf.get(
                                    "cloud", {}).get("secret_key"))
                            full_meta.setdefault("changes", {})
                            full_meta["changes"].update(urls)
                            s3basedir = os.path.join(s3_release_folder,
                                                     build_version)
                            self.register_status(
                                bdoc,
                                "success",
                                job={"step": "release-note"},
                                publish={
                                    "full": {
                                        snapshot: {
                                            "conf": self.envconf,
                                            "release-note": {
                                                "base_dir": s3basedir,
                                                "bucket": s3_release_bucket,
                                                "url": urls
                                            }
                                        }
                                    }
                                })
                        except Exception as e:
                            self.logger.exception(
                                "Failed to upload release notes: %s" % e)
                            self.register_status(
                                bdoc,
                                "failed",
                                job={
                                    "step": "release-note",
                                    "err": str(e)
                                },
                                publish={
                                    "full": {
                                        snapshot: {
                                            "conf": self.envconf,
                                            "release-note": {
                                                "err": str(e),
                                                # TODO: set value to s3basedir in case it not defined before
                                                "base_dir": s3basedir,
                                                "bucket": s3_release_bucket
                                            },
                                        }
                                    }
                                })
                            raise

                    else:
                        self.logger.info(
                            "No release_folder found, no release notes will be part of the publishing"
                        )
                        # yet create the folder so we can dump metadata json file in there later
                        os.makedirs(release_folder)

                try:
                    self.register_status(bdoc,
                                         "publishing",
                                         transient=True,
                                         init=True,
                                         job={"step": "metadata"},
                                         publish={"full": {
                                             snapshot: {}
                                         }})
                    # now dump that metadata
                    build_info = "%s.json" % build_version
                    build_info_path = os.path.join(btconfig.RELEASE_PATH,
                                                   build_info)
                    json.dump(full_meta, open(build_info_path, "w"))
                    # override lastmodified header with our own timestamp
                    local_ts = dtparse(bdoc["_meta"]["build_date"])
                    utc_epoch = int(time.mktime(local_ts.timetuple()))
                    str_utc_epoch = str(utc_epoch)
                    # it's a full release, but all build info metadata (full, incremental) all go
                    # to the diff bucket (this is the main entry)
                    s3key = os.path.join(s3_release_folder, build_info)
                    aws.send_s3_file(
                        build_info_path,
                        s3key,
                        aws_key=self.envconf.get("cloud",
                                                 {}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",
                                                    {}).get("secret_key"),
                        s3_bucket=s3_release_bucket,
                        metadata={"lastmodified": str_utc_epoch},
                        overwrite=True)
                    url = aws.get_s3_url(
                        s3key,
                        aws_key=self.envconf.get("cloud",
                                                 {}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",
                                                    {}).get("secret_key"),
                        s3_bucket=s3_release_bucket)
                    self.logger.info(
                        "Full release metadata published for version: '%s'" %
                        url)
                    full_info = {
                        "build_version": full_meta["build_version"],
                        "require_version": None,
                        "target_version": full_meta["target_version"],
                        "type": full_meta["type"],
                        "release_date": full_meta["release_date"],
                        "url": url
                    }
                    publish_data_version(
                        s3_release_bucket,
                        s3_release_folder,
                        full_info,
                        aws_key=self.envconf.get("cloud",
                                                 {}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",
                                                    {}).get("secret_key"))
                    self.logger.info("Registered version '%s'" %
                                     (build_version))
                    self.register_status(bdoc,
                                         "success",
                                         job={"step": "metadata"},
                                         publish={
                                             "full": {
                                                 snapshot: {
                                                     "conf": self.envconf,
                                                     "metadata": full_info
                                                 }
                                             }
                                         })
                except Exception as e:
                    self.logger.exception(
                        "Failed to upload snapshot metadata: %s" % e)
                    self.register_status(bdoc,
                                         "failed",
                                         job={
                                             "step": "metadata",
                                             "err": str(e)
                                         },
                                         publish={
                                             "full": {
                                                 snapshot: {
                                                     "conf": self.envconf,
                                                     "metadata": {
                                                         "err": str(e)
                                                     }
                                                 }
                                             }
                                         })
                    raise

            if "post" in steps:
                pinfo["step"] = "post"
                self.logger.info("Running post-publish step")
                self.register_status(bdoc,
                                     "post-publishing",
                                     transient=True,
                                     init=True,
                                     job={"step": "post-publish"},
                                     publish={"fulle": {
                                         snapshot: {}
                                     }})
                job = await self.job_manager.defer_to_thread(
                    pinfo,
                    partial(self.post_publish, snapshot, self.envconf, bdoc))
                job.add_done_callback(partial(done, step="post"))
                await job
                jobs.append(job)

            def published(f):
                try:
                    res = f.result()
                    self.logger.info("Snapshot '%s' uploaded to S3: %s" %
                                     (snapshot, res),
                                     extra={"notify": True})
                except Exception as e:
                    self.logger.exception(
                        "Failed to upload snapshot '%s' uploaded to S3: %s" %
                        (snapshot, e),
                        extra={"notify": True})

            if jobs:
                await asyncio.wait(jobs)
                task = asyncio.gather(*jobs)
                task.add_done_callback(published)
                await task

        def done(f):
            try:
                _ = f.result()
            except Exception as e:
                self.logger.exception("Unable to publish full release: %s" % e)
                raise

        task = asyncio.ensure_future(do())
        task.add_done_callback(done)

        return task

    def run_post_publish_snapshot(self, snapshot_name, repo_conf, build_doc):
        return self.run_pre_post("snapshot", "post", snapshot_name, repo_conf,
                                 build_doc)

    def post_publish(self, snapshot_name, repo_conf, build_doc):
        """
        Post-publish hook, running steps declared in config,
        but also whatever would be defined in a sub-class
        """
        return self.run_post_publish_snapshot(snapshot_name, repo_conf,
                                              build_doc)

    def pre_publish(self, snapshot_name, repo_conf, build_doc):
        """
        Pre-publish hook, running steps declared in config,
        but also whatever would be defined in a sub-class
        """
        return self.run_pre_publish_snapshot(snapshot_name, repo_conf,
                                             build_doc)


class DiffPublisher(BasePublisher):
    def __init__(self, diff_manager, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.diff_manager = diff_manager
        self.setup()

    def get_pre_post_previous_result(self, build_doc, key_value):
        previous_result = {
            "diff": build_doc["diff"][key_value]["diff"],
            "diff_folder": build_doc["diff"][key_value]["diff_folder"]
        }
        return previous_result

    def run_pre_publish_diff(self, previous_build_name, repo_conf, build_doc):
        return self.run_pre_post("diff", "pre", previous_build_name, repo_conf,
                                 build_doc)

    def pre_publish(self, previous_build_name, repo_conf, build_doc):
        """
        Pre-publish hook, running steps declared in config,
        but also whatever would be defined in a sub-class
        """
        return self.run_pre_publish_diff(previous_build_name, repo_conf,
                                         build_doc)

    def reset_synced(self, diff_folder, backend=None):
        """
        Remove "synced" flag from any pyobj file in diff_folder
        """
        synced_files = glob.glob(os.path.join(diff_folder, "*.pyobj.synced"))
        for synced in synced_files:
            diff_file = re.sub(r"\.pyobj\.synced$", ".pyobj", synced)
            os.rename(synced, diff_file)

    def get_release_note_filename(self, build_version):
        assert "." in build_version  # make sure it's an incremental
        _, tgt = build_version.split(".")
        return "release_%s" % tgt

    def publish(self,
                build_name,
                previous_build=None,
                steps=["pre", "reset", "upload", "meta", "post"]):
        """
        Publish diff files and metadata about the diff files, release note, etc... on s3.
        Using build_name, a src_build document is fetched, and a diff release is searched. If more
        than one diff release is found, "previous_build" must be specified to pick the correct one.
        - steps:
          * pre/post: optional steps processed as first and last steps.
          * reset: highly recommended, reset synced flag in diff files so they won't get skipped when used...
          * upload: upload diff_folder content to S3
          * meta: publish/register the version as available for auto-updating hubs
        """
        bdoc = self.load_build(build_name)

        self.setup_log(bdoc["_id"])

        assert bdoc, "No such build named '%s'" % build_name
        assert "diff" in bdoc, "No diff release found in build document named '%s'" % build_name
        if previous_build is None:
            dkeys = list(bdoc["diff"].keys())
            assert len(dkeys) == 1, "'previous_build' parameter is required because " \
                                    + "more than one diff release found: %s" % dkeys
            previous_build = dkeys.pop()
        assert previous_build, "No previous build could be found in order to pick correct diff release"
        # check what to do
        if type(steps) == str:
            steps = [steps]

        # instantiate publishing environment
        self.envconf = self.template_out_conf(bdoc)

        s3_release_folder = self.envconf["release"]["folder"]
        s3_release_bucket = self.envconf["release"]["bucket"]
        self.create_bucket(bucket_conf=self.envconf["release"],
                           credentials=self.envconf.get("cloud", {}))
        s3_diff_folder = self.envconf["diff"]["folder"]
        s3_diff_bucket = self.envconf["diff"]["bucket"]
        self.create_bucket(bucket_conf=self.envconf["diff"],
                           credentials=self.envconf.get("cloud", {}))

        # check whether a release note was generated for that (build_name,previous_build) couple
        release_folder = None
        if bdoc.get("release_note", {}).get(previous_build, {}):
            release_folder = bdoc["release_note"][previous_build][
                "release_folder"]

        diff_folder = bdoc["diff"][previous_build]["diff_folder"]
        try:
            meta = json.load(open(os.path.join(diff_folder, "metadata.json")))
        except FileNotFoundError:
            raise FileNotFoundError("metadata.json is missing")

        diff_version = meta["diff"]["version"]
        s3_diff_basedir = os.path.join(s3_diff_folder, diff_version)

        # hold error/exception on each step
        got_error = None

        async def do():
            jobs = []
            pinfo = self.get_pinfo()
            pinfo["source"] = diff_folder
            pinfo["description"] = diff_version

            def done(f, step):
                try:
                    res = f.result()
                    self.register_status(bdoc,
                                         "success",
                                         job={
                                             "step": step,
                                             "result": res
                                         },
                                         publish={
                                             "incremental": {
                                                 previous_build: {
                                                     "conf": self.envconf,
                                                     step: res
                                                 }
                                             }
                                         })
                except Exception as e:
                    nonlocal got_error
                    got_error = e
                    self.register_status(bdoc,
                                         "failed",
                                         job={
                                             "step": step,
                                             "err": str(e)
                                         },
                                         publish={
                                             "incremental": {
                                                 previous_build: {
                                                     "conf": self.envconf,
                                                     step: {
                                                         "err": str(e)
                                                     }
                                                 }
                                             }
                                         })
                    self.logger.exception(
                        "Error while running %s-publish: %s" %
                        (step, got_error))

            if "_meta" not in bdoc:
                raise PublisherException("No metadata (_meta) found in build document")

            if "pre" in steps:
                # then we upload all the folder content
                pinfo["step"] = "pre"
                self.logger.info("Running pre-publish step")
                self.register_status(
                    bdoc,
                    "pre",
                    transient=True,
                    init=True,
                    job={"step": "pre"},
                    publish={"incremental": {
                        previous_build: {}
                    }})
                job = await self.job_manager.defer_to_thread(
                    pinfo,
                    partial(self.pre_publish, previous_build, self.envconf,
                            bdoc))
                job.add_done_callback(partial(done, step="pre"))
                await job
                if got_error:
                    raise got_error
                jobs.append(job)

            if "reset" in steps:
                # first we need to reset "synced" flag in diff files to make
                # sure all of them will be applied by client
                pinfo["step"] = "reset synced"
                self.logger.info(
                    "Resetting 'synced' flag in pyobj files located in folder '%s'"
                    % diff_folder)
                self.register_status(
                    bdoc,
                    "reset-synced",
                    transient=True,
                    init=True,
                    job={"step": "reset-synced"},
                    publish={"incremental": {
                        previous_build: {}
                    }})
                job = await self.job_manager.defer_to_thread(
                    pinfo, partial(self.reset_synced, diff_folder))
                job.add_done_callback(partial(done, step="reset"))
                await job
                if got_error:
                    raise got_error
                jobs.append(job)

            if "upload" in steps:
                # then we upload all the folder content
                pinfo["step"] = "upload"
                self.logger.info(
                    "Uploading files from '%s' to s3 (%s/%s)" %
                    (diff_folder, s3_diff_bucket, s3_diff_basedir))
                self.register_status(
                    bdoc,
                    "upload",
                    transient=True,
                    init=True,
                    job={"step": "upload"},
                    publish={"incremental": {
                        previous_build: {}
                    }})
                job = await self.job_manager.defer_to_thread(
                    pinfo,
                    partial(aws.send_s3_folder,
                            diff_folder,
                            s3basedir=s3_diff_basedir,
                            aws_key=self.envconf.get("cloud",
                                                     {}).get("access_key"),
                            aws_secret=self.envconf.get("cloud",
                                                        {}).get("secret_key"),
                            s3_bucket=s3_diff_bucket,
                            overwrite=True))
                job.add_done_callback(partial(done, step="upload"))
                await job
                jobs.append(job)

            if "meta" in steps:
                # finally we create a metadata json file pointing to this release
                try:
                    self.register_status(
                        bdoc,
                        "metadata",
                        transient=True,
                        init=True,
                        job={"step": "metadata"},
                        publish={"incremental": {
                            previous_build: {}
                        }})
                    pinfo["step"] = "generate meta"
                    self.logger.info(
                        "Generating JSON metadata for incremental release '%s'"
                        % diff_version)
                    # if the same, this would create an infinite loop in autoupdate hub
                    # (X requires X, where to find X ? there, but X requires X, where to find X ?...)
                    if meta["old"]["version"] == meta["new"]["version"]:
                        raise PublisherException("Required version is the same as target version "
                                                 + "('%s'), prevent publishing to avoid infinite loop " % meta["new"]["version"]
                                                 + "while resolving updates in auto-update hub")
                    # generate json metadata about this diff release
                    if getattr(btconfig, "SKIP_CHECK_VERSIONS", None):
                        self.logger.info(
                            "SKIP_CHECK_VERSIONS %s, no version check will be performed on diff metadata"
                            % repr(btconfig.SKIP_CHECK_VERSIONS))
                    else:
                        assert getattr(
                            btconfig, "BIOTHINGS_VERSION", "master"
                        ) != "master", "I won't publish data refering BIOTHINGS_VERSION='master'"
                        assert getattr(
                            btconfig, "APP_VERSION", "master"
                        ) != "master", "I won't publish data refering APP_VERSION='master'"
                        assert getattr(btconfig, "STANDALONE_VERSION",
                                       None), "STANDALONE_VERSION not defined"
                    diff_meta = {
                        "type": "incremental",
                        "build_version": diff_version,
                        "require_version": meta["old"]["version"],
                        "target_version": meta["new"]["version"],
                        "release_date": datetime.now().astimezone().isoformat(),
                        "app_version": btconfig.APP_VERSION,
                        "biothings_version": btconfig.BIOTHINGS_VERSION,
                        "standalone_version": btconfig.STANDALONE_VERSION,
                        "metadata": {
                            "url":
                            aws.get_s3_url(
                                os.path.join(s3_diff_basedir, "metadata.json"),
                                aws_key=self.envconf.get("cloud",
                                                         {}).get("access_key"),
                                aws_secret=self.envconf.get(
                                    "cloud", {}).get("secret_key"),
                                s3_bucket=s3_diff_bucket)
                        },
                    }
                    # upload release notes
                    if release_folder:
                        urls = self.publish_release_notes(
                            release_folder,
                            diff_version,
                            s3_release_folder,
                            s3_release_bucket,
                            aws_key=self.envconf.get("cloud",
                                                     {}).get("access_key"),
                            aws_secret=self.envconf.get("cloud",
                                                        {}).get("secret_key"))
                        diff_meta.setdefault("changes", {})
                        diff_meta["changes"].update(urls)
                    # and then upload diff metadataf files
                    diff_file = "%s.json" % diff_version
                    diff_meta_path = os.path.join(btconfig.RELEASE_PATH,
                                                  diff_file)
                    json.dump(diff_meta,
                              open(diff_meta_path, "w"),
                              indent=True)
                    # get a timestamp from metadata to force lastdmodifed header
                    # timestamp is when the new collection was built (not when the diff
                    # was generated, as diff can be generated way after). New collection's
                    # timestamp remains a good choice as data (diff) relates to that date anyway
                    local_ts = dtparse(diff_meta["release_date"])
                    utc_epoch = int(time.mktime(local_ts.timetuple()))
                    str_utc_epoch = str(utc_epoch)
                    s3key = os.path.join(s3_release_folder, diff_file)
                    aws.send_s3_file(
                        diff_meta_path,
                        s3key,
                        aws_key=self.envconf.get("cloud",
                                                 {}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",
                                                    {}).get("secret_key"),
                        s3_bucket=s3_release_bucket,
                        metadata={"lastmodified": str_utc_epoch},
                        overwrite=True)
                    url = aws.get_s3_url(
                        s3key,
                        aws_key=self.envconf.get("cloud",
                                                 {}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",
                                                    {}).get("secret_key"),
                        s3_bucket=s3_release_bucket)
                    self.logger.info(
                        "Incremental release metadata published for version: '%s'"
                        % url)
                    diff_info = {
                        "build_version": diff_meta["build_version"],
                        "require_version": diff_meta["require_version"],
                        "target_version": diff_meta["target_version"],
                        "type": diff_meta["type"],
                        "release_date": diff_meta["release_date"],
                        "url": url
                    }
                    publish_data_version(
                        s3_release_bucket,
                        s3_release_folder,
                        diff_info,
                        aws_key=self.envconf.get("cloud",
                                                 {}).get("access_key"),
                        aws_secret=self.envconf.get("cloud",
                                                    {}).get("secret_key"))
                    self.logger.info("Registered version '%s'" %
                                     (diff_version))

                    self.register_status(bdoc,
                                         "success",
                                         job={"step": "metadata"},
                                         publish={
                                             "incremental": {
                                                 previous_build: {
                                                     "conf": self.envconf,
                                                     "metadata": diff_info
                                                 }
                                             }
                                         })

                except Exception as e:
                    self.logger.exception(
                        "Failed to upload snapshot metadata: %s" % e)
                    self.register_status(bdoc,
                                         "failed",
                                         job={
                                             "step": "metadata",
                                             "err": str(e)
                                         },
                                         publish={
                                             "incremental": {
                                                 previous_build: {
                                                     "conf": self.envconf,
                                                     "metadata": {
                                                         "err": str(e)
                                                     }
                                                 }
                                             }
                                         })
                    raise

                jobs.append(job)

            if "post" in steps:
                pinfo["step"] = "post"
                self.logger.info("Running post-publish step")
                self.register_status(
                    bdoc,
                    "post",
                    transient=True,
                    init=True,
                    job={"step": "post"},
                    publish={"incremental": {
                        previous_build: {}
                    }})
                job = await self.job_manager.defer_to_thread(
                    pinfo,
                    partial(self.post_publish, previous_build, self.envconf,
                            bdoc))
                job.add_done_callback(partial(done, step="post"))
                await job
                if got_error:
                    raise got_error
                jobs.append(job)

            def uploaded(f):
                try:
                    res = f.result()
                    self.logger.info("Diff folder '%s' uploaded to S3: %s" %
                                     (diff_folder, res),
                                     extra={"notify": True})
                except Exception as e:
                    self.logger.exception(
                        "Failed to upload diff folder '%s' uploaded to S3: %s"
                        % (diff_folder, e),
                        extra={"notify": True})

            await asyncio.wait(jobs)
            task = asyncio.gather(*jobs)
            task.add_done_callback(uploaded)
            await task

        def done(f):
            try:
                _ = f.result()
            except Exception as e:
                self.logger.exception("Unable to publish incremental release: %s" % e)
                raise

        task = asyncio.ensure_future(do())
        task.add_done_callback(done)

        return task

    def run_post_publish_diff(self, build_name, repo_conf, build_doc):
        return self.run_pre_post("diff", "post", build_name, repo_conf,
                                 build_doc)

    def post_publish(self, build_name, repo_conf, build_doc):
        """
        Post-publish hook, running steps declared in config,
        but also whatever would be defined in a sub-class
        """
        return self.run_post_publish_diff(build_name, repo_conf, build_doc)


class ReleaseManager(BaseManager, BaseStatusRegisterer):

    DEFAULT_SNAPSHOT_PUBLISHER_CLASS = SnapshotPublisher
    DEFAULT_DIFF_PUBLISHER_CLASS = DiffPublisher

    def __init__(self,
                 diff_manager,
                 snapshot_manager,
                 poll_schedule=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.diff_manager = diff_manager
        self.snapshot_manager = snapshot_manager
        self.t0 = time.time()
        self.log_folder = btconfig.LOG_FOLDER
        self.timestamp = datetime.now()
        self.release_config = {}
        self.poll_schedule = poll_schedule
        self.es_backups_folder = getattr(btconfig, "ES_BACKUPS_FOLDER", None)
        self.setup()

    def clean_stale_status(self):
        src_build = get_src_build()
        for build in src_build.find():
            dirty = False
            for job in build.get("jobs", []):
                if job.get("status") == "generating":
                    logging.warning(
                        "Found stale build '%s', marking release-note status as 'canceled'"
                        % build["_id"])
                    job["status"] = "canceled"
                    dirty = True
            if dirty:
                src_build.replace_one({"_id": build["_id"]}, build)

    def setup(self):
        self.setup_log()

    def setup_log(self, build_name=None):
        name = RELEASEMANAGER_CATEGORY
        log_folder = self.log_folder
        if build_name:
            log_folder = os.path.join(btconfig.LOG_FOLDER, "build", build_name)
        self.logger, self.logfile = get_logger(name, log_folder=log_folder, force=True)

    def poll(self, state, func):
        super().poll(state, func, col=get_src_build())

    def __getitem__(self, stage_env):
        """
        Return an instance of a releaser for the release environment named "env"
        """
        stage, env = stage_env
        kwargs = copy.copy(self.register[stage_env])
        klass = kwargs.pop("class")
        return klass(**kwargs)

    def configure(self, release_confdict):
        """
        Configure manager with release "confdict". See config_hub.py in API
        for the format.
        """
        self.release_config = copy.deepcopy(release_confdict)
        for env, envconf in self.release_config.get("env", {}).items():
            try:
                if envconf.get("cloud"):
                    assert envconf["cloud"]["type"] == "aws", \
                        "Only Amazon AWS cloud is supported at the moment"
                # here we register publisher class and args passed during init
                # which are common to the manager
                self.register[("snapshot", env)] = {
                    "class": self.DEFAULT_SNAPSHOT_PUBLISHER_CLASS,
                    "envconf": envconf,
                    "job_manager": self.job_manager,
                    "snapshot_manager": self.snapshot_manager,
                    "log_folder": self.log_folder,
                    "es_backups_folder": self.es_backups_folder,
                }
                self.register[("diff", env)] = {
                    "class": self.DEFAULT_DIFF_PUBLISHER_CLASS,
                    "envconf": envconf,
                    "job_manager": self.job_manager,
                    "diff_manager": self.diff_manager,
                    "log_folder": self.log_folder,
                    "es_backups_folder": self.es_backups_folder,
                }
            except Exception as e:
                self.logger.exception(
                    "Couldn't setup release environment '%s' because: %s" %
                    (env, e))

    def get_predicates(self):
        return []

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": RELEASEMANAGER_CATEGORY,
            "source": "",
            "step": "",
            "description": ""
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    @property
    def collection(self):
        return get_src_build()

    def register_status(self,
                        bdoc,
                        stage,
                        status,
                        transient=False,
                        init=False,
                        **extra):
        BaseStatusRegisterer.register_status(self,
                                             bdoc,
                                             stage,
                                             status,
                                             transient=transient,
                                             init=init,
                                             **extra)

    def reset_synced(self, old, new):
        """
        Reset sync flags for diff files produced between "old" and "new" build.
        Once a diff has been applied, diff files are flagged as synced so subsequent diff
        won't be applied twice (for optimization reasons, not to avoid data corruption since
        diff files can be safely applied multiple times).
        In any needs to apply the diff another time, diff files needs to reset.
        """
        # we need a diff publisher to do that, whatever the target (s3, filesystem,...)
        dtypes = [pub for pub in self.register if pub[0] == "diff"]
        assert len(
            dtypes
        ), "Can't reset synced diff files, no diff publisher registered"
        # get one, all are able to reset synced files
        dtype = dtypes[0]
        diff_folder = generate_folder(btconfig.DIFF_PATH, old, new)
        diff_publisher = self[dtype]
        return diff_publisher.reset_synced(diff_folder)

    def load_build(self, key_name, stage=None):
        if stage is None:
            # picke build doc searching for snapshot then diff key if not found
            return self.load_doc(key_name, "snapshot") or self.load_doc(
                key_name, "diff")
        else:
            return self.load_doc(key_name, stage)

    def publish_diff(self,
                     publisher_env,
                     build_name,
                     previous_build=None,
                     steps=["pre", "reset", "upload", "meta", "post"]):
        if publisher_env not in self.release_config.get("env", {}):
            raise ValueError("Unknonw release environment '%s'" %
                             publisher_env)
        publisher = self[("diff", publisher_env)]
        return publisher.publish(build_name=build_name,
                                 previous_build=previous_build,
                                 steps=steps)

    def publish_snapshot(self,
                         publisher_env,
                         snapshot,
                         build_name=None,
                         previous_build=None,
                         steps=["pre", "meta", "post"]):
        if publisher_env not in self.release_config.get("env", {}):
            raise ValueError("Unknonw release environment '%s'" %
                             publisher_env)
        publisher = self[("snapshot", publisher_env)]
        return publisher.publish(snapshot=snapshot,
                                 build_name=build_name,
                                 previous_build=previous_build,
                                 steps=steps)

    def publish(self, publisher_env, snapshot_or_build_name, *args, **kwargs):

        if not publisher_env:
            build_doc = self.load_build(snapshot_or_build_name)
            build_conf = build_doc['build_config']
            try:
                publisher_env = build_conf['autopublish']['env']
            except KeyError:
                raise PublisherException("Cannot infer publish environment.")

        snapshot_doc = None
        diff_doc = None
        try:
            snapshot_doc = self.load_doc(snapshot_or_build_name, "snapshot")
            if isinstance(snapshot_doc, list):
                raise PublisherException("More than one build_doc associated to snapshot '%s', " % snapshot_or_build_name + "use publish_snapshot()")
        except AssertionError:
            # no doc at all
            pass
        try:
            diff_doc = self.load_doc(snapshot_or_build_name, "diff")
            if isinstance(diff_doc, list):
                raise PublisherException("More than one build_doc associated to diff '%s', " % snapshot_or_build_name + "use explicetely publish_diff()")
        except AssertionError:
            # no doc at all
            pass
        # check returned doc contains what we think it does and load_doc hasn't use snapshot_or_build_name as _id
        if snapshot_doc and snapshot_or_build_name not in snapshot_doc.get(
                "snapshot", {}):
            # doc was returend with snapshot_or_build_name matching _id, not snapshot name, invalidate
            snapshot_doc = None
        # TODO
        # diff is wrt another diff, maybe should be this way?
        #--------------------------------------
        # if diff_doc and snapshot_or_build_name not in diff_doc.get("diff", {}):
        if diff_doc and not diff_doc.get("diff", {}):
            diff_doc = None
        #--------------------------------------
        # do we still have something ambiguous ?
        if snapshot_doc and diff_doc:
            # so we have 2 releases associated, we can't know which one user wants
            raise PublisherException("'%s' is associated to 2 different releases " % snapshot_or_build_name
                                     + "(document _id '%s' and '%s'" % (snapshot_doc["_id"], diff_doc["_id"])
                                     + "use publish_snapshot() or publish_diff()")
        elif snapshot_doc:
            self.logger.info("'%s' associated to a snapshot/full release" %
                             snapshot_or_build_name)
            return self.publish_snapshot(publisher_env, snapshot_or_build_name,
                                         *args, **kwargs)
        elif diff_doc:
            self.logger.info("'%s' associated to a diff/incremental release" %
                             snapshot_or_build_name)
            return self.publish_diff(publisher_env, snapshot_or_build_name,
                                     *args, **kwargs)
        else:
            raise PublisherException("No release associated to '%s'" %
                                     snapshot_or_build_name)

    def publish_build(self, build_doc):
        build_conf = AutoBuildConfig(build_doc['build_config'])
        if build_conf.should_publish_new_diff() or build_conf.should_publish_new_snapshot():
            # differentiate TODO
            logging.info("Publish new build.")
            self.publish(None, build_doc["_id"])
            # if build_conf.should_install_new_release():
            #     logging.info("Should install new release next.")
            #     # TODO

    def get_release_note(self, old, new, format="txt", prefix="release_*"):
        release_folder = generate_folder(btconfig.RELEASE_PATH, old, new)
        if not os.path.exists(release_folder):
            raise PublisherException("No release note folder found")
        notes = glob.glob(
            os.path.join(release_folder, "%s.%s" % (prefix, format)))
        if not notes:
            raise PublisherException("No release notes found in folder")
        if len(notes) != 1:
            raise PublisherException(
                "Found %d notes (%s), expected only one" %
                (len(notes), [os.path.basename(n) for n in notes]))
        content = open(notes[0]).read()
        if format == "json":
            content = json.loads(content)
        return content

    def create_release_note_from_build(self, build_doc):
        async def _():
            if build_doc.get("release_note"):
                self.logger.info(
                    "Not a brand-new build. "
                    "Skip release note automation.")
                return
            try:
                old = get_previous_collection(build_doc["_id"])
            except AssertionError:
                self.logger.warning(
                    "Cannot find the previous build. "
                    "Create a fresh release note.")
                old = "none"

            if old == build_doc["_id"]:
                self.logger.error(
                    "Error finding the previous build. "
                    "Skip release note automation. ")
                return
            await self.create_release_note(old=old, new=build_doc["_id"])

            build_conf = AutoBuildConfig(build_doc['build_config'])
            if build_conf.should_publish_new_diff() or build_conf.should_publish_new_snapshot():
                # TODO differentiate at some level
                logging.info("Set pending publish for %s.", build_doc['_id'])
                set_pending_to_publish(build_doc['_id'])
        return asyncio.ensure_future(_())

    def create_release_note(self,
                            old,
                            new,
                            filename=None,
                            note=None,
                            format="txt"):
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

        self.setup_log(bdoc["_id"])

        old = old or get_previous_collection(new)
        release_folder = generate_folder(btconfig.RELEASE_PATH, old, new)
        if not os.path.exists(release_folder):
            os.makedirs(release_folder)
        filepath = None

        def do():
            changes = self.build_release_note(old, new, note=note)
            nonlocal filepath
            nonlocal filename
            assert format == "txt", "Only 'txt' format supported for now"
            filename = filename or "release_%s.%s" % (
                changes["new"]["_version"], format)
            filepath = os.path.join(release_folder, filename)
            render = ReleaseNoteTxt(changes)
            txt = render.save(filepath)
            filename = filename.replace(".%s" % format, ".json")
            filepath = os.path.join(release_folder, filename)
            json.dump(changes, open(filepath, "w"), indent=True)
            return {"txt": txt, "changes": changes}

        async def main(release_folder):
            got_error = False
            pinfo = self.get_pinfo()
            pinfo["step"] = "release_note"
            pinfo["source"] = release_folder
            pinfo["description"] = filename
            self.register_status(bdoc,
                                 "release_note",
                                 "generating",
                                 transient=True,
                                 init=True,
                                 job={"step": "release_note"},
                                 release_note={old: {}})
            job = await self.job_manager.defer_to_thread(pinfo, do)

            def reported(f):
                nonlocal got_error
                try:
                    res = f.result()
                    self.register_status(bdoc,
                                         "release_note",
                                         "success",
                                         job={"step": "release_note"},
                                         release_note={
                                             old: {
                                                 "changes": res["changes"],
                                                 "release_folder":
                                                 release_folder
                                             }
                                         })
                    self.logger.info("Release note ready, saved in %s: %s" %
                                     (release_folder, res["txt"]),
                                     extra={"notify": True})
                    set_pending_to_publish(new)
                except Exception as e:
                    self.logger.exception(e)
                    got_error = e

            job.add_done_callback(reported)
            await job
            if got_error:
                self.logger.exception("Failed to create release note: %s" %
                                      got_error,
                                      extra={"notify": True})
                self.register_status(bdoc,
                                     "release_note",
                                     "failed",
                                     job={
                                         "step": "release_note",
                                         "err": str(e)
                                     },
                                     release_note={old: {}})
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
            for subsrc, count in dstats.items():
                try:
                    src_sub = get_source_fullname(subsrc).split(".")
                except AttributeError:
                    # not a merge stats coming from a source
                    # (could be custom field stats, eg. total_* in mygene)
                    src_sub = [subsrc]
                if len(src_sub) > 1:
                    # we have sub-sources we need to split the count
                    src, sub = src_sub
                    stats.setdefault(src, {})
                    stats[src][sub] = {"_count": count}
                else:
                    src = src_sub[0]
                    stats[src] = {"_count": count}
            return stats

        def get_versions(doc):
            try:
                versions = dict((k, {"_version": v["version"]}) for k, v in
                                doc.get("_meta", {}).get("src", {}).items() if "version" in v)
            except KeyError:
                # previous version format
                versions = dict((k, {
                    "_version": v
                })
                    for k, v in doc.get("_meta", {}).get(
                        "src_version", {}).items())
            return versions

        diff_folder = generate_folder(btconfig.DIFF_PATH, old_colname,
                                      new_colname)
        try:
            metafile = os.path.join(diff_folder, "metadata.json")
            metadata = json.load(open(metafile))
            old_colname = metadata["old"]["backend"]
            new_colname = metadata["new"]["backend"]
            diff_stats = metadata["diff"]["stats"]
        except FileNotFoundError:
            # we're generating a release note without diff information
            self.logger.info(
                "No metadata.json file found, this release note won't have diff stats included"
            )
            diff_stats = {}

        new = create_backend(new_colname)
        old = create_backend(old_colname)
        assert isinstance(old, DocMongoBackend) and isinstance(new, DocMongoBackend), \
            "Only MongoDB backend types are allowed when generating a release note"
        assert old.target_collection.database.name == btconfig.DATA_TARGET_DATABASE and \
            new.target_collection.database.name == btconfig.DATA_TARGET_DATABASE, \
            "Target databases must match current DATA_TARGET_DATABASE setting"
        new_doc = get_src_build().find_one({"_id": new.target_collection.name})
        if not new_doc:
            raise PublisherException("Collection '%s' has no corresponding build document" %
                                     new.target_collection.name)
        # old_doc doesn't have to exist (but new_doc has) in case we build a initial release note
        # compared against nothing
        old_doc = get_src_build().find_one({"_id": old.target_collection.name
                                            }) or {}
        tgt_db = get_target_db()
        old_total = tgt_db[old.target_collection.name].count()
        new_total = tgt_db[new.target_collection.name].count()
        changes = {
            "old": {
                "_version": old.version,
                "_count": old_total,
            },
            "new": {
                "_version": new.version,
                "_count": new_total,
                "_fields": {},
                "_summary": diff_stats,
            },
            "stats": {
                "added": {},
                "deleted": {},
                "updated": {},
            },
            "note": note,
            "generated_on": str(datetime.now().astimezone()),
            "sources": {
                "added": {},
                "deleted": {},
                "updated": {},
            }
        }
        # for later use
        new_versions = get_versions(new_doc)
        old_versions = get_versions(old_doc)
        # now deal with stats/counts. Counts are related to uploader, ie. sub-sources
        new_merge_stats = get_counts(new_doc.get("merge_stats", {}))
        old_merge_stats = get_counts(old_doc.get("merge_stats", {}))
        new_stats = get_counts(new_doc.get("_meta", {}).get("stats", {}))
        old_stats = get_counts(old_doc.get("_meta", {}).get("stats", {}))
        new_info = update_dict_recur(new_versions, new_merge_stats)
        old_info = update_dict_recur(old_versions, old_merge_stats)

        def analyze_diff(ops, destdict, old, new):
            for op in ops:
                # get main source / main field
                key = op["path"].strip("/").split("/")[0]
                if op["op"] == "add":
                    destdict["added"][key] = new[key]
                elif op["op"] == "remove":
                    destdict["deleted"][key] = old[key]
                elif op["op"] == "replace":
                    destdict["updated"][key] = {
                        "new": new[key],
                        "old": old[key]
                    }
                else:
                    raise ValueError(
                        "Unknown operation '%s' while computing changes" %
                        op["op"])

        # diff source info
        # this only works on main source information: if there's a difference in a
        # sub-source, it won't be shown but considered as if it was the main-source
        ops = jsondiff(old_info, new_info)
        analyze_diff(ops, changes["sources"], old_info, new_info)

        ops = jsondiff(old_stats, new_stats)
        analyze_diff(ops, changes["stats"], old_stats, new_stats)

        # mapping diff: we re-compute them and don't use any mapping.pyobj because that file
        # only allows "add" operation as a safety rule (can't delete fields in ES mapping once indexed)
        ops = jsondiff(old_doc.get("mapping", {}), new_doc["mapping"])
        for op in ops:
            changes["new"]["_fields"].setdefault(op["op"], []).append(
                op["path"].strip("/").replace("/", "."))

        return changes

    def release_info(self, env=None, remote=False):
        res = copy.deepcopy(self.release_config)
        for kenv in self.release_config["env"]:
            if env and env != kenv:
                continue
            if remote:
                raise NotImplementedError()
                # TODO: could list all releases from S3 ?
        return res
