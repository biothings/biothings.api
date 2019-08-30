import sys, re, os, time, math, glob, copy
from datetime import datetime
from dateutil.parser import parse as dtparse
import pickle, json
from pprint import pformat
import asyncio
from functools import partial
import subprocess

from biothings.utils.mongo import get_previous_collection
from biothings.utils.hub_db import get_src_build
import biothings.utils.aws as aws
from biothings.utils.common import timesofar, get_random_string, iter_n, \
                                   get_class_from_classpath, get_dotfield_value
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
from biothings import config as btconfig
from biothings.utils.hub import publish_data_version
from biothings.hub.databuild.backend import generate_folder, create_backend, \
                                            merge_src_build_metadata
from biothings.hub.dataindex.indexer import ESIndexerException
from biothings.hub import RELEASEMANAGER_CATEGORY

# default from config
logging = btconfig.logger


def template_out(field,confdict):
    """
    Return field as a templated-out filed,
    substituting some "%(...)s" part with confdict,
    as well as replacing "$(...)" with a timestamp
    following specified format.
    Ex:
        confdict = {"a":"one"}
        field = "%(a)s_two_three_$(%Y%m)"
        => "one_two_three_201908" # assuming we're in August 2019
    """
    # first deal with timestamp
    pat = re.compile(".*(\$\((.*?)\)).*")
    m = pat.match(field)
    if m:
        tosub,fmt = m.groups()
        ts = datetime.now().strftime(fmt)
        field.replace(tosub,ts)
    # then use dict to sub keys
    field = field % confdict

    return field


class ReleaseException(Exception): pass

class ReleaseManager(BaseManager):

    def __init__(self, poll_schedule=None, *args, **kwargs):
        super(ReleaseManager,self).__init__(*args, **kwargs)
        self.t0 = time.time()
        self.log_folder = btconfig.LOG_FOLDER
        self.timestamp = datetime.now()
        self.release_config = {}
        self.poll_schedule = poll_schedule
        self.es_backups_folder = btconfig.ES_BACKUPS_FOLDER
        self.ti = time.time()
        self.setup()

    def setup(self):
        self.setup_log()
    
    def setup_log(self):
        self.logger, self.logfile = get_logger(RELEASEMANAGER_CATEGORY,self.log_folder)
    
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
    
    def __getitem__(self,env):
        """
        Return an instance of a releaser for the release environment named "env"
        """
        kwargs = BaseManager.__getitem__(self,env)
        return kwargs
    
    def poll(self,state,func):
        super().poll(state,func,col=get_src_build())
    
    def configure(self, confdict, diff_manager, index_manager):
        """
        Configure manager with release "confdict". See config_hub.py in API
        for the format.
        """
        self.release_config = copy.deepcopy(confdict)
        self.diff_manager = diff_manager
        self.index_manager = index_manager
        for env, conf in self.release_config.get("env",{}).items():
            try:
                assert conf.get("cloud") and conf["cloud"]["type"] == "aws", \
                       "Only Amazon AWS cloud is supported at the moment"
                self.register[env] = conf
            except Exception as e:
                self.logger.error("Couldn't setup release environment '%s' because: %s" % (env,e))

    def find_build(self, name, ktype):
        '''
        Load build info from src_build collection, either from an index name (ktype="index")
        or snaphost name (ktype="snapshot"), not a build_name. In most cases,
        build=index=snapshot names so first search accordingly. If none could be found,
        iterate over all build docs looking for matching inner keys. (no complex query as
        hub db interface doesn't handle complex ones - for ES, SQLite, etc... backends,
        see bt.utils.hub_db for more).
        '''
        src_build = get_src_build()
        build_doc = src_build.find_one({'_id': name})
        if not build_doc:
            bdocs = src_build.find()
            for doc in bdoc:
                if name in doc.get(ktype,{}):
                    build_doc = doc
                    break
        assert build_doc, "Can't find build document with %s named '%s'" % (ktype,name)
        return build_doc
    
    def register_status(self,status,transient=False,init=False,**extra):
        src_build = get_src_build()
        job_info = {
                'status': status,
                'step_started_at': datetime.now(),
                'logfile': self.logfile,
                }
        release_info = {}
        stage = None
        stage_key = None
        # register status can be about different stages:
        if "snapshot" in extra:
            stage = "snapshot"
            release_info.setdefault("snapshot",{}).update(extra["snapshot"])
        if "publish" in extra:
            stage = "publish"
            release_info.setdefault("publish",{}).update(extra["publish"])
        assert stage, "Unknown stage '%s' to register status with" % stage
        stage_key  = list(extra[stage].keys())
        assert len(stage_key) == 1, stage_key
        stage_key = stage_key.pop()
        if transient:
            # record some "in-progress" information
            job_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            job_info["time"] = timesofar(self.ti)
            t1 = round(time.time() - self.ti, 0)
            job_info["time_in_s"] = t1
            ####release_info["created_at"] = datetime.now()
            release_info.setdefault("snapshot",{}).setdefault(stage_key,{}).update({"created_at" : datetime.now()})
        if "release" in extra:
            release_info["release"].update(extra["release"])
        if "job" in extra:
            job_info.update(extra["job"])
        # since the base is the merged collection, we register info there
        build = self.find_build(stage_key,stage)
        if init:
            # init timer for this step
            self.ti = time.time()
            src_build.update({'_id': build["_id"]}, {"$push": {'jobs': job_info}})
            # now refresh/sync
            build = src_build.find_one({'_id': build["_id"]})
        else:
            # merge extra at root level
            build["jobs"] and build["jobs"].append(job_info)
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
            build = merge_index_info(build,release_info)
            src_build.replace_one({"_id" : build["_id"]}, build)
    
    
    def publish_release(self):#, releaser_env, s3_folder, prev=None, snapshot=None, release_folder=None, index=None,
                         #repository=btconfig.SNAPSHOT_REPOSITORY, steps=["meta","post"]):
            """
            TODO
            """
            pass
    
    # from indexer
    def post_publish(self):#, releaser_env, s3_folder, prev, snapshot, release_folder, index,
                         #repository, steps, *args, **kwargs):
        """Post-publish hook, can be implemented in sub-class"""
        return
    # from differ
    def post_publish(self):#, s3_folder, old_db_col_names, new_db_col_names, diff_folder, release_folder,
                     #steps, s3_bucket, *args, **kwargs):
        """Post-publish hook, can be implemented in sub-class"""
        return


    def release_info(self, env=None, remote=False):
        pass

    #####################
    # Full data-release #
    #####################
    def get_es_idxr(self, envconf, index=None):
        if envconf["snapshot"]["es"].get("env"):
            # we should take indexer params from ES_CONFIG, ie. index_manager
            idxklass = self.index_manager.find_indexer(index)
            idxkwargs = self.index_manager[envconf["snapshot"]["es"]["env"]]
        else:
            idxklass = self.index_manager.DEFAULT_INDEXER
            es_host = envconf["snapshot"]["es"]["host"]
            idxkwargs = envconf["snapshot"]["es"]["host"]["args"]
        idxr = idxklass(**idxkwargs)
        es_idxr = ESIndexer(index=index,doc_type=idxr.doc_type,
                            es_host=idxr.host,
                            check_index=not index is None)

        return es_idxr

    def snapshot(self, releaser_env, index, snapshot=None, steps=["pre","snapshot","post"]):
        """
        Create a snapshot named "snapshot" (or, by default, same name as the index) from "index"
        according to environment definition (repository, etc...) "releaser_env".

        """
        if not releaser_env in self.release_config.get("env",{}):
            raise ValueError("Unknonw release environment '%s'" % releaser_env)
        envconf = self.release_config["env"][releaser_env]
        # check what to do
        if type(steps) == str:
            steps = [steps]
        snapshot_name = snapshot or index
        es_idxr = self.get_es_idxr(envconf,index)
        # create repo if needed
        index_meta = es_idxr.get_mapping_meta()["_meta"] # read from index
        repo_conf = self.create_repository(envconf, index_meta)
        repo_name = repo_conf["name"]
        monitor_delay = envconf["snapshot"]["monitor_delay"]
        # will hold the overall result
        fut = asyncio.Future()

        def get_status():
            try:
                res = es_idxr.get_snapshot_status(repo_name, snapshot_name)
                assert "snapshots" in res, "Can't find snapshot '%s' in repository '%s'" % (snapshot_name,repo_name)
                # assuming only one index in the snapshot, so only check first elem
                info = res["snapshots"][0]
                assert info.get("state"), "Can't find state in snapshot '%s'" % snapshot_name
                return info
            except Exception as e:
                # somethng went wrong, report as failure
                return "FAILED"

        @asyncio.coroutine
        def do(index):
            try:
                global_state = None
                got_error = None

                def snapshot_launched(f):
                    try:
                        self.logger.info("Snapshot launched: %s" % f.result())
                    except Exception as e:
                        self.logger.error("Error while lauching snapshot: %s" % e)
                        nonlocal got_error
                        got_error = e
                
                def done(f,step):
                    try:
                        action_done = f.result()
                        self.register_status("success",
                                job={
                                    "step":"%s-snapshot" % step,
                                    "actions" : [a["name"] for a in action_done],
                                    },
                                snapshot={
                                    snapshot_name : {
                                        "env" : releaser_env,
                                        step : action_done
                                        }
                                    }
                                )
                        self.logger.info("%s-snapshot done: %s" % (step,action_done))
                    except Exception as e:
                        nonlocal got_error
                        got_error = e
                        self.register_status("failed",
                                job={
                                    "step":"%s-snapshot" % step,
                                    "err" : str(e)
                                    },
                                snapshot={
                                    snapshot_name : {
                                        "env" : releaser_env,
                                        step : None,
                                        }
                                    }
                                )
                        self.logger.exception("Error while running pre-snapshot: %s" % e)

                pinfo = self.get_pinfo()
                pinfo["source"] = index

                if "pre" in steps:
                    self.register_status("snapshotting",transient=True,init=True,
                                         job={"step":"pre-snapshot"},snapshot={snapshot_name:{}})
                    pinfo["step"] = "pre-snapshot"
                    pinfo.pop("description",None)
                    job = yield from self.job_manager.defer_to_thread(pinfo,
                            partial(self.run_pre_snapshot,envconf,repo_conf,index_meta))
                    job.add_done_callback(partial(done,step="pre"))
                    yield from job
                    if got_error:
                        fut.set_exception(got_error)
                        return
                    
                if "snapshot" in steps:
                    self.register_status("snapshotting",transient=True,init=True,
                                         job={"step":"snapshot"},snapshot={snapshot_name:{}})
                    pinfo["step"] = "snapshot"
                    pinfo["description"] = es_idxr.es_host
                    self.logger.info("Creating snapshot for index '%s' on host '%s', repository '%s'" % (index,es_idxr.es_host,repo_name))
                    job = yield from self.job_manager.defer_to_thread(pinfo,
                            partial(es_idxr.snapshot,repo_name,snapshot_name))
                    job.add_done_callback(snapshot_launched)
                    yield from job

                    # launched, so now monitor completion
                    while True:
                        info = get_status()
                        state = info["state"]
                        failed_shards = info["shards_stats"]["failed"]
                        if state in ["INIT","IN_PROGRESS","STARTED"]:
                            yield from asyncio.sleep(monitor_delay)
                        else:
                            if state == "SUCCESS" and failed_shards == 0:
                                global_state = state.lower()
                                self.logger.info("Snapshot '%s' successfully created (host: '%s', repository: '%s')" % \
                                        (snapshot_name,es_idxr.es_host,repo_name),extra={"notify":True})
                            else:
                                if state == "SUCCESS":
                                    e = IndexerException("Snapshot '%s' partially failed: state is %s but %s shards failed" % (snapshot_name,state,failed_shards))
                                else:
                                    e = IndexerException("Snapshot '%s' failed: state is %s" % (snapshot_name,state))
                                self.logger.error("Failed creating snapshot '%s' (host: %s, repository: %s), state: %s" % \
                                        (snapshot_name,es_idxr.es_host,repo_name,state),extra={"notify":True})
                                got_error = e
                            break

                    if got_error:
                        self.register_status("failed",
                                job={
                                    "step":"snapshot",
                                    "err" : str(got_error)
                                    },
                                snapshot={
                                    snapshot_name : {
                                        "env" : releaser_env,
                                        "snapshot": None,
                                        }
                                    }
                                )
                        fut.set_exception(got_error)
                        return

                    else:
                        self.register_status("success",
                                job={
                                    "step" : "snapshot",
                                    "status" : global_state,
                                    },
                                snapshot={
                                    snapshot_name : {
                                        "env" : releaser_env,
                                        "snapshot": repo_conf,
                                        }
                                    }
                                )

                if "post" in steps:
                    self.register_status("snapshotting",transient=True,init=True,
                                         job={"step":"post-snapshot"},snapshot={snapshot_name:{}})
                    pinfo["step"] = "post-snapshot"
                    pinfo.pop("description",None)
                    job = yield from self.job_manager.defer_to_thread(pinfo,
                            partial(self.run_post_snapshot,envconf,repo_conf,index_meta))
                    job.add_done_callback(partial(done,step="post"))
                    yield from job
                    if got_error:
                        fut.set_exception(got_error)
                        return

                # if  we get there all is fine
                fut.set_result(global_state)

            except Exception as e:
                self.logger.exception(e)
                fut.set_exception(e)

        task = asyncio.ensure_future(do(index))
        return fut

    def run_post_snapshot(self, envconf, repo_conf, index_meta):
        return self.run_pre_post("snapshot","post",envconf,repo_conf,index_meta)

    def run_pre_snapshot(self, envconf, repo_conf, index_meta):
        return self.run_pre_post("snapshot","pre",envconf,repo_conf,index_meta)

    def run_pre_publish_snapshot(self, envconf, repo_conf, index_meta):
        return self.run_pre_post("snapshot","pre",envconf,repo_conf,index_meta)

    def publish_snapshot(self, releaser_env, s3_folder, prev=None, snapshot=None, release_folder=None, index=None,
                         repository=btconfig.SNAPSHOT_REPOSITORY, steps=["meta","post"]):
        """
        Publish snapshot metadata (not the actal snapshot, but the metadata, release notes, etc... associated to it) to S3,
        and then register that version to it's available to auto-updating hub.

        Though snapshots don't need any previous version to be applied on, a release note with significant changes
        between current snapshot and a previous version could have been generated. In that case, 

        'prev' and 'snaphost' must be defined (as strings, should match merged collections names) to generate
        a release folder, or directly release_folder (if it's required to find release notes).
        If all 3 are None, no release note will be referenced in snapshot metadata.

        'snapshot' and actual underlying index can have different names, if so, 'index' can be specified.
        'index' is mainly used to get the build_version from metadata as this information isn't part of snapshot
        information. It means in order to publish a snaphost, both the snapshot *and* the index must exist.
        """
        if type(steps) == str:
            steps = [steps]
        assert getattr(btconfig,"BIOTHINGS_ROLE",None) == "master","Hub needs to be master to publish metadata about snapshots"
        # keep passed values if any, otherwise derive them
        index = index or snapshot
        snapshot = snapshot or index
        # TODO: merged collection name can be != index name which can be != snapshot name...
        if prev and index and not release_folder:
            release_folder = generate_folder(btconfig.RELEASE_PATH,prev,index)

        @asyncio.coroutine
        def do():
            jobs = []
            pinfo = self.get_pinfo()
            pinfo["step"] = "publish"
            pinfo["source"] = snapshot
            if "meta" in steps:
                # TODO: this is a clocking call
                # snapshot at this point can be totally different than original
                # target_name but we still use it to potentially custom indexed
                # (anyway, it's just to access some snapshot info so default indexer 
                # will work)
                idxklass = self.find_indexer(snapshot) 
                idxkwargs = self[releaser_env]
                es_idxr = self.get_es_idxr(confenv)
                esb = DocESBackend(es_idxr)
                assert esb.version, "Can't retrieve a version from index '%s'" % index
                self.logger.info("Generating JSON metadata for full release '%s'" % esb.version)
                repo = es_idxr._es.snapshot.get_repository(repository)
                release_note = "release_%s" % esb.version
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
                        "build_version": esb.version,
                        "target_version": esb.version,
                        "release_date" : datetime.now().isoformat(),
                        "app_version": btconfig.APP_VERSION,
                        "biothings_version": btconfig.BIOTHINGS_VERSION,
                        "standalone_version": btconfig.STANDALONE_VERSION,
                        "metadata" : {"repository" : repo,
                                      "snapshot_name" : snapshot}
                        }
                if release_folder and os.path.exists(release_folder):
                    # ok, we have something in that folder, just pick the release note files
                    # (we can generate diff + snaphost at the same time, so there could be diff files in that folder
                    # from a diff process done before. release notes will be the same though)
                    s3basedir = os.path.join(s3_folder,esb.version)
                    notes = glob.glob(os.path.join(release_folder,"%s.*" % release_note))
                    self.logger.info("Uploading release notes from '%s' to s3 folder '%s'" % (notes,s3basedir))
                    for note in notes:
                        if os.path.exists(note):
                            s3key = os.path.join(s3basedir,os.path.basename(note))
                            aws.send_s3_file(note,s3key,
                                    aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                                    s3_bucket=btconfig.S3_RELEASE_BUCKET,overwrite=True)
                    # specify release note URLs in metadata
                    rel_txt_url = aws.get_s3_url(os.path.join(s3basedir,"%s.txt" % release_note),
                                    aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=btconfig.S3_RELEASE_BUCKET)
                    rel_json_url = aws.get_s3_url(os.path.join(s3basedir,"%s.json" % release_note),
                                    aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=btconfig.S3_RELEASE_BUCKET)
                    if rel_txt_url:
                        full_meta.setdefault("changes",{})
                        full_meta["changes"]["txt"] = {"url" : rel_txt_url}
                    if rel_json_url:
                        full_meta.setdefault("changes",{})
                        full_meta["changes"]["json"] = {"url" : rel_json_url}
                else:
                    self.logger.info("No release_folder found, no release notes will be part of the publishing")

                # now dump that metadata
                build_info = "%s.json" % esb.version
                build_info_path = os.path.join(btconfig.DIFF_PATH,build_info)
                json.dump(full_meta,open(build_info_path,"w"))
                # override lastmodified header with our own timestamp
                local_ts = dtparse(es_idxr.get_mapping_meta()["_meta"]["build_date"])
                utc_epoch = int(time.mktime(local_ts.timetuple()))
                utc_ts = datetime.fromtimestamp(time.mktime(time.gmtime(utc_epoch)))
                str_utc_epoch = str(utc_epoch)
                # it's a full release, but all build info metadata (full, incremental) all go
                # to the diff bucket (this is the main entry)
                s3key = os.path.join(s3_folder,build_info)
                aws.send_s3_file(build_info_path,s3key,
                        aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                        s3_bucket=btconfig.S3_RELEASE_BUCKET,metadata={"lastmodified":str_utc_epoch},
                         overwrite=True)
                url = aws.get_s3_url(s3key,aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                        s3_bucket=btconfig.S3_RELEASE_BUCKET)
                self.logger.info("Full release metadata published for version: '%s'" % url)
                full_info = {"build_version":full_meta["build_version"],
                        "require_version":None,
                        "target_version":full_meta["target_version"],
                        "type":full_meta["type"],
                        "release_date":full_meta["release_date"],
                        "url":url}
                publish_data_version(s3_folder,full_info)
                self.logger.info("Registered version '%s'" % (esb.version))

            if "post" in steps:
                # then we upload all the folder content
                pinfo["step"] = "post"
                self.logger.info("Runnig post-publish step")
                job = yield from self.job_manager.defer_to_thread(pinfo,partial(self.post_publish,
                            releaser_env=releaser_env, s3_folder=s3_folder, prev=prev, snapshot=snapshot,
                            release_folder=release_folder, index=index,
                            repository=repository, steps=steps))
                yield from job
                jobs.append(job)

            def published(f):
                try:
                    res = f.result()
                    self.logger.info("Snapshot '%s' uploaded to S3: %s" % (snapshot,res),extra={"notify":True})
                except Exception as e:
                    self.logger.error("Failed to upload snapshot '%s' uploaded to S3: %s" % (snapshot,e),extra={"notify":True})

            if jobs:
                yield from asyncio.wait(jobs)
                task = asyncio.gather(*jobs)
                task.add_done_callback(published)
                yield from task

        return asyncio.ensure_future(do())

    def run_post_publish_snapshot(self, envconf, repo_conf, index_meta):
        return self.run_pre_post("snapshot","post",envconf,repo_conf,index_meta)

    def get_repository_config(self, repo_conf, index_meta):
        """
        Search repo values for special values (template)
        and return a repo conf instantiated with values from idxr.
        Templated values can look like:
            "base_path" : "onefolder/%(build_version)s"
        where "build_version" value is taken from index_meta dictionary.
        In other words, such repo config are dynamic and potentially change
        for each index/snapshot created.
        """
        repo_conf["name"] = template_out(repo_conf["name"],index_meta)
        for setting in repo_conf["settings"]:
            repo_conf["settings"][setting] = template_out(repo_conf["settings"][setting],index_meta)

        return repo_conf

    def create_repository(self, envconf, index_meta={}):
        aws_key=envconf.get("cloud",{}).get("access_key")
        aws_secret=envconf.get("cloud",{}).get("secret_key")
        repo_conf = self.get_repository_config(envconf["snapshot"]["repository"],index_meta)
        self.logger.info("Repository config: %s" % repo_conf)
        repository = repo_conf["name"]
        settings = repo_conf["settings"]
        repo_type = repo_conf["type"]
        es_idxr = self.get_es_idxr(envconf)
        try:
            es_idxr.get_repository(repository)
        except ESIndexerException:
            # need to create that repo
            if repo_type == "s3":
                acl = repo_conf.get("acl",None) # let aws.create_bucket default it
                # first make sure bucket exists
                aws.create_bucket(
                        name=settings["bucket"],
                        region=settings["region"],
                        aws_key=aws_key,aws_secret=aws_secret,
                        acl=acl,
                        ignore_already_exists=True)
            settings = {"type" : repo_type,
                        "settings" : settings}
            self.logger.info("Create repository named '%s': %s" % (repository,pformat(settings)))
            es_idxr.create_repository(repository,settings=settings)

        return repo_conf

    ############################
    # Incremental data-release #
    ############################
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
                    self.logger.error("Failed to upload diff folder '%s' uploaded to S3: %s" % (diff_folder,e),extra={"notify":True})

            yield from asyncio.wait(jobs)
            task = asyncio.gather(*jobs)
            task.add_done_callback(uploaded)
            yield from task

        return asyncio.ensure_future(do())

    def run_pre_post(self, key, stage, envconf, repo_conf, index_meta):
        """
        Run pre- and post- steps for given stage (eg. "snapshot", "publish").
        These steps are defined in config file.
        """
        # start pipeline with repo config from "snapshot" step
        previous_result = repo_conf
        steps = envconf[key].get(stage,[])
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
                        previous_result = getattr(self,methname)(envconf,step_conf,index_meta,previous_result)
                        break
                if not found:
                    # default to generic one
                    previous_result = getattr(self,"step_%s" % action)(envconf,step_conf,index_meta,previous_result)

                action_done.append({"name" : action, "result" : previous_result})

            except AttributeError as e:
                raise ValueError("No such %s-%s step '%s'" % (stage,key,action))

        return action_done

    def step_archive(self, envconf, step_conf, index_meta, previous):
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
                    raise ReleaseException("Archiving failed, code: %s" % ret_code)
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

    def step_upload(self, envconf, step_conf, index_meta, previous):
        if step_conf["type"] == "s3":
            return self.step_upload_s3(envconf, step_conf, index_meta, previous)
        else:
            raise ValueError("Only 's3' upload type supported for now, got %s" % repr(step_conf["type"]))

    def step_upload_s3(self, envconf, step_conf, index_meta, previous):
        aws_key=envconf.get("cloud",{}).get("access_key")
        aws_secret=envconf.get("cloud",{}).get("secret_key")
        # create bucket if needed
        aws.create_bucket(
                name=step_conf["bucket"],
                region=step_conf["region"],
                aws_key=aws_key,aws_secret=aws_secret,
                acl=step_conf["acl"],
                ignore_already_exists=True)
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

    def reset_synced(self,diff_folder,backend=None):
        """
        Remove "synced" flag from any pyobj file in diff_folder
        """
        synced_files = glob.glob(os.path.join(diff_folder,"*.pyobj.synced"))
        for synced in synced_files:
            diff_file = re.sub("\.pyobj\.synced$",".pyobj",synced)
            os.rename(synced,diff_file)

    ################
    # Release note #
    ################
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
            return txt

        @asyncio.coroutine
        def main(release_folder):
            got_error = False
            pinfo = self.get_pinfo()
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
        #print("old_stats")
        #pprint(old_stats)
        #print("new_stats")
        #pprint(new_stats)

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

