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
from biothings.utils.manager import BaseManager, BaseStatusRegisterer
from biothings.utils.es import ESIndexer, IndexerException as ESIndexerException
from biothings.utils.backend import DocESBackend
from biothings import config as btconfig
from biothings.utils.mongo import doc_feeder, id_feeder
from config import LOG_FOLDER, logger as logging
from biothings.utils.hub import publish_data_version, template_out
from biothings.hub.databuild.backend import generate_folder, create_backend, \
                                            merge_src_build_metadata
from biothings.hub import SNAPSHOOTER_CATEGORY, SNAPSHOTMANAGER_CATEGORY


class Snapshooter(BaseStatusRegisterer):

    def __init__(self, job_manager, index_manager, envconf, log_folder, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index_manager = index_manager
        self.job_manager = job_manager
        self.envconf = envconf
        self.log_folder = log_folder
        self.ti = time.time()
        self.setup()
        # extract some "special" values depending on the config type, etc...
        if self.envconf.get("repository",{}).get("type") == "fs":
            self.es_backups_folder = self.envconf["repository"]["es_backups_folder"]

    @property
    def collection(self):
        return get_src_build()

    def setup(self):
        self.setup_log()
    
    def setup_log(self):
        self.logger, self.logfile = get_logger(SNAPSHOOTER_CATEGORY,self.log_folder)

    def get_predicates(self):
        return []
    
    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {"category" : SNAPSHOOTER_CATEGORY,
                "source" : "",
                "step" : "",
                "description" : ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def load_build(self, index_name):
        return self.load_doc(index_name,"index")

    def register_status(self, bdoc, status,transient=False,init=False,**extra):
        super().register_status(bdoc,"snapshot",status,transient=transient,init=init,**extra)

    def get_es_idxr(self, envconf, index=None):
        if envconf["indexer"].get("env"):
            # we should take indexer params from ES_CONFIG, ie. index_manager
            idxklass = self.index_manager.find_indexer(index)
            idxkwargs = self.index_manager[envconf["indexer"]["env"]]
        else:
            idxklass = self.index_manager.DEFAULT_INDEXER
            es_host = envconf["indexer"]["host"]
            idxkwargs = envconf["indexer"]["host"]["args"]
        idxr = idxklass(**idxkwargs)
        es_idxr = ESIndexer(index=index,doc_type=idxr.doc_type,
                            es_host=idxr.host,
                            check_index=not index is None)

        return es_idxr

    def pre_snapshot(self, envconf, repo_conf, index_meta):
        pass

    def snapshot(self, index, snapshot=None, steps=["pre","snapshot","post"]):
        bdoc = self.load_build(index)
        envconf = self.envconf
        # check what to do
        if type(steps) == str:
            steps = [steps]
        snapshot_name = snapshot or index
        es_idxr = self.get_es_idxr(envconf,index)
        # create repo if needed
        index_meta = es_idxr.get_mapping_meta()["_meta"] # read from index
        repo_name, repo_conf = self.create_repository(envconf, index_meta)
        monitor_delay = envconf["monitor_delay"]
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
                        res = f.result()
                        self.register_status(bdoc,"success",
                                job={"step":"%s-snapshot" % step,"result" : res},
                                snapshot={snapshot_name : {"env" : self.envconf,step : res}})
                        self.logger.info("%s-snapshot done: %s" % (step,res))
                    except Exception as e:
                        nonlocal got_error
                        got_error = e
                        self.register_status(bdoc,"failed",
                                job={"step":"%s-snapshot" % step,"err" : str(e)},
                                snapshot={snapshot_name : {"env" : self.envconf,step : None}})
                        self.logger.exception("Error while running pre-snapshot: %s" % e)

                pinfo = self.get_pinfo()
                pinfo["source"] = index

                if "pre" in steps:
                    self.register_status(bdoc,"pre-snapshotting",transient=True,init=True,
                                         job={"step":"pre-snapshot"},snapshot={snapshot_name:{}})
                    pinfo["step"] = "pre-snapshot"
                    pinfo.pop("description",None)
                    job = yield from self.job_manager.defer_to_thread(pinfo,
                            partial(self.pre_snapshot,envconf,repo_conf,index_meta))
                    job.add_done_callback(partial(done,step="pre"))
                    yield from job
                    if got_error:
                        fut.set_exception(got_error)
                        return
                    
                if "snapshot" in steps:
                    self.register_status(bdoc,"snapshotting",transient=True,init=True,
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
                        self.register_status(
                                bdoc,
                                "failed",
                                job={
                                    "step":"snapshot",
                                    "err" : str(got_error)
                                    },
                                snapshot={
                                    snapshot_name : {
                                        "env" : self.envconf,
                                        "repository": None,
                                        }
                                    }
                                )
                        fut.set_exception(got_error)
                        return

                    else:
                        self.register_status(
                                bdoc,
                                "success",
                                job={
                                    "step" : "snapshot",
                                    "status" : global_state,
                                    },
                                snapshot={
                                    snapshot_name : {
                                        "env" : self.envconf,
                                        "repository": {repo_name : repo_conf}
                                        }
                                    }
                                )

                if "post" in steps:
                    self.register_status(bdoc,"post-snapshotting",transient=True,init=True,
                                         job={"step":"post-snapshot"},snapshot={snapshot_name:{}})
                    pinfo["step"] = "post-snapshot"
                    pinfo.pop("description",None)
                    job = yield from self.job_manager.defer_to_thread(pinfo,
                            partial(self.post_snapshot,envconf,repo_conf,index_meta))
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

    def post_snapshot(self, envconf, repo_conf, index_meta):
        pass

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
        repo_name = template_out(repo_conf["name"],index_meta)
        repo_type = template_out(repo_conf["type"],index_meta)
        repo_settings = {}
        for setting in repo_conf["settings"]:
            repo_settings[setting] = template_out(repo_conf["settings"][setting],index_meta)

        return repo_name, {"type" : repo_type,"settings" : repo_settings}

    def create_repository(self, envconf, index_meta={}):
        aws_key=envconf.get("cloud",{}).get("access_key")
        aws_secret=envconf.get("cloud",{}).get("secret_key")
        repo_name, repo_conf = self.get_repository_config(envconf["repository"],index_meta)
        self.logger.info("Repository config: %s" % repo_conf)
        settings = repo_conf["settings"]
        repo_type = repo_conf["type"]
        es_idxr = self.get_es_idxr(envconf)
        try:
            es_idxr.get_repository(repo_name)
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
            self.logger.info("Create repository named '%s': %s" % (repo_name,pformat(settings)))
            es_idxr.create_repository(repo_name,settings=settings)

        return repo_name,repo_conf


class SnapshotManager(BaseManager):

    DEFAULT_SNAPSHOOTER_CLASS = Snapshooter

    def __init__(self, index_manager, poll_schedule=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index_manager = index_manager
        self.t0 = time.time()
        self.log_folder = btconfig.LOG_FOLDER
        self.timestamp = datetime.now()
        self.snapshot_config = {}
        self.poll_schedule = poll_schedule
        self.es_backups_folder = btconfig.ES_BACKUPS_FOLDER
        self.setup()

    def setup(self):
        self.setup_log()
    
    def setup_log(self):
        self.logger, self.logfile = get_logger(SNAPSHOTMANAGER_CATEGORY,self.log_folder)
    
    def poll(self,state,func):
        super().poll(state,func,col=get_src_build())
    
    def __getitem__(self,env):
        """
        Return an instance of a snapshooter for snapshot environment named "env"
        """
        d = self.register[env]
        envconf = self.snapshot_config["env"][env]
        return d["class"](index_manager=d["index_manager"],
                          job_manager=d["job_manager"],
                          envconf=envconf,log_folder=self.log_folder)

    def configure(self, snapshot_confdict):
        """
        Configure manager with snapshot config dict. See SNAPSHOT_CONFIG in config_hub.py
        for the format.
        """
        self.snapshot_config = copy.deepcopy(snapshot_confdict)
        for env, envconf in self.snapshot_config.get("env",{}).items():
            try:
                if envconf.get("cloud"):
                    assert envconf["cloud"]["type"] == "aws", \
                            "Only Amazon AWS cloud is supported at the moment"
                self.register[env] = {
                        "class" : self.DEFAULT_SNAPSHOOTER_CLASS,
                        "conf" : envconf,
                        "job_manager" : self.job_manager,
                        "index_manager" : self.index_manager
                        }
            except Exception as e:
                self.logger.exception("Couldn't setup snapshot environment '%s' because: %s" % (env,e))

    def snapshot(self, snapshot_env, index, snapshot=None, steps=["pre","snapshot","post"]):
        """
        Create a snapshot named "snapshot" (or, by default, same name as the index) from "index"
        according to environment definition (repository, etc...) "snapshot_env".

        """
        if not snapshot_env in self.snapshot_config.get("env",{}):
            raise ValueError("Unknonw snapshot environment '%s'" % snapshot_env)
        snapshooter = self[snapshot_env]
        return snapshooter.snapshot(index, snapshot=snapshot, steps=steps)

