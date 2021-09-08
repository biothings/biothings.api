import asyncio
import copy
import json
import time
from collections import UserDict, UserString
from dataclasses import dataclass
from datetime import datetime
from functools import partial

import boto3
from biothings.hub import SNAPSHOOTER_CATEGORY, SNAPSHOTMANAGER_CATEGORY
from biothings.hub.databuild.buildconfig import AutoBuildConfig
from biothings.hub.datarelease import set_pending_to_release_note
from biothings.utils.common import merge
from biothings.utils.hub import template_out
from biothings.utils.hub_db import get_src_build
from biothings.utils.manager import BaseManager
from elasticsearch import Elasticsearch

from config import logger as logging

from . import snapshot_cleanup as cleaner
from . import snapshot_registrar as registrar
from .snapshot_repo import Repository
from .snapshot_task import Snapshot


class ProcessInfo():
    """
    JobManager Process Info.
    Reported in Biothings Studio.
    """

    def __init__(self, env):
        self.env_name = env

    def get_predicates(self):
        return []

    def get_pinfo(self, step, snapshot, description=""):
        pinfo = {
            "__predicates__": self.get_predicates(),
            "category": SNAPSHOOTER_CATEGORY,
            "step": f"{step}:{snapshot}",
            "description": description,
            "source": self.env_name
        }
        return pinfo


@dataclass
class CloudStorage():
    type: str
    access_key: str
    secret_key: str
    region: str = "us-west-2"

    def get(self):
        if self.type == "aws":
            session = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region)
            return session.resource("s3")  # [X]
        raise ValueError(self.type)

class Bucket():

    def __init__(self, client, bucket):
        self.client = client  # boto3.S3.Client [X]
        self.bucket = bucket  # bucket name

    def exists(self):
        bucket = self.client.Bucket(self.bucket)
        return bool(bucket.creation_date)

    def create(self, acl="private"):

        # https://boto3.amazonaws.com/v1/documentation/api
        # /latest/reference/services/s3.html
        # #S3.Client.create_bucket

        return self.client.create_bucket(
            ACL=acl, Bucket=self.bucket,
            CreateBucketConfiguration={
                'LocationConstraint': self.region
            }
        )

    def __str__(self):
        return (
            f"<Bucket {'READY' if self.exists() else 'MISSING'}"
            f" name='{self.bucket}'"
            f" client={self.client}"
            f">"
        )


class _UserString(UserString):

    def __str__(self):
        return f"{type(self).__name__}({self.data})"

class TemplateStr(_UserString):
    ...

class RenderedStr(_UserString):
    ...


class RepositoryConfig(UserDict):
    """
    {
        "type": "s3",
        "name": "s3-$(Y)",
        "settings": {
            "bucket": "<SNAPSHOT_BUCKET_NAME>",
            "base_path": "mynews.info/$(Y)",  # per year
        }
    }
    """
    @property
    def repo(self):
        return self["name"]

    @property
    def bucket(self):
        return self["settings"]["bucket"]

    def format(self, doc=None):
        """ Template special values in this config.

        For example:
        {
            "bucket": "backup-$(Y)",
            "base_path" : "snapshots/%(_meta.build_version)s"
        }
        where "_meta.build_version" value is taken from doc in
        dot field notation, and the current year replaces "$(Y)".
        """
        template = TemplateStr(json.dumps(self.data))
        string = RenderedStr(template_out(template.data, doc or {}))

        if "%" in string:
            logging.error(template)
            logging.error(string)
            raise ValueError("Failed to template.")

        if template != string:
            logging.debug(template)
            logging.debug(string)

        return RepositoryConfig(json.loads(string.data))


class _SnapshotResult(UserDict):

    def __str__(self):
        return f"{type(self).__name__}({str(self.data)})"

class CumulativeResult(_SnapshotResult):
    ...

class StepResult(_SnapshotResult):
    ...


class SnapshotEnv():

    def __init__(self, job_manager, cloud, repository, indexer, **kwargs):
        self.job_manager = job_manager

        self.cloud = CloudStorage(**cloud).get()
        self.repcfg = RepositoryConfig(repository)
        self.client = Elasticsearch(**indexer["args"])

        self.name = kwargs["name"]  # snapshot env
        self.idxenv = indexer["name"]  # indexer env

        self.pinfo = ProcessInfo(self.name)
        self.wtime = kwargs.get("monitor_delay", 15)

    def _doc(self, index):
        doc = get_src_build().find_one({
            f"index.{index}.environment": self.idxenv})
        if not doc:  # not asso. with a build
            raise ValueError("Not a hub-managed index.")
        return doc  # TODO UNIQUENESS

    def snapshot(self, index, snapshot=None):
        @asyncio.coroutine
        def _snapshot(snapshot):
            x = CumulativeResult()
            build_doc = self._doc(index)
            cfg = self.repcfg.format(build_doc)
            for step in ("pre", "snapshot", "post"):
                state = registrar.dispatch(step)  # _TaskState Class
                state = state(get_src_build(), build_doc.get("_id"))
                logging.info(state)
                state.started()

                job = yield from self.job_manager.defer_to_thread(
                    self.pinfo.get_pinfo(step, snapshot),
                    partial(
                        getattr(self, state.func),
                        cfg, index, snapshot
                    ))
                try:
                    dx = yield from job
                    dx = StepResult(dx)

                except Exception as exc:
                    logging.exception(exc)
                    state.failed({}, exc)
                    raise exc
                else:
                    merge(x.data, dx.data)
                    logging.info(dx)
                    logging.info(x)
                    state.succeed(
                        {snapshot: x.data},
                        res=dx.data
                    )
            return x
        future = asyncio.ensure_future(_snapshot(snapshot or index))
        future.add_done_callback(logging.debug)
        return future

    def pre_snapshot(self, cfg, index, snapshot):

        bucket = Bucket(self.cloud, cfg.bucket)
        repo = Repository(self.client, cfg.repo)

        logging.info(bucket)
        logging.info(repo)

        if not repo.exists():
            if not bucket.exists():
                bucket.create(cfg.get("acl"))
                logging.info(bucket)
            repo.create(**cfg)
            logging.info(repo)

        return {
            "__REPLACE__": True,
            "conf": {"repository": cfg.data},
            "indexer_env": self.idxenv,
            "environment": self.name
        }

    def _snapshot(self, cfg, index, snapshot):

        snapshot = Snapshot(
            self.client,
            cfg.repo,
            snapshot)
        logging.info(snapshot)

        _replace = False
        if snapshot.exists():
            snapshot.delete()
            logging.info(snapshot)
            _replace = True

        # ------------------ #
        snapshot.create(index)
        # ------------------ #

        while True:
            logging.info(snapshot)
            state = snapshot.state()

            if state == "FAILED":
                raise ValueError(state)
            elif state == "IN_PROGRESS":
                time.sleep(self.wtime)
            elif state == "SUCCESS":
                break
            else:  # PARTIAL/MISSING/N/A
                raise ValueError(state)

        return {
            "index_name": index,
            "replaced": _replace,
            "created_at": datetime.now().astimezone()
        }

    def post_snapshot(self, cfg, index, snapshot):
        build_id = self._doc(index)['_id']
        set_pending_to_release_note(build_id)
        return {}


class SnapshotManager(BaseManager):
    """
    Hub ES Snapshot Management

    Config Ex:

    # env.<name>:
    {
        "cloud": {
            "type": "aws",  # default, only one supported.
            "access_key": <------------------>,
            "secret_key": <------------------>,
            "region": "us-west-2"
        },
        "repository": {
            "name": "s3-$(Y)",
            "type": "s3",
            "settings": {
                "bucket": "<SNAPSHOT_BUCKET_NAME>",
                "base_path": "mygene.info/$(Y)",  # year
            },
            "acl": "private",
        },
        "indexer": {
            "name": "local",
            "args": {
                "timeout": 100,
                "max_retries": 5
            }
        },
        "monitor_delay": 15,
    }    
    """

    def __init__(self, index_manager, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index_manager = index_manager
        self.snapshot_config = {}

    @staticmethod
    def pending_snapshot(build_name):
        src_build = get_src_build()
        src_build.update(
            {"_id": build_name},
            {"$addToSet": {"pending": "snapshot"}}
        )

    # Object Lifecycle Calls
    # --------------------------
    # manager = IndexManager(job_manager)
    # manager.clean_stale_status() # in __init__
    # manager.configure(config)

    def clean_stale_status(self):
        registrar.audit(get_src_build(), logging)

    def configure(self, conf):
        self.snapshot_config = conf
        for name, envdict in conf.get("env", {}).items():

            # Merge Indexer Config
            # ----------------------------------------
            dx = envdict["indexer"]

            if isinstance(dx, str):  # {"indexer": "prod"}
                dx = dict(name=dx)  # .          ↓
            if not isinstance(dx, dict):  # {"indexer": {"name": "prod"}}
                raise TypeError(dx)

            # compatibility with previous hubs.
            dx.setdefault("name", dx.pop("env", None))

            x = self.index_manager[dx["name"]]
            x = dict(x)  # merge into a copy
            merge(x, dx)  # <-

            envdict["indexer"] = x
            # ------------------------------------------
            envdict["name"] = name

            self.register[name] = SnapshotEnv(self.job_manager, **envdict)

    def poll(self, state, func):
        super().poll(state, func, col=get_src_build())

    # Features
    # -----------

    def snapshot(self, snapshot_env, index, snapshot=None):
        """
        Create a snapshot named "snapshot" (or, by default, same name as the index)
        from "index" according to environment definition (repository, etc...) "env".
        """
        env = self.register[snapshot_env]
        return env.snapshot(index, snapshot)

    def snapshot_a_build(self, build_doc):
        """
        Create a snapshot basing on the autobuild settings in the build config.
        If the build config associated with this build has:
        {
            "autobuild": {
                "type": "snapshot", // implied when env is set. env must be set.
                "env": "local" // which es env to make the snapshot.
            },
            ...
        }
        Attempt to make a snapshot for this build on the specified es env "local".
        """
        @asyncio.coroutine
        def _():
            autoconf = AutoBuildConfig(build_doc['build_config'])
            env = autoconf.auto_build.get('env')
            assert env, "Unknown autobuild env."

            if isinstance(env, str):
                indexer_env = env
                snapshot_env = env
            else:  # assume env is an (x,y) pair
                indexer_env, snapshot_env = env

            try:  # find the index (latest) to snapshot
                latest_index = list(build_doc['index'].keys())[-1]

            except Exception:  # no existing indices, need to create one
                yield from self.index_manager.index(indexer_env, build_doc['_id'])
                latest_index = build_doc['_id']  # index_name is build name

            return self.snapshot(snapshot_env, latest_index)
        return asyncio.ensure_future(_())

    def snapshot_info(self, env=None, remote=False):
        return self.snapshot_config

    def cleanup(
        self, env=None,  # a snapshot environment describing a repository
        keep=3,  # the number of most recent snapshots to keep in one group
        group_by="build_config",  # the attr of which its values form groups
        dryrun=True,  # display the snapshots to be deleted without deleting them
        **filters  # a set of criterions to limit which snapshots are to be cleaned
    ):
        """ Delete past snapshots and keep only the most recent ones.

        Examples:
            >>> snapshot_cleanup()
            >>> snapshot_cleanup("s3_outbreak")
            >>> snapshot_cleanup("s3_outbreak", keep=0)
        """

        snapshots = cleaner.find(  # filters support dotfield.
            get_src_build(), env, keep, group_by, **filters)

        if dryrun:
            return '\n'.join((
                "-" * 75, cleaner.plain_text(snapshots), "-" * 75,
                "DRYRUN ONLY - APPLY THE ACTIONS WITH:",
                "   > snapshot_cleanup(..., dryrun=False)"
            ))

        # return the number of snapshots successfully deleted
        return cleaner.delete(get_src_build(), snapshots, self)
