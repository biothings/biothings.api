import asyncio
import json
import os
import time
from collections import UserDict, UserString
from dataclasses import dataclass
from datetime import datetime
from functools import partial

import boto3
from config import logger as logging
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError, NotFoundError

from biothings import config as btconfig
from biothings.hub import SNAPSHOOTER_CATEGORY
from biothings.hub.databuild.buildconfig import AutoBuildConfig
from biothings.hub.datarelease import set_pending_to_release_note
from biothings.utils.common import merge
from biothings.utils.exceptions import RepositoryVerificationFailed
from biothings.utils.hub import template_out
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager

from . import snapshot_cleanup as cleaner, snapshot_registrar as registrar
from .snapshot_repo import Repository
from .snapshot_task import Snapshot


class ProcessInfo:
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
            "source": self.env_name,
        }
        return pinfo


@dataclass
class CloudStorage:
    type: str
    access_key: str
    secret_key: str
    region: str = "us-west-2"

    def get(self):
        if self.type == "aws":
            session = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )
            return session.resource("s3")  # [X]
        raise ValueError(self.type)


class Bucket:
    def __init__(self, client, bucket, region=None):
        self.client = client  # boto3.S3.Client [X]
        self.bucket = bucket  # bucket name
        self.region = region

    def exists(self):
        bucket = self.client.Bucket(self.bucket)
        return bool(bucket.creation_date)

    def create(self, acl="private"):
        # https://boto3.amazonaws.com/v1/documentation/api
        # /latest/reference/services/s3.html
        # #S3.Client.create_bucket

        return self.client.create_bucket(
            ACL=acl,
            Bucket=self.bucket,
            CreateBucketConfiguration={"LocationConstraint": self.region},
        )

    def __str__(self):
        return f"<Bucket {'READY' if self.exists() else 'MISSING'} name='{self.bucket}' client={self.client}>"


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

    @property
    def region(self):
        return self["settings"]["region"]

    def format(self, doc=None):
        """Template special values in this config.

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


class SnapshotEnv:
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
        doc = get_src_build().find_one({f"index.{index}.environment": self.idxenv})
        if not doc:  # not asso. with a build
            raise ValueError("Not a hub-managed index.")
        return doc  # TODO UNIQUENESS

    def setup_log(self, index):
        build_doc = self._doc(index)
        log_name = build_doc["target_name"] or build_doc["_id"]
        log_folder = os.path.join(btconfig.LOG_FOLDER, "build", log_name, "snapshot") if btconfig.LOG_FOLDER else None
        self.logger, _ = get_logger(index, log_folder=log_folder, force=True)

    def snapshot(self, index, snapshot=None, recreate_repo=False):
        self.setup_log(index)

        async def _snapshot(snapshot):
            x = CumulativeResult()
            build_doc = self._doc(index)
            cfg = self.repcfg.format(build_doc)
            for step in ("pre", "snapshot", "post"):
                state = registrar.dispatch(step)  # _TaskState Class
                state = state(get_src_build(), build_doc.get("_id"))
                self.logger.info(state)
                state.started()

                job = await self.job_manager.defer_to_thread(
                    self.pinfo.get_pinfo(step, snapshot),
                    partial(getattr(self, state.func), cfg, index, snapshot, recreate_repo=recreate_repo),
                )
                try:
                    dx = await job
                    dx = StepResult(dx)
                except RepositoryVerificationFailed as ex:
                    self.logger.exception(ex)
                    state.failed(snapshot, detail=ex.args)
                    raise ex
                except Exception as exc:
                    self.logger.exception(exc)
                    state.failed({}, exc)
                    raise exc
                else:
                    merge(x.data, dx.data)
                    self.logger.info(dx)
                    self.logger.info(x)
                    state.succeed({snapshot: x.data}, res=dx.data)
            return x

        future = asyncio.ensure_future(_snapshot(snapshot or index))
        future.add_done_callback(self.logger.debug)
        return future

    def pre_snapshot(self, cfg, index, snapshot, **kwargs):
        bucket = Bucket(self.cloud, cfg.bucket, region=cfg.region)
        repo = Repository(self.client, cfg.repo)

        self.logger.info(bucket)
        self.logger.info(repo)

        if kwargs.get("recreate_repo"):
            self.logger.info("Delete old repository")
            repo.delete()

        if not repo.exists():
            if not bucket.exists():
                bucket.create(cfg.get("acl"))
                self.logger.info(bucket)
            repo.create(**cfg)
            self.logger.info(repo)

        try:
            repo.verify(config=cfg)
        except TransportError as tex:
            raise RepositoryVerificationFailed({"error": tex.error, "detail": tex.info["error"]})

        return {
            "__REPLACE__": True,
            "conf": {"repository": cfg.data},
            "indexer_env": self.idxenv,
            "environment": self.name,
        }

    def _snapshot(self, cfg, index, snapshot, **kwargs):
        snapshot = Snapshot(self.client, cfg.repo, snapshot)
        self.logger.info(snapshot)

        _replace = False
        if snapshot.exists():
            snapshot.delete()
            self.logger.info(snapshot)
            _replace = True

        # ------------------ #
        snapshot.create(index)
        # ------------------ #

        while True:
            self.logger.info(snapshot)
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
            "created_at": datetime.now().astimezone(),
        }

    def post_snapshot(self, cfg, index, snapshot, **kwargs):
        build_id = self._doc(index)["_id"]
        set_pending_to_release_note(build_id)
        return {}

    def snapshot_exists(self, snapshot_name, build_doc):
        cfg = self.repcfg.format(build_doc)
        snapshot = Snapshot(self.client, cfg.repo, snapshot_name)
        try:
            return snapshot.exists()
        except NotFoundError:
            return False
        except Exception as e:
            logging.exception(f"Error checking if snapshot '{snapshot_name}' exists: {e}")
            raise


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
                "request_timeout": 100,
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
            {"$addToSet": {"pending": "snapshot"}},
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

    def snapshot(self, snapshot_env, index, snapshot=None, recreate_repo=False):
        """
        Create a snapshot named "snapshot" (or, by default, same name as the index)
        from "index" according to environment definition (repository, etc...) "env".
        """
        env = self.register[snapshot_env]
        return env.snapshot(index, snapshot, recreate_repo=recreate_repo)

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

        async def _():
            autoconf = AutoBuildConfig(build_doc["build_config"])
            env = autoconf.auto_build.get("env")
            assert env, "Unknown autobuild env."

            if isinstance(env, str):
                indexer_env = env
                snapshot_env = env
            else:  # assume env is an (x,y) pair
                indexer_env, snapshot_env = env

            try:  # find the index (latest) to snapshot
                latest_index = list(build_doc["index"].keys())[-1]

            except Exception:  # no existing indices, need to create one
                await self.index_manager.index(indexer_env, build_doc["_id"])
                latest_index = build_doc["_id"]  # index_name is build name

            return self.snapshot(snapshot_env, latest_index)

        return asyncio.ensure_future(_())

    def snapshot_info(self, env=None, remote=False):
        return self.snapshot_config

    def list_snapshots(self, env=None, return_db_cols=True, **filters):
        return cleaner.find( # filters support dotfield
            get_src_build(),
            env=env,
            group_by="build_config",
            return_db_cols=return_db_cols,
            **filters,
        )

    def cleanup(
        self,
        env=None,  # a snapshot environment describing a repository
        keep=3,  # the number of most recent snapshots to keep in one group
        group_by="build_config",  # the attr of which its values form groups
        dryrun=True,  # display the snapshots to be deleted without deleting them
        **filters,  # a set of criterions to limit which snapshots are to be cleaned
    ):
        """Delete past snapshots and keep only the most recent ones.

        Examples:
            >>> snapshot_cleanup()
            >>> snapshot_cleanup("s3_outbreak")
            >>> snapshot_cleanup("s3_outbreak", keep=0)
        """

        # filters support dotfield.
        snapshots = cleaner.find(
            get_src_build(), env=env, keep=keep, group_by=group_by, **filters
        )

        if dryrun:
            return "\n".join(
                (
                    "-" * 75,
                    cleaner.plain_text(snapshots),
                    "-" * 75,
                    "DRYRUN ONLY - APPLY THE ACTIONS WITH:",
                    "   > snapshot_cleanup(..., dryrun=False)",
                )
            )

        # return the number of snapshots successfully deleted
        return cleaner.delete(get_src_build(), snapshots, self)

    def delete_snapshots(self, snapshots_data):
        async def delete(environment, snapshot_names):
            if environment == "__no_env__":
                environment = None
            return self.cleanup(env=environment, keep=0, dryrun=False, _id={"$in": snapshot_names})

        def done(f):
            try:
                # just consume the result to raise exception
                # if there were an error... (what an api...)
                f.result()
                logging.info("success", extra={"notify": True})
            except Exception as e:
                logging.exception("failed: %s" % e, extra={"notify": True})

        jobs = []
        try:
            for environment, snapshot_names in snapshots_data.items():
                job = self.job_manager.submit(partial(delete, environment, snapshot_names))
                jobs.append(job)
            tasks = asyncio.gather(*jobs)
            tasks.add_done_callback(done)
        except Exception as ex:
            logging.exception("Error while deleting snapshots. error: %s", ex, extra={"notify": True})
        return jobs

    def delete_snapshot_from_db(self, build_name, snapshot_name):
        collection = get_src_build()
        collection.update_one(
            {"_id": build_name},
            {"$unset": {f"snapshot.{snapshot_name}": 1}},
        )
        logging.info(f"Snapshot '{snapshot_name}' deleted from build '{build_name}' in MongoDB")

    def sync_snapshots(self):
        async def sync():
            logging.info("Starting synchronization of snapshots...")
            collection = get_src_build()
            snapshots = self.list_snapshots(return_db_cols=True)
            errors = []

            for group in snapshots:
                for snapshot_data in group['items']:
                    snapshot_name = snapshot_data['_id']
                    build_name = snapshot_data['build_name']
                    environment = snapshot_data.get('environment') or snapshot_data['conf']['indexer']['env']

                    if not environment:
                        msg = f"[{snapshot_name}] Snapshot '{snapshot_name}' does not have an environment associated with it. Skipping synchronization."
                        logging.warning(msg)
                        errors.append(msg)
                        continue

                    try:
                        env = self.register[environment]
                    except KeyError:
                        msg = f"[{snapshot_name}] Environment '{environment}' is not registered and connection details are unavailable. Consider adding it to the hub configuration othwerwise manual deletion is required."
                        logging.error(msg)
                        errors.append(msg)
                        continue

                    build_doc = collection.find_one({'_id': build_name})

                    try:
                        exists = env.snapshot_exists(snapshot_name, build_doc)
                        if not exists:
                            logging.info(f"Deleting snapshot '{snapshot_name}' from MongoDB")
                            self.delete_snapshot_from_db(build_name, snapshot_name)
                    except Exception as e:
                        msg = f"Error checking snapshot '{snapshot_name}': {str(e)}"
                        logging.exception(msg)
                        errors.append(msg)
            logging.info("Synchronization of snapshots completed.")
            if errors:
                raise ValueError("Errors occurred during synchronization:\n" + "\n".join(errors))

        def done(f):
            try:
                f.result()
                logging.info("Synchronization successful", extra={"notify": True})
            except Exception as e:
                logging.exception(f"Synchronization failed: {e}", extra={"notify": True})


        try:
            job = self.job_manager.submit(sync)
            job.add_done_callback(done)
        except Exception as ex:
            logging.exception(
                f"Error while submitting synchronization job: {ex}", extra={"notify": True})
        return job
