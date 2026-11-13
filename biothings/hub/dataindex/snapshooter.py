import asyncio
import copy
import json
import math
import os
import time
import warnings
from collections import UserDict, UserString
from dataclasses import dataclass
from datetime import datetime
from functools import partial

import boto3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, TransportError

from biothings import config as btconfig
from biothings.hub import SNAPSHOOTER_CATEGORY
from biothings.hub.databuild.buildconfig import AutoBuildConfig
from biothings.hub.datarelease import set_pending_to_release_note
from biothings.hub.manager import BaseManager
from biothings.utils.common import merge
from biothings.utils.es import ESIndexer
from biothings.utils.exceptions import RepositoryVerificationFailed
from biothings.utils.hub import template_out
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from config import logger as logging

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


class TemplateStr(_UserString): ...


class RenderedStr(_UserString): ...


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


class CumulativeResult(_SnapshotResult): ...


class StepResult(_SnapshotResult): ...


class SnapshotBatchError(RuntimeError):
    """A failed batch, retaining the outcomes of every requested index."""

    def __init__(self, results):
        self.results = results
        super().__init__("Snapshot batch failed: " + json.dumps(results, default=str))


class RestoreBatchError(RuntimeError):
    """A failed restore batch, retaining every snapshot's outcome."""

    def __init__(self, results):
        self.results = results
        super().__init__("Restore batch failed: " + json.dumps(results, default=str))


def _snapshot_indices(index, *, kind="index"):
    names = [index] if isinstance(index, str) else index
    if not isinstance(names, list):
        raise TypeError(f"{kind} must be a name or a list of exact {kind} names")
    for name in names:
        if (
            not isinstance(name, str)
            or not name
            or name in {".", ".."}
            or name.startswith(("_", "-", "+"))
            or any(char.isspace() or char in '*?,#:/\\<>|"[]' for char in name)
        ):
            raise ValueError(f"Expected one exact concrete {kind} name: {name!r}")
    if len(names) != len(set(names)):
        raise ValueError(f"Duplicate {kind} names are not allowed")
    return names


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

        future = self.job_manager.loop.create_task(_snapshot(snapshot or index))
        # logging the result doubles as marking the exception retrieved
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

    def restore(
        self,
        snapshot,
        index=None,
        *,
        include_aliases=False,
        index_settings=None,
        ignore_index_settings=None,
        recovery_timeout=3600,
    ):
        """Restore a single-index snapshot and monitor recovery to completion.

        The destination defaults to the index recorded in the snapshot. Existing
        indices and aliases are rejected. The environment owns the client shared
        with ESIndexer; this operation does not close it. Source aliases are
        excluded by default, and global state is never restored.
        """
        if not isinstance(snapshot, str):
            raise TypeError("restore requires a single snapshot name; use restore_many for a batch")
        options = dict(
            include_aliases=include_aliases,
            index_settings=copy.deepcopy(index_settings),
            ignore_index_settings=copy.deepcopy(ignore_index_settings),
            recovery_timeout=recovery_timeout,
        )
        plan = self._restore_plan(snapshot, index, **options)
        if len(plan["indices"]) != 1:
            raise ValueError("restore requires a single snapshot; use restore_many for a batch")
        row = plan["indices"][0]
        if row["action"] == "blocked":
            raise ValueError(f"Cannot restore snapshot: {row}")
        name = row["index_name"]
        progress = [f"{name}: PENDING"]

        async def work(step, operation):
            job = await self.job_manager.defer_to_thread(self.pinfo.get_pinfo(step, snapshot), operation)
            return await job

        def submit():
            # Recheck identity and destination immediately before submitting. Never
            # retry an uncertain restore request through the client's transport.
            current = self._restore_plan(snapshot, index, **options)["indices"][0]
            if current["action"] == "blocked":
                raise ValueError(f"Cannot restore snapshot: {current}")
            if current["snapshot_uuid"] != row["snapshot_uuid"] or current["index_name"] != name:
                raise ValueError(f"Snapshot changed after preflight: {snapshot}")
            indexer = ESIndexer(
                name,
                check_index=False,
                client=self.client.options(max_retries=0, retry_on_timeout=False),
            )
            response = indexer.restore(
                row["repository"],
                snapshot,
                name,
                include_aliases=include_aliases,
                index_settings=options["index_settings"],
                ignore_index_settings=options["ignore_index_settings"],
            )
            return indexer, response

        async def _restore():
            try:
                progress[0] = f"{name}: RESTORING"
                indexer, response = await work("restore", submit)
                if not response.get("accepted"):
                    raise RuntimeError(f"Restore was not accepted for {name}: {response}")
                deadline = self.job_manager.loop.time() + recovery_timeout
                errors = 0
                while True:
                    remaining = deadline - self.job_manager.loop.time()
                    if remaining <= 0:
                        raise TimeoutError(f"Recovery timed out for {name}")
                    try:
                        status = await asyncio.wait_for(work("recovery", indexer.get_restore_status), remaining)
                    except asyncio.TimeoutError:
                        raise TimeoutError(f"Recovery timed out for {name}") from None
                    except Exception as exc:
                        errors += 1
                        if errors >= 3:
                            raise
                        logging.warning("Recovery check failed for %s; retrying: %s", name, exc)
                    else:
                        errors = 0
                        progress[0] = f"{name}: {status['status']} {status.get('progress', '')}".rstrip()
                        if status["status"] == "DONE":
                            return {
                                "snapshot_env": self.name,
                                "indexer_env": self.idxenv,
                                **{key: row[key] for key in ("snapshot", "repository", "snapshot_uuid", "index_name")},
                                **status,
                            }
                        if status["status"] == "FAILED":
                            raise RuntimeError(f"Recovery failed for {name}: {status.get('reason', status)}")
                        if status["status"] not in {"INIT", "IN_PROGRESS"}:
                            raise RuntimeError(f"Unexpected recovery status for {name}: {status}")
                    await asyncio.sleep(min(self.wtime, max(0, deadline - self.job_manager.loop.time())))
            except asyncio.CancelledError:
                progress[0] = f"{name}: CANCELLED"
                raise
            except Exception:
                progress[0] = f"{name}: FAILED"
                raise

        task = self.job_manager.loop.create_task(_restore())
        task.progress = progress
        return task

    def _restore_plan(
        self,
        snapshot,
        index=None,
        *,
        include_aliases=False,
        index_settings=None,
        ignore_index_settings=None,
        recovery_timeout=3600,
    ):
        names = _snapshot_indices(snapshot, kind="snapshot")
        if index is not None:
            if not isinstance(index, str):
                raise TypeError("index must be one destination index name")
            _snapshot_indices(index)
            if len(names) != 1:
                raise ValueError("A destination index name requires a single snapshot")
        if not isinstance(include_aliases, bool):
            raise TypeError("include_aliases must be a boolean")
        if index_settings is not None and not isinstance(index_settings, dict):
            raise TypeError("index_settings must be a dictionary")
        if ignore_index_settings is not None and (
            not isinstance(ignore_index_settings, list)
            or any(not isinstance(setting, str) or not setting for setting in ignore_index_settings)
        ):
            raise TypeError("ignore_index_settings must be a list of setting names")
        if (
            isinstance(recovery_timeout, bool)
            or not isinstance(recovery_timeout, (int, float))
            or not math.isfinite(recovery_timeout)
            or recovery_timeout <= 0
        ):
            raise ValueError("recovery_timeout must be a positive number of seconds")
        plan = {
            "snapshot_env": self.name,
            "indexer_env": self.idxenv,
            "dry_run": True,
            "include_aliases": include_aliases,
            "index_settings": copy.deepcopy(index_settings),
            "ignore_index_settings": copy.deepcopy(ignore_index_settings),
            "indices": [],
        }
        if not names:
            return plan
        repository = self.repcfg.format().repo
        try:
            response = self.client.snapshot.get(repository=repository, snapshot=names, ignore_unavailable=True)
            snapshots = {item["snapshot"]: item for item in response["snapshots"]}
        except NotFoundError:
            snapshots = {}
        targets = {}
        for name in names:
            row = {"snapshot": name, "repository": repository, "action": "restore"}
            plan["indices"].append(row)
            metadata = snapshots.get(name)
            if not metadata or metadata.get("state") != "SUCCESS" or metadata.get("shards", {}).get("failed", 0):
                row.update(action="blocked", reason="snapshot or repository is missing, or snapshot is incomplete")
                continue
            indices = metadata.get("indices", [])
            if len(indices) != 1:
                row.update(action="blocked", reason="expected a snapshot containing exactly one index")
                continue
            _snapshot_indices(indices)
            row["index_name"] = index or indices[0]
            row["snapshot_uuid"] = metadata["uuid"]
            targets.setdefault(row["index_name"], []).append(row)
        if targets:
            existing = self.client.indices.get(
                index=list(targets),
                features="aliases",
                expand_wildcards="all",
                ignore_unavailable=True,
                allow_no_indices=True,
            )
            conflicts = set(existing)
            for info in existing.values():
                conflicts.update(info.get("aliases", {}))
            for name, rows in targets.items():
                reason = None
                if len(rows) > 1:
                    reason = "multiple snapshots target the same destination index"
                elif name in conflicts:
                    reason = "destination index or alias already exists"
                if reason:
                    for row in rows:
                        row.update(action="blocked", reason=reason)
        return plan

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

    def create_snapshot(self, snapshot_env, index, snapshot=None, recreate_repo=False):
        """
        Create a snapshot named "snapshot" (or, by default, same name as the index)
        from "index" according to environment definition (repository, etc...) "env".
        """
        if snapshot_env not in self.register:
            raise ValueError("Unknown snapshot environment '%s' (see SNAPSHOT_CONFIG)" % snapshot_env)
        env = self.register[snapshot_env]
        return env.snapshot(index, snapshot, recreate_repo=recreate_repo)

    def snapshot(self, snapshot_env, index, snapshot=None, recreate_repo=False):
        """Deprecated alias for create_snapshot(), preserving its task and result."""
        warnings.warn(
            "SnapshotManager.snapshot() is deprecated; use SnapshotManager.create_snapshot() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.create_snapshot(snapshot_env, index, snapshot, recreate_repo=recreate_repo)

    def create_snapshots(self, snapshot_env, indices, snapshot=None, recreate_repo=False):
        """
        Submit one snapshot per index, reusing create_snapshot() for each job.

        Return one tracked batch task with per-index SUCCESS or SKIPPED
        results. Missing source indices are skipped. An unregistered index,
        read-only repository, or snapshot already running blocks the whole batch
        before submission. Runtime failures raise SnapshotBatchError, whose
        results retain successful, skipped, and failed entries. A batch is not atomic.

        Each snapshot defaults to its index name. A custom snapshot name and
        recreate_repo=True require a single index. Indices from the same build
        run sequentially to preserve the native registrar's job updates.
        An empty list returns [] without querying Elasticsearch.
        """
        plan = self._snapshot_plan(snapshot_env, indices, snapshot, recreate_repo)
        env = self.register[snapshot_env]
        blocked = [row for row in plan["indices"] if row["action"] == "blocked"]
        if blocked:
            raise ValueError(f"Cannot create snapshots: {blocked}")
        if not plan["indices"]:
            return []

        progress = [f"{row['index_name']}: PENDING" for row in plan["indices"]]

        async def _create_snapshots():
            locks = {row["build_id"]: asyncio.Lock() for row in plan["indices"] if "build_id" in row}

            async def run(position, row):
                result = {key: row[key] for key in ("index_name", "snapshot", "repository") if key in row}
                if row["action"] == "skip":
                    result.update(status="SKIPPED", reason=row["reason"])
                else:
                    async with locks[row["build_id"]]:
                        progress[position] = f"{row['index_name']}: RUNNING"
                        try:
                            # Give each job its own environment for its logger.
                            worker = copy.copy(self)
                            worker.register = {**self.register, snapshot_env: copy.copy(env)}
                            snapshot_task: asyncio.Task[CumulativeResult] = worker.create_snapshot(
                                snapshot_env, row["index_name"], row["snapshot"], recreate_repo=recreate_repo
                            )
                            outcome = await snapshot_task
                            result.update(
                                status="SUCCESS",
                                result=outcome.data if isinstance(outcome, _SnapshotResult) else outcome,
                            )
                        except Exception as exc:
                            result.update(status="FAILED", error=f"{type(exc).__name__}: {exc}")
                progress[position] = f"{row['index_name']}: {result['status']}"
                return result

            results = await asyncio.gather(*(run(position, row) for position, row in enumerate(plan["indices"])))
            if any(result["status"] == "FAILED" for result in results):
                raise SnapshotBatchError(results)
            return {**plan, "dry_run": False, "indices": results}

        task = self.job_manager.loop.create_task(_create_snapshots())
        task.progress = progress
        return task

    def run_snapshot(self, snapshot_env, index, snapshot=None, recreate_repo=False, *, dry_run: bool = False):
        """
        Run the snapshot command for one index or a list of exact index names.

        Single-index execution delegates to create_snapshot() and returns its task unchanged.
        A list delegates to create_snapshots(), returning one tracked batch task.
        dry_run=True returns a read-only preflight preview without submitting jobs.
        Custom snapshot names and recreate_repo=True require a single index.
        """
        if dry_run:
            return self._snapshot_plan(snapshot_env, index, snapshot, recreate_repo)
        if isinstance(index, str):
            return self.create_snapshot(snapshot_env, index, snapshot, recreate_repo=recreate_repo)
        return self.create_snapshots(snapshot_env, index, snapshot, recreate_repo=recreate_repo)

    def _snapshot_plan(self, snapshot_env, index, snapshot, recreate_repo):
        if snapshot_env not in self.register:
            raise ValueError("Unknown snapshot environment '%s' (see SNAPSHOT_CONFIG)" % snapshot_env)
        env = self.register[snapshot_env]
        names = _snapshot_indices(index)
        if snapshot is not None and not isinstance(snapshot, str):
            raise TypeError("snapshot must be a single snapshot name")
        if len(names) > 1 and snapshot:
            raise ValueError("A custom snapshot name requires a single index")
        if len(names) > 1 and recreate_repo:
            raise ValueError("recreate_repo requires a single index")

        plan = {"snapshot_env": snapshot_env, "indexer_env": env.idxenv, "dry_run": True, "indices": []}
        if not names:
            return plan
        response = env.client.indices.get(
            index=names,
            features=["settings", "aliases"],
            ignore_unavailable=True,
            allow_no_indices=True,
        )
        existing = getattr(response, "body", response)
        if set(existing) - set(names):
            raise ValueError("Pass concrete index names, not aliases")
        for name in names:
            row = {"index_name": name, "snapshot": snapshot or name}
            plan["indices"].append(row)
            if name not in existing:
                row.update(action="skip", reason="index missing on source cluster")
                continue
            try:
                build = env._doc(name)
            except ValueError:
                row.update(action="blocked", reason=f"no Hub index registration under {env.idxenv}")
                continue
            row["build_id"] = build["_id"]
            config = env.repcfg.format(build)
            row["repository"] = config.repo
            row["aliases"] = sorted(existing[name].get("aliases", {}))
            if str(config.get("settings", {}).get("readonly", False)).lower() == "true":
                row.update(action="blocked", reason="repository is read-only")
                continue
            state = Snapshot(env.client, config.repo, row["snapshot"]).state()
            row["snapshot_state"] = state
            if state == "IN_PROGRESS":
                row.update(action="blocked", reason="snapshot is already in progress")
            else:
                row["action"] = "create" if state in {"MISSING", "N/A"} else "replace"
        return plan

    def restore(self, snapshot_env, snapshot, index=None, **kwargs):
        """Restore one snapshot on the selected environment and monitor recovery."""
        if snapshot_env not in self.register:
            raise ValueError("Unknown snapshot environment '%s' (see SNAPSHOT_CONFIG)" % snapshot_env)
        env = self.register[snapshot_env]
        return env.restore(snapshot, index, **kwargs)

    def restore_many(self, snapshot_env, snapshots, index=None, **kwargs):
        """Submit a batch through restore(), retaining every snapshot's outcome.

        Preflight the entire batch before submission. Return a task which waits
        for every recovery, raising RestoreBatchError if any snapshot fails.
        An empty list returns [] without querying Elasticsearch.
        """
        if snapshot_env not in self.register:
            raise ValueError("Unknown snapshot environment '%s' (see SNAPSHOT_CONFIG)" % snapshot_env)
        env = self.register[snapshot_env]
        kwargs = copy.deepcopy(kwargs)
        plan = env._restore_plan(snapshots, index, **kwargs)
        blocked = [row for row in plan["indices"] if row["action"] == "blocked"]
        if blocked:
            raise ValueError(f"Cannot restore snapshots: {blocked}")
        if not plan["indices"]:
            return []
        progress = [f"{row['index_name']}: PENDING" for row in plan["indices"]]

        async def _restore_many():
            async def run(position, row):
                result = {key: row[key] for key in ("snapshot", "repository", "index_name", "snapshot_uuid")}
                progress[position] = f"{row['index_name']}: RUNNING"
                try:
                    restore_task: asyncio.Task = self.restore(
                        snapshot_env, row["snapshot"], row["index_name"], **kwargs
                    )
                    result.update(status="SUCCESS", result=await restore_task)
                except Exception as exc:
                    result.update(status="FAILED", error=f"{type(exc).__name__}: {exc}")
                progress[position] = f"{row['index_name']}: {result['status']}"
                return result

            results = await asyncio.gather(*(run(position, row) for position, row in enumerate(plan["indices"])))
            if any(row["status"] == "FAILED" for row in results):
                raise RestoreBatchError(results)
            return {**plan, "dry_run": False, "indices": results}

        task = self.job_manager.loop.create_task(_restore_many())
        task.progress = progress
        return task

    def run_restore(
        self,
        snapshot_env,
        snapshot,
        index=None,
        *,
        include_aliases=False,
        index_settings=None,
        ignore_index_settings=None,
        recovery_timeout=3600,
        dry_run: bool = False,
    ):
        """Run restore_snapshot for one snapshot or a list, or preview a dry run.

        Select the destination cluster and repository through SNAPSHOT_CONFIG.
        Each snapshot must contain one index; its metadata supplies the default
        destination name. index optionally renames a single destination. Existing
        destination indices and aliases block execution. Source aliases are
        excluded by default. The returned task finishes after recovery, with
        per-snapshot results for batches. dry_run belongs to this command wrapper.
        """
        options = dict(
            include_aliases=include_aliases,
            index_settings=index_settings,
            ignore_index_settings=ignore_index_settings,
            recovery_timeout=recovery_timeout,
        )
        if dry_run:
            if snapshot_env not in self.register:
                raise ValueError("Unknown snapshot environment '%s' (see SNAPSHOT_CONFIG)" % snapshot_env)
            return self.register[snapshot_env]._restore_plan(snapshot, index, **options)
        if isinstance(snapshot, str):
            return self.restore(snapshot_env, snapshot, index, **options)
        return self.restore_many(snapshot_env, snapshot, index, **options)

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

            return self.create_snapshot(snapshot_env, latest_index)

        return self.job_manager.loop.create_task(_())

    def snapshot_info(self, env=None, remote=False):
        return self.snapshot_config

    def list_snapshots(self, env=None, return_db_cols=True, **filters):
        return cleaner.find(  # filters support dotfield
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
        ignoreErrors=False,  # continue deleting snapshots even if an error occurs
        **filters,  # a set of criterions to limit which snapshots are to be cleaned
    ):
        """Delete past snapshots and keep only the most recent ones.

        Examples:
            >>> snapshot_cleanup()
            >>> snapshot_cleanup("s3_outbreak")
            >>> snapshot_cleanup("s3_outbreak", keep=0)
        """

        # filters support dotfield.
        snapshots = cleaner.find(get_src_build(), env=env, keep=keep, group_by=group_by, **filters)

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
        return cleaner.delete(get_src_build(), snapshots, self, ignoreErrors)

    def delete_snapshots(self, snapshots_data, ignoreErrors=False):
        async def delete(environment, snapshot_names):
            if environment == "__no_env__":
                environment = None
            return self.cleanup(
                env=environment, keep=0, dryrun=False, ignoreErrors=ignoreErrors, _id={"$in": snapshot_names}
            )

        async def all_done():
            try:
                await asyncio.gather(*jobs)
                logging.info("success", extra={"notify": True})
            except Exception as e:
                logging.exception("failed: %s" % e, extra={"notify": True})

        jobs = []
        try:
            for environment, snapshot_names in snapshots_data.items():
                job = self.job_manager.submit(partial(delete, environment, snapshot_names))
                jobs.append(job)
            self.job_manager.loop.create_task(all_done())
        except Exception as ex:
            logging.exception("Error while deleting snapshots. error: %s", ex, extra={"notify": True})
        return jobs

    def delete_snapshot_from_db(self, build_name, snapshot_name):
        """
        Delete a snapshot entry from the MongoDB database.

        This method removes the specified snapshot from the build document in the source build collection.
        Called when a snapshot is found to be missing in the snapshot environment during validation.

        Parameters:
            build_name (str): The name of the build from which to delete the snapshot.
            snapshot_name (str): The name of the snapshot to delete.

        Returns:
            None
        """
        collection = get_src_build()
        collection.update_one(
            {"_id": build_name},
            {"$unset": {f"snapshot.{snapshot_name}": 1}},
        )
        logging.info("Snapshot '%s' deleted from build '%s' in MongoDB", snapshot_name, build_name)

    def validate_snapshots(self):
        """
        Validate the snapshots stored in the database.

        This method checks each snapshot in the source build collection to verify whether it exists
        in the corresponding snapshot environment. If a snapshot does not exist in the environment,
        it is removed from the database. This helps keep the database in sync with the actual snapshots
        present in the environments.

        The validation process is executed asynchronously and any errors encountered during validation are logged.

        Returns:
            job (asyncio.Task): The asynchronous task representing the validation process.
        """

        async def validate():
            logging.info("Starting validation of snapshots...")
            collection = get_src_build()
            snapshots = self.list_snapshots(return_db_cols=True)
            errors = []
            snapshots_deleted = 0

            for group in snapshots:
                for snapshot_data in group["items"]:
                    snapshot_name = snapshot_data["_id"]
                    build_name = snapshot_data["build_name"]
                    environment = snapshot_data.get("environment") or snapshot_data["conf"]["indexer"]["env"]

                    if not environment:
                        msg = (
                            f"[{snapshot_name}] Snapshot '{snapshot_name}' does not have an environment "
                            "associated with it. Skipping validation."
                        )
                        logging.warning(msg)
                        errors.append(msg)
                        continue

                    try:
                        env = self.register[environment]
                    except KeyError:
                        msg = (
                            f"[{snapshot_name}] Environment '{environment}' is not registered and "
                            "connection details are unavailable. Consider adding it to the hub "
                            "configuration otherwise manual deletion is required."
                        )
                        logging.error(msg)
                        errors.append(msg)
                        continue

                    build_doc = collection.find_one({"_id": build_name})

                    try:
                        exists = env.snapshot_exists(snapshot_name, build_doc)
                        if not exists:
                            logging.info("Deleting snapshot '%s' from MongoDB", snapshot_name)
                            self.delete_snapshot_from_db(build_name, snapshot_name)
                            snapshots_deleted += 1
                    except Exception as e:
                        msg = f"Error checking snapshot '{snapshot_name}': {str(e)}"
                        logging.exception(msg)
                        errors.append(msg)
            logging.info("Validation of snapshots completed.")
            return {"snapshots_deleted": snapshots_deleted, "errors": errors}

        async def done(job):
            try:
                result = await job
                snapshots_deleted = result.get("snapshots_deleted", 0)
                errors = result.get("errors", [])
                if errors:
                    error_message = "\n".join(errors)
                    logging.error("Validation completed with errors:\n%s", error_message, extra={"notify": True})
                else:
                    logging.info(
                        "Validation successful, %d snapshots deleted.", snapshots_deleted, extra={"notify": True}
                    )
            except Exception as e:
                logging.exception("Validation failed: %s", e, extra={"notify": True})

        try:
            job = self.job_manager.submit(validate)
            self.job_manager.loop.create_task(done(job))
        except Exception as ex:
            logging.exception("Error while submitting validation job: %s", ex, extra={"notify": True})
        return job
