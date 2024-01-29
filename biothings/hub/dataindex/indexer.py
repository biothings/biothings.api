import abc
import asyncio
import copy
import os
from collections import UserDict
from copy import deepcopy
from datetime import datetime
from functools import partial
from typing import NamedTuple, Optional

import elasticsearch
from elasticsearch import AsyncElasticsearch

from biothings import config as btconfig
from biothings.hub import INDEXER_CATEGORY, INDEXMANAGER_CATEGORY
from biothings.hub.databuild.backend import merge_src_build_metadata
from biothings.utils.common import get_class_from_classpath, get_random_string, iter_n, merge, traverse
from biothings.utils.es import ESIndexer
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.mongo import DatabaseClient, id_feeder

from .indexer_cleanup import Cleaner
from .indexer_payload import DEFAULT_INDEX_MAPPINGS, DEFAULT_INDEX_SETTINGS, IndexMappings, IndexSettings
from .indexer_registrar import IndexJobStateRegistrar, MainIndexJSR, PostIndexJSR, PreIndexJSR
from .indexer_schedule import Schedule
from .indexer_task import dispatch

# Summary
# -------
# IndexManager: a hub feature, providing top level commands and config environments(env).
# Indexer/ColdHotIndexer: the "index" command, handles jobs, db state and errors.
# .indexer_task.IndexingTask: index a set of ids, running independent of the hub.

# TODO
# Clarify returned result
# Distinguish creates/updates/deletes
# So that hot/cold indexer doc count can be accurate

# TODO
# Multi-layer logging


class IndexerException(Exception):
    ...


class ProcessInfo:
    def __init__(self, indexer, concurrency):
        self.indexer = indexer
        self.concurrency = concurrency

    def get_predicates(self):
        def limit_indexer_concurrency(job_manager):
            def by_indexer_environment(job):
                return all(
                    (
                        job["category"] == INDEXER_CATEGORY,
                        job["source"] == self.indexer.env_name,
                    )
                )

            return len(list(filter(by_indexer_environment, job_manager.jobs.values()))) < self.concurrency

        return [limit_indexer_concurrency]

    def get_pinfo(self, step="", description=""):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "__predicates__": self.get_predicates(),
            "category": INDEXER_CATEGORY,
            "source": self.indexer.env_name,
            "description": description,
            "step": step,
        }
        return pinfo


class _BuildBackend(NamedTuple):  # mongo
    args: dict = {}
    dbs: Optional[str] = None
    col: Optional[str] = None


class _BuildDoc(UserDict):
    """Represent A Build Under "src_build" Collection.

    Example:
    {
        "_id":"mynews_202105261855_5ffxvchx",
        "target_backend": "mongo",
        "target_name": "mynews_202105261855_5ffxvchx", # UNUSED
        "backend_url": "mynews_202105261855_5ffxvchx",
        "build_config": {
            "_id": "mynews",
            "name": "mynews",
            "doc_type": "news",
            ...
            "cold_collection": "mynews_202012280220_vsdevjdk"
        },
        "mapping": {
            "author": {"type": "text" },
            "title": {"type": "text" },
            "description": {"type": "text" },
            ...
        },
        "_meta": {
            "biothing_type": "news",
            "build_version": "202105261855",
            "build_date": "2021-05-26T18:55:00.054622+00:00",
            ...
        },
        ...
    }
    """

    @property
    def build_name(self):
        return self.get("_id")

    @property
    def build_config(self):
        return self.setdefault("build_config", {})

    def enrich_mappings(self, mappings):
        mappings["__hub_doc_type"] = self.build_config.get("doc_type")
        mappings["properties"].update(self.get("mapping", {}))
        mappings["_meta"] = self.get("_meta", {})

    def enrich_settings(self, settings):
        settings["number_of_shards"] = self.build_config.get("num_shards", 1)
        settings["number_of_replicas"] = self.build_config.get("num_replicas", 0)
        # this feature may be removed at any time
        settings.update(self.build_config.get("extra_index_settings", {}))

    def parse_backend(self):
        # Support Sebastian's hub style backend URI
        # #biothings.hub.databuild.backend.create_backend
        backend = self.get("target_backend")
        backend_url = self.get("backend_url")

        # Case 1:
        # As a dummy indexer
        # Used in validate_mapping, ...

        if backend is None:
            return _BuildBackend()

        # Case 2:
        # Most common setup
        # Index a merged collection

        elif backend == "mongo":
            from biothings.hub.databuild import backend

            db = backend.mongo.get_target_db()
            if backend_url in db.list_collection_names():
                return _BuildBackend(
                    dict(zip(("host", "port"), db.client.address)),
                    db.name,
                    backend_url,
                )

        # Case 3:
        # For single source build_config(s)
        # Index the source collection directly

        elif backend == "link":
            from biothings.hub.databuild import backend

            if backend_url[0] == "src":
                db = backend.mongo.get_src_db()
            else:  # backend_url[0] == "target"
                db = backend.mongo.get_target_db()

            if backend_url[1] in db.list_collection_names():
                return _BuildBackend(
                    dict(zip(("host", "port"), db.client.address)),
                    db.name,
                    backend_url[1],
                )

        raise ValueError(backend, backend_url)

    def extract_coldbuild(self):
        cold_target = self.build_config["cold_collection"]
        cold_build_doc = get_src_build().find_one({"_id": cold_target})
        cold_build_doc = _BuildDoc(cold_build_doc)

        cold_build_doc["_id"] = self.build_name  # *
        cold_build_doc["mapping"].update(self["mapping"])  # combine mapping
        merge_src_build_metadata([cold_build_doc, self])  # combine _meta

        # * About State Updates
        # All updates are diverted to the hot collection.
        # Indices & snapshots are only registered there.

        if self.build_config.get("num_shards"):
            cold_build_doc.build_config["num_shards"] = self.build_config["num_shards"]
        if self.build_config.get("num_replicas"):
            cold_build_doc.build_config["num_replicas"] = self.build_config["num_replicas"]
        return cold_build_doc


class Step(abc.ABC):
    name: property(abc.abstractmethod(lambda _: ...))
    state: property(abc.abstractmethod(lambda _: ...))
    method: property(abc.abstractmethod(lambda _: ...))
    catelog = dict()

    @staticmethod
    def order(steps):
        if isinstance(steps, str):
            return (yield from Step.order([steps]))
        for _step in ("pre", "index", "post"):
            if _step in steps:
                yield _step

    def __init__(self, indexer):
        self.indexer = indexer
        self.state = self.state(
            get_src_build(),
            indexer.build_name,
            indexer.es_index_name,
            logfile=indexer.logfile,
        )

    @classmethod
    def __init_subclass__(cls):
        cls.catelog[cls.name] = cls

    @classmethod
    def dispatch(cls, name):
        return cls.catelog[name]

    async def execute(self, *args, **kwargs):
        coro = getattr(self.indexer, self.method)
        coro = coro(*args, **kwargs)
        return await coro

    def __str__(self):
        return f"<Step name='{self.name}' indexer={self.indexer}>"


class PreIndexStep(Step):
    name = "pre"
    state = PreIndexJSR
    method = "pre_index"


class MainIndexStep(Step):
    name = "index"
    state = MainIndexJSR
    method = "do_index"


class PostIndexStep(Step):
    name = "post"
    state = PostIndexJSR
    method = "post_index"


class _IndexerResult(UserDict):
    def __str__(self):
        return f"{type(self).__name__}({str(self.data)})"


class IndexerCumulativeResult(_IndexerResult):
    ...


class IndexerStepResult(_IndexerResult):
    ...


class Indexer:
    """
    MongoDB -> Elasticsearch Indexer.
    """

    def __init__(self, build_doc, indexer_env, index_name):
        # build_doc primarily describes the source.
        # indexer_env primarily describes the destination.

        _build_doc = _BuildDoc(build_doc)
        _build_backend = _build_doc.parse_backend()

        # ----------source----------

        self.mongo_client_args = _build_backend.args
        self.mongo_database_name = _build_backend.dbs
        self.mongo_collection_name = _build_backend.col

        # -----------dest-----------

        # [1] https://elasticsearch-py.readthedocs.io/en/v7.12.0/api.html#elasticsearch.Elasticsearch
        # [2] https://elasticsearch-py.readthedocs.io/en/v7.12.0/helpers.html#elasticsearch.helpers.bulk
        self.es_client_args = indexer_env.get("args", {})  # See [1] for available args
        self.es_blkidx_args = indexer_env.get("bulk", {})  # See [2] for available args
        self.es_index_name = index_name or _build_doc.build_name
        self.es_index_settings = IndexSettings(deepcopy(DEFAULT_INDEX_SETTINGS))
        self.es_index_mappings = IndexMappings(deepcopy(DEFAULT_INDEX_MAPPINGS))

        _build_doc.enrich_settings(self.es_index_settings)
        _build_doc.enrich_mappings(self.es_index_mappings)

        # -----------info-----------

        self.env_name = indexer_env.get("name")
        self.conf_name = _build_doc.build_config.get("name")
        self.build_name = _build_doc.build_name

        self.setup_log()
        self.pinfo = ProcessInfo(self, indexer_env.get("concurrency", 10))

    def setup_log(self):
        log_folder = (
            os.path.join(btconfig.LOG_FOLDER, "build", self.build_name or "", "index") if btconfig.LOG_FOLDER else None
        )
        log_name = f"index_{self.es_index_name}"
        self.logger, self.logfile = get_logger(log_name, log_folder=log_folder, force=True)

    def __str__(self):
        showx = self.mongo_collection_name != self.es_index_name
        lines = [
            f"<{type(self).__name__}",
            f" source='{self.mongo_collection_name}'" if showx else "",
            f" dest='{self.es_index_name}'>",
        ]
        return "".join(lines)

    # --------------
    #  Entry Point
    # --------------
    async def index(self, job_manager, **kwargs):
        """
        Build an Elasticsearch index (self.es_index_name)
        with data from MongoDB collection (self.mongo_collection_name).

        "ids" can be passed to selectively index documents.

        "mode" can have the following values:
            - 'purge': will delete an index if it exists.
            - 'resume': will use an existing index and add missing documents.
            - 'merge': will merge data to an existing index.
            - 'index' (default): will create a new index.
        """

        steps = kwargs.pop("steps", ("pre", "index", "post"))
        batch_size = kwargs.setdefault("batch_size", 10000)
        # mode = kwargs.setdefault("mode", "index")
        kwargs.setdefault("mode", "index")
        ids = kwargs.setdefault("ids", None)

        assert job_manager
        assert all(isinstance(_id, str) for _id in ids) if ids else True
        assert 500 <= batch_size <= 10000, '"batch_size" out-of-range'

        # the batch size here controls only the task partitioning
        # it does not affect how the elasticsearch python client
        # makes batch requests. a number larger than 10000 may exceed
        # es result window size and doc_feeder maximum fetch size.
        # a number smaller than chunk_size is too small that the docs
        # can be sent to elasticsearch within one request, making it
        # inefficient, amplifying the scheduling overhead.

        x = IndexerCumulativeResult()
        for step in Step.order(steps):
            step = Step.dispatch(step)(self)
            self.logger.info(step)
            step.state.started()
            try:
                dx = await step.execute(job_manager, **kwargs)
                dx = IndexerStepResult(dx)
            except Exception as exc:
                _exc = str(exc)[:500]
                self.logger.exception(_exc)
                step.state.failed(_exc)
                raise exc
            else:
                merge(x.data, dx.data)
                self.logger.info(dx)
                self.logger.info(x)
                step.state.succeed(x.data)

        return x

    # ---------
    #   Steps
    # ---------
    async def pre_index(self, *args, mode, **kwargs):
        client = AsyncElasticsearch(**self.es_client_args)
        try:
            if mode in ("index", None):
                # index MUST NOT exist
                # ----------------------

                if await client.indices.exists(self.es_index_name):
                    msg = (
                        "Index '%s' already exists, (use mode='purge' to "
                        "auto-delete it or mode='resume' to add more documents)"
                    )
                    raise IndexerException(msg % self.es_index_name)

            elif mode in ("resume", "merge"):
                # index MUST exist
                # ------------------

                if not (await client.indices.exists(self.es_index_name)):
                    raise IndexerException("'%s' does not exist." % self.es_index_name)
                self.logger.info(("Exists", self.es_index_name))
                return  # skip index creation

            elif mode == "purge":
                # index MAY exist
                # -----------------

                response = await client.indices.delete(self.es_index_name, ignore_unavailable=True)
                self.logger.info(("Deleted", self.es_index_name, response))

            else:
                raise ValueError("Invalid mode: %s" % mode)

            response = await client.indices.create(
                self.es_index_name,
                body={
                    "settings": (await self.es_index_settings.finalize(client)),
                    "mappings": (await self.es_index_mappings.finalize(client)),
                },
            )
            self.logger.info(("Created", self.es_index_name, response))
            return {
                "__REPLACE__": True,
                "host": self.es_client_args.get("hosts"),  # for frontend display
                "environment": self.env_name,  # used in snapshot module.
            }

        finally:
            await client.close()

    async def do_index(self, job_manager, batch_size, ids, mode, **kwargs):
        client = DatabaseClient(**self.mongo_client_args)
        database = client[self.mongo_database_name]
        collection = database[self.mongo_collection_name]

        if ids:
            self.logger.info(
                ("Indexing from '%s' with specific list of _ids, " "create indexer job with batch_size=%d."),
                self.mongo_collection_name,
                batch_size,
            )
            # use user provided ids in batch
            id_provider = iter_n(ids, batch_size)
        else:
            self.logger.info(
                ("Fetch _ids from '%s', and create " "indexer job with batch_size=%d."),
                self.mongo_collection_name,
                batch_size,
            )
            # use ids from the target mongodb collection in batch
            id_provider = id_feeder(collection, batch_size, logger=self.logger)

        jobs = []  # asyncio.Future(s)
        error = None  # the first Exception

        total = len(ids) if ids else collection.count()
        schedule = Schedule(total, batch_size)

        def batch_finished(future):
            nonlocal error
            try:
                schedule.finished += future.result()
            except Exception as exc:
                self.logger.warning(exc)
                error = exc

        for batch_num, ids in zip(schedule, id_provider):
            await asyncio.sleep(0.0)

            # when one batch failed, and job scheduling has not completed,
            # stop scheduling and cancel all on-going jobs, to fail quickly.

            if error:
                for job in jobs:
                    if not job.done():
                        job.cancel()
                raise error

            self.logger.info(schedule)

            pinfo = self.pinfo.get_pinfo(schedule.suffix(self.mongo_collection_name))

            job = await job_manager.defer_to_process(
                pinfo,
                dispatch,
                self.mongo_client_args,
                self.mongo_database_name,
                self.mongo_collection_name,
                self.es_client_args,
                self.es_blkidx_args,
                self.es_index_name,
                ids,
                mode,
                batch_num,
            )
            job.add_done_callback(batch_finished)
            jobs.append(job)

        self.logger.info(schedule)
        await asyncio.gather(*jobs)

        schedule.completed()
        self.logger.notify(schedule)
        return {"count": total, "created_at": datetime.now().astimezone()}

    async def post_index(self, *args, **kwargs):
        ...


class ColdHotIndexer:
    """MongoDB to Elasticsearch 2-pass Indexer.
    (
        1st pass: <MongoDB Cold Collection>, # static data
        2nd pass: <MongoDB Hot Collection> # changing data
    ) =>
        <Elasticsearch Index>
    """

    # "ColdHotIndexer" is not a subclass of the "Indexer".
    # Step-level customization requires a subclass of "Indexer"
    # and assigning it to the "INDEXER" class attribute below.

    INDEXER = Indexer

    def __init__(self, build_doc, indexer_env, index_name):
        hot_build_doc = _BuildDoc(build_doc)
        cold_build_doc = hot_build_doc.extract_coldbuild()

        self.hot = self.INDEXER(hot_build_doc, indexer_env, index_name)
        self.cold = self.INDEXER(cold_build_doc, indexer_env, self.hot.es_index_name)

    async def index(
        self,
        job_manager,
        batch_size=10000,
        steps=("pre", "index", "post"),
        ids=None,
        mode=None,
        **kwargs,
    ):
        result = []

        cold_task = self.cold.index(
            job_manager,
            steps=set(Step.order(steps)) & {"pre", "index"},
            batch_size=batch_size,
            ids=ids,
            mode=mode,
        )
        result.append((await cold_task))

        hot_task = self.hot.index(
            job_manager,
            steps=set(Step.order(steps)) & {"index", "post"},
            batch_size=batch_size,
            ids=ids,
            mode="merge",
        )
        result.append((await hot_task))

        return result


class IndexManager(BaseManager):
    # An index config is considered a "source" for the manager
    # Each call returns a different instance from a factory call

    DEFAULT_INDEXER = Indexer

    def __init__(self, *args, **kwargs):
        """
        An example of config dict for this module.
        {
            "indexer_select": {
                None: "hub.dataindex.indexer.DrugIndexer", # default
                "build_config.cold_collection" : "mv.ColdHotVariantIndexer",
            },
            "env": {
                "prod": {
                    "host": "localhost:9200",
                    "indexer": {
                        "args": {
                            "timeout": 300,
                            "retry_on_timeout": True,
                            "max_retries": 10,
                        },
                        "bulk": {
                            "chunk_size": 50
                            "raise_on_exception": False
                        },
                        "concurrency": 3
                    },
                    "index": [
                        # for information only, only used in index_info
                        {"index": "mydrugs_current", "doc_type": "drug"},
                        {"index": "mygene_current", "doc_type": "gene"}
                    ],
                },
                "dev": { ... }
            }
        }
        """
        super().__init__(*args, **kwargs)
        self._srcbuild = get_src_build()
        self._config = {}

        self.logger, self.logfile = get_logger("indexmanager")

    # Object Lifecycle Calls
    # --------------------------
    # manager = IndexManager(job_manager)
    # manager.clean_stale_status() # in __init__
    # manager.configure(config)

    def clean_stale_status(self):
        IndexJobStateRegistrar.prune(get_src_build())

    def configure(self, conf):
        if not isinstance(conf, dict):
            raise TypeError(type(conf))
        self._config = conf

        # ES client argument defaults.
        # During heavy indexing, the following settings
        # significantly increase the one-pass success rate.
        esargs = {
            "timeout": 300,
            "retry_on_timeout": True,
            "max_retries": 10,
        }

        # register each indexing environment
        for name, env in conf["env"].items():
            indexer = env.setdefault("indexer", {})
            indexer.setdefault("args", dict(esargs))
            indexer["args"]["hosts"] = env.get("host")
            self.register[name] = deepcopy(indexer)
            self.register[name]["name"] = name

        self.logger.info(self.register)

    # Job Manager Hooks
    # ----------------------

    def get_predicates(self):
        def no_other_indexmanager_step_running(job_manager):
            """IndexManager deals with snapshot, publishing,
            none of them should run more than one at a time"""
            return len([j for j in job_manager.jobs.values() if j["category"] == INDEXMANAGER_CATEGORY]) == 0

        return [no_other_indexmanager_step_running]

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": INDEXMANAGER_CATEGORY,
            "source": "",
            "step": "",
            "description": "",
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    # Hub Features
    # --------------

    def _select_indexer(self, build_name=None):
        """Find the indexer class required to index build_name."""

        rules = self._config.get("indexer_select")
        if not rules or not build_name:
            self.logger.debug(self.DEFAULT_INDEXER)
            return self.DEFAULT_INDEXER

        # the presence of a path in the build doc
        # can determine the indexer class to use.

        path = None
        doc = self._srcbuild.find_one({"_id": build_name})
        for path_in_doc, _ in traverse(doc or dict(), True):
            if path_in_doc in rules:
                if not path:
                    path = path_in_doc
                else:
                    _ERR = "Multiple indexers matched."
                    raise RuntimeError(_ERR)

        kls = get_class_from_classpath(rules[path])
        self.logger.info(kls)
        return kls

    def index(self, indexer_env, build_name, index_name=None, ids=None, **kwargs):
        """
        Trigger an index creation to index the collection build_name and create an
        index named index_name (or build_name if None). Optional list of IDs can be
        passed to index specific documents.
        """

        indexer_env_ = dict(self[indexer_env])  # describes a destination
        build_doc = self._srcbuild.find_one({"_id": build_name})  # describes a source

        if not build_doc:
            raise ValueError("Cannot find build %s." % build_name)
        if not build_doc.get("build_config"):
            raise ValueError("Cannot find build config for '%s'." % build_name)

        idx = self._select_indexer(build_name)
        idx = idx(build_doc, indexer_env_, index_name)
        job = idx.index(self.job_manager, ids=ids, **kwargs)
        job = asyncio.ensure_future(job)
        job.add_done_callback(self.logger.debug)

        return job

    def update_metadata(
        self,
        indexer_env,
        index_name,
        build_name=None,
        _meta=None,
    ):
        """
        Update _meta field of the index mappings, basing on
            1. the _meta value provided, including {}.
            2. the _meta value of the build_name in src_build.
            3. the _meta value of the build with the same name as the index.

        Examples:
            update_metadata("local", "mynews_201228_vsdevjd")
            update_metadata("local", "mynews_201228_vsdevjd", _meta={})
            update_metadata("local", "mynews_201228_vsdevjd", _meta={"author":"b"})
            update_metadata("local", "mynews_201228_current", "mynews_201228_vsdevjd")
        """

        async def _update_meta(_meta):
            env = self.register[indexer_env]
            async with AsyncElasticsearch(**env["args"]) as client:
                doc_type = None
                if int((await client.info())["version"]["number"].split(".")[0]) < 7:
                    mappings = client.indices.get_mapping(index_name)
                    mappings = mappings[index_name]["mappings"]
                    doc_type = next(iter(mappings.keys()))

                if _meta is None:
                    _id = build_name or index_name  # best guess
                    build = get_src_build().find_one({"_id": _id})
                    _meta = (build or {}).get("_meta")

                return await client.indices.put_mapping(
                    body=dict(_meta=_meta),
                    index=index_name,
                    doc_type=doc_type,
                )

        job = asyncio.ensure_future(_update_meta(_meta))
        job.add_done_callback(self.logger.debug)
        return job

    def index_info(self, remote=False):
        """Show index manager config with enhanced index information."""
        # http://localhost:7080/index_manager

        async def _enhance(conf):
            conf = copy.deepcopy(conf)

            for name, env in self.register.items():
                async with AsyncElasticsearch(**env["args"]) as client:
                    try:
                        indices = await client.indices.get("*")
                    except elasticsearch.exceptions.ConnectionError:
                        ...  # keep the hard-coded place-holders info
                    else:  # replace the index key with remote info
                        conf["env"][name]["index"] = [
                            {
                                "index": k,
                                "aliases": list(v["aliases"].keys()),
                                "doc_type": v["mappings"]["_meta"]["biothing_type"],
                            }
                            for k, v in indices.items()
                        ]
            return conf

        if remote:
            job = asyncio.ensure_future(_enhance(self._config))
            job.add_done_callback(self.logger.debug)
            return job

        return self._config

    def get_indexes_by_name(self, index_name=None, env_name=None, limit=10):
        """Accept an index_name and return a list of indexes get from all elasticsearch environments
        or from specific elasticsearch environment.

        If index_name is blank, it will be return all indexes.
        limit can be used to specify how many indexes should be return.

        The list of indexes will be like this:
        [
            {
                "index_name": "...",
                "build_version": "...",
                "count": 1000,
                "creation_date": 1653468868933,
                "environment": {
                    "name": "env name",
                    "host": "localhost:9200",
                }
            },
        ]
        """

        if not index_name:
            index_name = "*"
        limit = int(limit)

        async def fetch(index_name, env_name=None, limit=None):
            indexes = []
            for _env_name, env in self.register.items():
                # If env_name is set, only fetch indexes for the specific es server
                if env_name and env_name != _env_name:
                    continue

                async with AsyncElasticsearch(**env["args"]) as client:
                    try:
                        indices = await client.indices.get(index_name)
                    except Exception:
                        continue
                    for index_name, index_data in indices.items():
                        mapping_meta = index_data["mappings"]["_meta"]
                        indexes.append(
                            {
                                "index_name": index_name,
                                "build_version": mapping_meta["build_version"],
                                "count": mapping_meta["stats"]["total"],
                                "creation_date": index_data["settings"]["index"]["creation_date"],
                                "environment": {
                                    "name": _env_name,
                                    "host": env["args"]["hosts"],
                                },
                            }
                        )

            indexes.sort(key=lambda index: index["creation_date"], reverse=True)

            if limit:
                indexes = indexes[:limit]
            return indexes

        job = asyncio.ensure_future(fetch(index_name, env_name=env_name, limit=limit))
        job.add_done_callback(self.logger.debug)
        return job

    def validate_mapping(self, mapping, env):
        indexer = self._select_indexer()  # default indexer
        indexer = indexer(dict(mapping=mapping), self[env], None)

        self.logger.debug(indexer.es_client_args)
        self.logger.debug(indexer.es_index_settings)
        self.logger.debug(indexer.es_index_mappings)

        async def _validate_mapping():
            client = AsyncElasticsearch(**indexer.es_client_args)
            index_name = ("hub_tmp_%s" % get_random_string()).lower()
            try:
                return await client.indices.create(
                    index_name,
                    body={
                        "settings": (await indexer.es_index_settings.finalize(client)),
                        "mappings": (await indexer.es_index_mappings.finalize(client)),
                    },
                )
            finally:
                await client.indices.delete(index_name, ignore_unavailable=True)
                await client.close()

        job = asyncio.ensure_future(_validate_mapping())
        job.add_done_callback(self.logger.info)
        return job

    def cleanup(self, env=None, keep=3, dryrun=True, **filters):
        """Delete old indices except for the most recent ones.

        Examples:
            >>> index_cleanup()
            >>> index_cleanup("production")
            >>> index_cleanup("local", build_config="demo")
            >>> index_cleanup("local", keep=0)
            >>> index_cleanup(_id="<elasticsearch_index>")
        """
        if not env and not dryrun:  # low specificity, unsafe.
            raise ValueError('Missing argument "env".')

        cleaner = Cleaner(get_src_build(), self, self.logger)
        cleanups = cleaner.find(env, keep, **filters)

        if dryrun:
            return "\n".join(
                (
                    "-" * 75,
                    cleaner.plain_text(cleanups),
                    "-" * 75,
                    "DRYRUN ONLY - APPLY THE ACTIONS WITH:",
                    "   > index_cleanup(..., dryrun=False)",
                )
            )

        job = asyncio.ensure_future(cleaner.clean(cleanups))
        job.add_done_callback(self.logger.info)
        return job


class DynamicIndexerFactory:
    """
    In the context of autohub/standalone instances, create indexer
    with parameters taken from versions.json URL.
    A list of  URLs is provided so the factory knows how to create these
    indexers for each URLs. There's no way to "guess" an ES host from a URL,
    so this parameter must be specified as well, common to all URLs
    "suffix" param is added at the end of index names.
    """

    def __init__(self, urls, es_host, suffix="_current"):
        self.urls = urls
        self.es_host = es_host
        self.bynames = {}
        for url in urls:
            if isinstance(url, dict):
                name = url["name"]
                # actual_url = url["url"]
            else:
                name = os.path.basename(os.path.dirname(url))
                # actual_url = url
            self.bynames[name] = {
                "es_host": self.es_host,
                "index": name + suffix,
            }

    def create(self, name):
        conf = self.bynames[name]
        pidxr = partial(
            ESIndexer,
            index=conf["index"],
            doc_type=None,
            es_host=conf["es_host"],
        )
        conf = {"es_host": conf["es_host"], "index": conf["index"]}
        return pidxr, conf
