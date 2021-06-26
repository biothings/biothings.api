import asyncio
import copy
import math
import os
import pickle
from copy import deepcopy
from functools import partial
from types import SimpleNamespace

from biothings import config as btconfig
from biothings.hub import INDEXER_CATEGORY, INDEXMANAGER_CATEGORY
from biothings.hub.databuild.backend import (create_backend,
                                             merge_src_build_metadata)
from biothings.utils import es
from biothings.utils.common import (get_class_from_classpath,
                                    get_random_string, iter_n, timesofar,
                                    traverse)
from biothings.utils.es import ESIndexer
from biothings.utils.hub_db import get_src_build
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.mongo import doc_feeder, id_feeder
from config import LOG_FOLDER
from config import logger as logging
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException

from . import indexer_registrar as registrar
from .indexer_task import dispatch_task

# this module has been refactored and simplified
# but it still has a variety of design decisions
# that deserve a second thought.

# TODO
# correct count in hot cold indexer

# TODO
# except Exception as exc:

#     self.logger.exception("indexer_worker failed")
#     exc_fn = os.path.join(btconfig.LOG_FOLDER, "%s.pick" % logger_name)
#     pickle.dump({"exc": e, "ids": ids}, open(exc_fn, "wb"))
#     self.logger.info("Exception and IDs were dumped in pickle file '%s'", exc_fn)
#     raise
# logger_name = "index_%s_%s_batch_%s" % (pindexer.keywords.get(
#     "index", "index"), col_name, batch_num)
# logger, _ = get_logger(logger_name, btconfig.LOG_FOLDER)

# Summary
# -------
# IndexManager: a hub feature, providing top level commands and config environments(env).
# Indexer/ColdHotIndexer: the "index" command, handles jobs, db state and errors.
# .indexer_task.IndexingTask: the data handling component of an indexer, supports modes.
# biothings.utils.es.Indexer: an index-aware elasticsearch client used in a task.
# elasticsearch.Elasticsearch: native elasticsearch python client used in an indexer.


class IndexerException(Exception):
    pass

class ProcessInfo():

    def __init__(self, indexer):
        self.indexer = indexer

    def get_predicates(self):
        return []

    def get_pinfo(self, step="", description=""):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": INDEXER_CATEGORY,
            "source": "%s:%s" % (self.indexer.conf_name, self.indexer.index_name),
            "description": description,
            "step": step
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo


DEFAULT_INDEX_SETTINGS = {
    # as of ES6, include_in_all was removed, we need to create our own "all" field
    "query": {
        "default_field": "_id,all"
    },
    "codec": "best_compression",
    # as of ES6, analysers/tokenizers must be defined in index settings, during creation
    "analysis": {
        "analyzer": {
            # soon deprecated in favor of keyword_lowercase_normalizer
            "string_lowercase": {
                "tokenizer": "keyword",
                "filter": "lowercase"
            },
            "whitespace_lowercase": {
                "tokenizer": "whitespace",
                "filter": "lowercase"
            },
        },
        "normalizer": {
            "keyword_lowercase_normalizer": {
                "filter": ["lowercase"],
                "type": "custom",
                "char_filter": []
            },
        }
    },
}

DEFAULT_INDEX_MAPPINGS = {
    "dynamic": "false",
    "properties": {"all": {'type': 'text'}}
}

def _clean_ids(ids, logger):  # TODO should this be in the task level?
    # can't use a generator, it's going to be pickled TODO: hah?
    cleaned = []
    for _id in ids:
        if not isinstance(_id, str):
            logger.warning(
                "_id '%s' has invalid type (!str), skipped", repr(_id)
            )
            continue
        if len(_id) > 512:  # this is an ES6 limitation
            logger.warning("_id is too long: '%s'", _id)
            continue
        cleaned.append(_id)
    return cleaned


class Indexer():
    """
    Basic indexer, reading documents from a mongo collection (target_name)
    and sending documents to ES.
    """

    def __init__(self, build_doc, indexer_env, target_name, index_name):

        # ----------from----------

        self.backend_url = build_doc.get("backend_url")  # for example, mongo connection URI
        self.target_name = target_name  # for example, mongo collection name

        # -----------to-----------

        self.host = indexer_env["host"]  # like localhost:9200
        self.kwargs = indexer_env["args"]  # es client kws like use_ssl=True

        self.index_name = index_name or target_name  # elasticsearch index name (destination)
        self.index_settings = deepcopy(DEFAULT_INDEX_SETTINGS)
        self.index_mappings = deepcopy(DEFAULT_INDEX_MAPPINGS)

        self.index_mappings["properties"].update(build_doc.get("mapping", {}))
        self.index_mappings["_meta"] = build_doc.get("_meta", {})

        build_config = build_doc.get("build_config", {})
        self.doc_type = build_config.get("doc_type")  # TODO: remove support in ES7
        self.num_shards = build_config.get("num_shards", 1)  # consider having the default auto-inferred from data size
        self.num_replicas = build_config.get("num_replicas", 0)  # this field is almost always 0 for a staging server

        # ----------meta----------
        self.env = indexer_env.get("name")
        self.conf_name = build_config.get("name")
        self.logger, self.logfile = get_logger('index_%s' % self.index_name, LOG_FOLDER)
        self.pinfo = ProcessInfo(self)

    # TODO
    # catch error if anything internal is wrong, like when registering status
    # may need to explicitly do this or add it to defer_to_* in job manager.

    @asyncio.coroutine
    def index(self,
              job_manager,
              steps=("pre", "index", "post"),
              batch_size=10000,
              ids=None,
              mode="index"):
        """
        Build an Elasticsearch index (self.index_name) 
        with data from MongoDB collection (self.target_name).

        "ids" can be passed to selectively index documents.

        "mode" can have the following values:
            - 'purge': will delete index if it exists
            - 'resume': will use existing index and add documents. "ids" can be passed as a list of missing IDs,
                    or, if not pass, ES will be queried to identify which IDs are missing for each batch in
                    order to complete the index.
            - 'merge': will merge data with existing index' documents, used when populated several distinct times (cold/hot merge for instance)
            - None (default): will create a new index, assuming it doesn't already exist
        """

        if isinstance(steps, str):
            steps = [steps]

        assert job_manager
        assert isinstance(steps, (list, tuple))
        assert isinstance(mode, str)

        cnt = 0

        if "pre" in steps:
            self.logger.info("Running pre-index process for index '%s'", self.index_name)
            status = registrar.PreIndexJSR(self, get_src_build())
            status.started()
            try:
                # currently the pre_indexing jobs are quick to perform,
                # thus not scheduling it with threading or multi-processing
                self.pre_index(mode)
            except Exception as exc:
                self.logger.error(str(exc))
                status.failed(str(exc))
                raise
            else:
                status.succeed()

        if "index" in steps:
            self.logger.info("Running indexing process for index '%s'", self.index_name)
            status = registrar.MainIndexJSR(self, get_src_build())
            status.started()
            try:
                # the indexing stage does its own scheduling,
                # there could be multiple jobs because of batching
                cnt = yield from self.do_index(job_manager, batch_size, ids, mode)
            except Exception as exc:
                self.logger.error(str(exc))
                status.failed(str(exc))
                raise
            else:
                status.succeed(index={self.index_name: {"count": cnt}})

        if "post" in steps:
            self.logger.info("Running post-index process for index '%s'", self.index_name)
            status = registrar.PostIndexJSR(self, get_src_build())
            status.started()

            # -------------------- Sebastien's Note --------------------
            # for some reason (like maintaining object's state between pickling).
            # we can't use process there. Need to use thread to maintain that state
            # without building an unmaintainable monster.
            # ----------------------------------------------------------
            pinfo = self.pinfo.get_pinfo("post_index")
            job = yield from job_manager.defer_to_thread(pinfo, self.post_index)

            try:
                res = yield from job

            except Exception as exc:
                self.logger.exception(
                    "Post-index process failed for index '%s':",
                    self.index_name, extra={"notify": True})
                status.failed(str(exc))
                raise

            else:
                self.logger.info(
                    "Post-index process done for index '%s': %s",
                    self.index_name, res)
                status.succeed()

        return {self.index_name: cnt}

    # -------
    #  steps
    # -------

    def pre_index(self, mode):

        es_idxer = ESIndexer(
            self.index_name, self.doc_type,
            self.host, **self.kwargs)

        if mode == "purge":
            if es_idxer.exists_index():
                es_idxer.delete_index()

        elif mode in ("resume", "merge"):
            return

        if es_idxer.exists_index():
            msg = (
                "Index already '%s' exists, (use mode='purge' to "
                "auto-delete it or mode='resume' to add more documents)"
            )
            raise IndexerException(msg % self.index_name)

        es_idxer.create_index(
            {self.doc_type: self.index_mappings},
            self.index_settings)

    @asyncio.coroutine
    def do_index(self, job_manager, batch_size, ids, mode):

        assert batch_size
        target_collection = create_backend(self.backend_url).target_collection

        jobs = []
        docs_scheduled = 0
        docs_finished = 0
        docs_total = target_collection.count()

        if not docs_total:
            self.logger.warning("No documents to index.")
            return

        if ids:
            self.logger.info(
                ("Indexing from '%s' with specific list of _ids, "
                 "create indexer job with batch_size=%d"),
                self.target_name, batch_size)
            # use user provided ids in batch
            id_provider = iter_n(ids, batch_size)
        else:
            self.logger.info(
                "Fetch _ids from '%s', and create indexer job with batch_size=%d",
                self.target_name, batch_size)
            # use ids from the target mongodb collection in batch
            id_provider = id_feeder(target_collection, batch_size, logger=self.logger)

        batches = math.ceil(docs_total / batch_size)
        for batch_num, ids in enumerate(id_provider):
            yield from asyncio.sleep(0.0)

            origcnt = len(ids)
            ids = _clean_ids(ids, self.logger)
            newcnt = len(ids)
            docs_scheduled += newcnt

            if origcnt != newcnt:
                self.logger.warning(
                    "%d document(s) can't be indexed and will be skipped (invalid _id)",
                    origcnt - newcnt)

            _percentage = docs_scheduled / docs_total * 100
            description = "#%d/%d (%.1f%%)" % (batch_num, batches, _percentage)

            self.logger.info(
                "Creating indexer job #%d/%d, to index '%s' %d/%d (%.1f%%)",
                batch_num, batches, self.backend_url, docs_scheduled, docs_total, _percentage)

            pinfo = self.pinfo.get_pinfo(self.target_name, description)
            job = yield from job_manager.defer_to_process(
                pinfo, dispatch_task,
                self.backend_url, ids, mode, batch_num,
                self.index_name, self.doc_type,
                self.host, batch_size, self.num_shards,
                self.num_replicas, **self.kwargs)
            jobs.append(job)

            # TODO
            # propagate error as soon as we know

        self.logger.info("%d jobs created for the indexing step.", len(jobs))
        results = yield from asyncio.gather(*jobs)

        # compute overall inserted/updated records
        # returned values looks like [(num,[]),(num,[]),...]
        docs_finished = sum((val[0] for val in results))

        if docs_total != docs_finished:
            # raise error if counts don't match, but index is still created,
            # fully registered in case we want to use it anyways TODO registered?
            err = ("Merged collection has %d documents "
                   "but %d have been indexed (check logs for more)")
            raise IndexerException(err % (docs_total, docs_finished))

        self.logger.info(
            "Index '%s' successfully created using merged collection %s",
            self.index_name, self.target_name, extra={"notify": True})
        return docs_total

    def post_index(self):
        """
        Override in sub-class to add a post-index process.
        This method will run in a thread (using job_manager.defer_to_thread())
        """
        pass


# TODO Mapping merge should be handled in indexer
# meta merging merge_src_build_metadata not used yet..

class ColdHotIndexer(Indexer):
    """
    This indexer works with 2 mongo collections to create a single index.
    - one premerge collection contains "cold" data, which never changes (not updated)
    - another collection contains "hot" data, regularly updated
    Index is created fetching the premerge documents. Then, documents from the hot collection
    are merged by fetching docs from the index, updating them, and putting them back in the index.
    """

    def __init__(self, build_doc, indexer_env, target_name, index_name):
        cold_target = build_doc["build_config"]["cold_collection"]
        cold_build_doc = get_src_build().find_one({'_id': cold_target})
        self.index_name = index_name or target_name

        self.cold = Indexer(cold_build_doc, indexer_env, cold_target, self.index_name)
        self.hot = Indexer(build_doc, indexer_env, target_name, self.index_name)

    @asyncio.coroutine
    def index(self,
              job_manager,
              steps=["index", "post"],
              batch_size=10000,
              ids=None,
              mode="index"):
        """
        Same as Indexer.index method but works with a cold/hot collections strategy: first index the cold collection then
        complete the index with hot collection (adding docs or merging them in existing docs within the index)
        """
        assert job_manager
        if isinstance(steps, str):
            steps = [steps]

        cnt = 0
        if "index" in steps:
            # ---------------- Sebastian's Note ---------------
            # selectively index cold then hot collections, using default index method
            # but specifically 'index' step to prevent any post-process before end of
            # index creation
            # Note: copy backend values as there are some references values between cold/hot and build_doc
            cold_task = self.cold.index(job_manager, ("pre", "index"), batch_size, ids, mode)
            cnt = yield from cold_task
            hot_task = self.hot.index(job_manager, "index", batch_size, ids, "merge")
            cnt = yield from hot_task
        if "post" in steps:
            # use super index but this time only on hot collection (this is the entry point, cold collection
            # remains hidden from outside)
            self.hot.post_index()

        return {self.index_name: cnt}


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
        self._build = get_src_build()
        self._config = {}  # config.INDEX_CONFIG if it's a dict
        # self.register = {} # env value in config.py (inherited)

        self.logger, self.logfile = get_logger('indexmanager', LOG_FOLDER)

    def clean_stale_status(self):  # LIFECYCLE CALL
        registrar.IndexJobStatusRegistrar.prune(get_src_build())

    def get_predicates(self):
        def no_other_indexmanager_step_running(job_manager):
            """IndexManager deals with snapshot, publishing,
            none of them should run more than one at a time"""
            return len([
                j for j in job_manager.jobs.values()
                if j["category"] == INDEXMANAGER_CATEGORY
            ]) == 0

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
            "description": ""
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def configure(self, conf):  # LIFECYCLE CALL
        if not isinstance(conf, dict):
            raise TypeError((
                "Unknown indexer definitions type "
                "(expecting a list or a dict)"
            ))
        self._config = copy.deepcopy(conf)
        for name, envconf in conf["env"].items():
            idxkwargs = dict(envconf["indexer"])
            idxkwargs["host"] = envconf["host"]
            self.register[name] = idxkwargs
        self.logger.info(self.register)

    def _find_indexer(self, target_name=None):
        """
        Return indexer class required to index target_name.
        Rules depend on what's inside the corresponding src_build doc
        and the indexers definitions
        """

        name = None
        indexers = self._config.get("indexer_select", {})
        doc = self._build.find_one({
            "_id": target_name
        }) or {}

        for path_in_doc, _ in traverse(doc, True):
            if path_in_doc in indexers:
                if not name:
                    name = path_in_doc
                else:
                    _ERR = "Multiple indexers matched."
                    raise RuntimeError(_ERR)

        try:
            strklass = indexers[name]
            klass = get_class_from_classpath(strklass)
        except Exception:
            self.logger.debug("Using default indexer.")
            klass = self.DEFAULT_INDEXER

        return klass

    def index(self,
              indexer_env,  # elasticsearch env
              target_name,  # source mongodb collection
              index_name=None,  # elasticsearch index name
              ids=None,  # document ids
              **kwargs):
        """
        Trigger an index creation to index the collection target_name and create an
        index named index_name (or target_name if None). Optional list of IDs can be
        passed to index specific documents.
        """

        indexer_env_ = dict(self[indexer_env])  # describes destination
        indexer_env_['name'] = indexer_env
        build_doc = self._build.find_one({'_id': target_name})  # describes source

        if not build_doc:
            raise ValueError("Cannot find %s" % target_name)
        if "build_config" not in build_doc:
            raise ValueError("Cannot find build config associated with '%s'" % target_name)

        def indexed(f):
            try:
                res = f.result()
                self.logger.info(
                    "Done indexing target '%s' to index '%s': %s",
                    target_name, index_name, res)
            except Exception:
                self.logger.exception("Error while running index job:")
                raise

        idx = self._find_indexer(target_name)
        idx = idx(build_doc, indexer_env_, target_name, index_name)
        job = idx.index(self.job_manager, ids=ids, **kwargs)
        job = asyncio.ensure_future(job)
        job.add_done_callback(indexed)

        return job

    def update_metadata(self,
                        indexer_env,
                        index_name,
                        build_name=None,
                        _meta=None):
        """
        Update _meta for index_name, based on build_name (_meta directly
        taken from the src_build document) or _meta
        """
        idxkwargs = self[indexer_env]
        # 1st pass we get the doc_type (don't want to ask that on the signature...)
        indexer = create_backend((idxkwargs["es_host"], index_name, None)).target_esidxer
        m = indexer._es.indices.get_mapping(index_name)
        assert len(m[index_name]["mappings"]) == 1, "Found more than one doc_type: " + \
            "%s" % m[index_name]["mappings"].keys()
        doc_type = list(m[index_name]["mappings"].keys())[0]
        # 2nd pass to re-create correct indexer
        indexer = create_backend((idxkwargs["es_host"], index_name, doc_type)).target_esidxer
        if build_name:
            build = get_src_build().find_one({"_id": build_name})
            assert build, "No such build named '%s'" % build_name
            _meta = build.get("_meta")
        assert _meta is not None, "No _meta found"
        return indexer.update_mapping_meta({"_meta": _meta})

    def index_info(self, env=None, remote=False):

        # remarks
        # where is this used externally?

        # return self.config with optionally enhanced index information
        conf = copy.deepcopy(self._config)

        # these two parameters are processed as they are intended in
        # the previous version of this module for compatibility.
        if env and remote:
            try:
                host = conf["env"][env]["host"]
                client = Elasticsearch(host, timeout=1, max_retries=0)

                # add these index info to env.index
                indices = [{
                    "index": k,
                    "doc_type": None,  # no longer supported
                    "aliases": list(v["aliases"].keys())
                } for k, v in client.indices.get("*").items()]

                # set the "index" key under "env"
                if "index" not in conf["env"][env]:
                    conf["env"][env]["index"] = []

                elif isinstance(conf["env"][env]["index"], dict):
                    # ------------------------------------------------
                    # no idea what "index" should look like.
                    # no idea what these comments mean.
                    # -------------- sebastian's note  ---------------
                    # we don't where to put those indices because we don't
                    # have that information, so we just put those in a default category
                    # TODO: put that info in metadata ?
                    # ------------------------------------------------
                    conf["env"][env]["index"].setdefault(None, indices)

                elif isinstance(conf["env"][env]["index"], list):
                    conf["env"][env]["index"].extend(indices)
                else:
                    raise TypeError("Unsupported env.index")

            except KeyError:
                self.logger.error("Env doesn't exist.")

            except ElasticsearchException:
                self.logger.exception("Can't load remote indices:")

        return conf

    def validate_mapping(self, mapping, env):

        indexer = self._find_indexer()  # get default
        indexer = indexer({}, self[env], None, None)  # instantiate

        host = indexer.host
        settings = indexer.index_settings

        # generate a random index, it'll be deleted at the end
        index_name = ("hub_tmp_%s" % get_random_string()).lower()
        idxr = ESIndexer(index=index_name, es_host=host)

        self.logger.info(
            ("Testing mapping by creating index "
             "'%s' on host '%s' (settings: %s)"),
            index_name, host, settings)

        try:
            res = idxr.create_index(mapping, settings)
        except Exception as e:
            self.logger.exception("create_index failed")
            raise e
        else:
            return res
        finally:
            try:
                idxr.delete_index()
            except Exception:
                pass


class DynamicIndexerFactory():
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
                "index": name + suffix
            }

    def create(self, name):
        conf = self.bynames[name]
        pidxr = partial(ESIndexer, index=conf["index"],
                        doc_type=None,
                        es_host=conf["es_host"])
        conf = {"es_host": conf["es_host"], "index": conf["index"]}
        return pidxr, conf
