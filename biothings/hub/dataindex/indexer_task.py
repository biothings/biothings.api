import logging
from collections import namedtuple
from enum import Enum
from functools import partial
from types import SimpleNamespace

from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient

from biothings.utils.es import ESIndex as BaseESIndex
from biothings.utils.loggers import get_logger
from biothings.utils.serializer import to_json

try:
    from biothings.utils.mongo import doc_feeder
except ImportError:
    import biothings

    biothings.config = SimpleNamespace()
    biothings.config.DATA_SRC_DATABASE = "biothings_src"
    biothings.config.DATA_TARGET_DATABASE = "biothings_build"
    from biothings.utils.mongo import doc_feeder

_IDExists = namedtuple("IDExists", ("id", "exists"))
EXISTS_BATCH_SIZE = 1000


def _batch(items, size):
    for start in range(0, len(items), size):
        yield items[start : start + size]


class ESIndex(BaseESIndex):
    def __init__(self, client, index_name, logger=None, **bulk_index_args):
        super().__init__(client, index_name)
        self.bulk_index_args = bulk_index_args
        self.logger = _ensure_logger(logger)

    # --------------------
    # bulk operations (m*)
    # --------------------

    def mget(self, ids):
        """Return a list of documents like
        [
            { "_id": "0", "a": "b" },
            { "_id": "1", "c": "d" },
            # 404s are skipped
        ]
        """
        response = self.client.mget(
            body={"ids": ids},
            index=self.index_name,
        )
        for doc in response["docs"]:
            if doc.get("found"):
                doc["_source"]["_id"] = doc["_id"]
                yield doc["_source"]

    def mexists(self, ids, batch_size=EXISTS_BATCH_SIZE):
        """Yield tuples like
        [
            (_id_0, True),
            (_id_1, False),
            (_id_2, True),
            ....
        ]
        """
        for ids_batch in _batch(ids, batch_size):
            res = self.client.search(
                index=self.index_name,
                body={
                    "query": {"ids": {"values": ids_batch}},
                    "_source": False,
                    "stored_fields": [],
                },
                size=len(ids_batch),
            )
            id_set = {doc["_id"] for doc in res["hits"]["hits"]}
            for _id in ids_batch:
                yield _IDExists(_id, _id in id_set)

    def mindex(self, docs) -> int:
        """Index and return the number of docs indexed."""

        def _action(doc):
            _doc = {
                "_index": self.index_name,
                "_type": self.doc_type,
                "_op_type": "index",
            }
            _doc.update(doc)  # with _id
            return _doc

        try:
            return helpers.bulk(self.client, map(_action, docs), **self.bulk_index_args)[0]
        except helpers.BulkIndexError as e:
            errors = e.errors
            for error in errors:
                _, op_details = next(iter(error.items()))  # e.g., 'index', {...}
                document_id = op_details.get("_id")
                reason = op_details.get("error", {}).get("reason")
                self.logger.error(error)
                self.logger.error("Document ID %s failed: %s", document_id, reason)

            serialized_errors = to_json(errors, indent=True)
            message = (
                f"Bulk indexing failed for index '{self.index_name}'. "
                f"Elasticsearch responded with errors:\n{serialized_errors}"
            )
            raise helpers.BulkIndexError(message, errors) from e

    # NOTE
    # Why doesn't "mget", "mexists", "mindex" belong to the base class?
    # At this moment, their interfaces are too opinionated/customized
    # for usage in this module. Unless we find them directly useful in
    # another module in the future, proving their genericity, they should
    # stay close in this module only.


# Data Collection Client


def _get_es_client(es_client_args, es_blk_args, es_idx_name):
    # Elasticsearch holds an HTTP connection pool and is meant to be created once and
    # shared, not per batch - same reasoning as _get_mg_client below. Keyed via
    # kwargs_cache_key() since es_client_args commonly contains a "hosts" list, which
    # isn't hashable on its own.
    from biothings.utils.mongo import cached_client, kwargs_cache_key

    key = kwargs_cache_key("indexer_task_es_client", es_client_args)
    client = cached_client(key, lambda: Elasticsearch(**es_client_args))
    return ESIndex(client, es_idx_name, **es_blk_args)


def _get_mg_client(mg_client_args, mg_dbs_name, mg_col_name):
    # MongoClient holds a connection pool and is meant to be created once and shared, not
    # per batch: this is called once per indexing batch (potentially thousands per job),
    # dispatched to worker threads/processes sharing the hub process's fd table under
    # HUB_FREE_THREADED_WORKERS, so an uncached client here leaks a full connection pool
    # per batch. Cache keyed on the connection kwargs so distinct targets don't collide.
    from biothings.utils.mongo import cached_client, kwargs_cache_key

    key = kwargs_cache_key("indexer_task_mg_client", mg_client_args)
    client = cached_client(key, lambda: MongoClient(**mg_client_args))
    return client[mg_dbs_name][mg_col_name]


# --------------
#  Entry Point
# --------------


def dispatch(
    mg_client_args,
    mg_dbs_name,
    mg_col_name,
    es_client_args,
    es_blk_args,
    es_idx_name,
    ids,
    mode,
    name,
):
    return IndexingTask(
        partial(_get_es_client, es_client_args, es_blk_args, es_idx_name),
        partial(_get_mg_client, mg_client_args, mg_dbs_name, mg_col_name),
        ids,
        mode,
        f"index_{es_idx_name}",
        name,
    ).dispatch()


def _ensure_logger(logger):
    if not logger:
        return logging.getLogger(__name__)
    if isinstance(logger, str):
        return get_logger(logger)[0]
    return logger


def _validate_ids(ids, logger=None):
    validated_ids = []
    invalid_ids = []
    for _id in ids:
        if not isinstance(_id, str):
            raise TypeError("_id '%s' has invalid type (!str)." % repr(_id))
        if len(_id) > 512:  # this is an ES limitation
            invalid_ids.append(_id)
            message = "_id is too long: '%s'" % _id
            if logger:
                logger.warning(message)
            else:
                print(message)
        else:
            validated_ids.append(_id)
    return validated_ids, invalid_ids


class Mode(Enum):
    INDEX = "index"
    PURGE = "purge"  # same as 'index' in this module
    MERGE = "merge"
    RESUME = "resume"


class IndexingTask:
    """
    Index one batch of documents from MongoDB to Elasticsearch.
    The documents to index are specified by their ids.
    """

    def __init__(self, es, mongo, ids, mode=None, logger=None, name="task"):
        assert callable(es)
        assert callable(mongo)

        self.logger = _ensure_logger(logger)
        self.name = f"#{name}" if isinstance(name, int) else name

        self.ids, self.invalid_ids = _validate_ids(ids, self.logger)
        self.mode = Mode(mode or "index")

        # these are functions to create clients,
        # each also associated with an organizational
        # structure in the corresponding database,
        # functioning as the source or destination
        # of batch document manipulation.
        self.backend = SimpleNamespace()
        self.backend.es = es  # wrt an index
        self.backend.mongo = mongo  # wrt a collection

    def _get_clients(self):
        clients = SimpleNamespace()
        clients.es = self.backend.es()
        clients.mongo = self.backend.mongo()
        return clients

    def _close_clients(self, clients):
        es_client = getattr(clients.es, "client", None)
        if es_client and hasattr(es_client, "close"):
            es_client.close()

        mongo_database = getattr(clients.mongo, "database", None)
        mongo_client = getattr(mongo_database, "client", None)
        if mongo_client and hasattr(mongo_client, "close"):
            mongo_client.close()

    def dispatch(self):
        if self.mode in (Mode.INDEX, Mode.PURGE):
            return self.index()
        elif self.mode == Mode.MERGE:
            return self.merge()
        elif self.mode == Mode.RESUME:
            return self.resume()

    def index(self):
        clients = self._get_clients()
        try:
            count_docs = self._index_ids(clients, self.ids)
            return count_docs + len(self.invalid_ids)
        finally:
            self._close_clients(clients)

    def _index_ids(self, clients, ids):
        if not ids:
            return 0
        docs = doc_feeder(
            clients.mongo,
            step=len(ids),
            inbatch=False,
            query={"_id": {"$in": ids}},
        )
        self.logger.info("%s: %d documents.", self.name, len(ids))
        return clients.es.mindex(docs)

    def merge(self):
        clients = self._get_clients()
        try:
            upd_cnt, docs_old = 0, {}
            new_cnt, docs_new = 0, {}

            # populate docs_old
            for doc in clients.es.mget(self.ids):
                docs_old[doc["_id"]] = doc

            # populate docs_new
            for doc in doc_feeder(
                clients.mongo,
                step=len(self.ids),
                inbatch=False,
                query={"_id": {"$in": self.ids}},
            ):
                docs_new[doc["_id"]] = doc
                doc.pop("_timestamp", None)

            # merge existing ids
            for key in list(docs_new):
                if key in docs_old:
                    docs_old[key].update(docs_new[key])
                    del docs_new[key]

            # updated docs (those existing in col *and* index)
            upd_cnt = clients.es.mindex(docs_old.values())
            self.logger.info("%s: %d documents updated.", self.name, upd_cnt)

            # new docs (only in col, *not* in index)
            new_cnt = clients.es.mindex(docs_new.values())
            self.logger.info("%s: %d new documents.", self.name, new_cnt)

            return upd_cnt + new_cnt
        finally:
            self._close_clients(clients)

    def resume(self):
        clients = self._get_clients()
        try:
            count_ids = len(self.ids) + len(self.invalid_ids)
            missing_ids = [x.id for x in clients.es.mexists(self.ids) if not x.exists]
            self.logger.info("%s: %d missing documents.", self.name, len(missing_ids))
            if missing_ids:
                self._index_ids(clients, missing_ids)
            return count_ids
        finally:
            self._close_clients(clients)
