from functools import partial
import logging
from enum import Enum
from types import SimpleNamespace
from biothings.hub.databuild.backend import create_backend
from biothings.utils.es import ESIndexer

try:
    from biothings.utils.mongo import doc_feeder
except ImportError:
    import biothings
    biothings.config = SimpleNamespace()
    biothings.config.DATA_SRC_DATABASE = 'biothings_src'
    biothings.config.DATA_TARGET_DATABASE = 'biothings_build'
    from biothings.utils.mongo import doc_feeder

def _get_es_client(*args, **kwargs):
    return ESIndexer(*args, **kwargs)

def _get_mongo_client(backend_url):
    return create_backend(backend_url).target_collection

def dispatch_task(backend_url, ids, mode, name, *esargs, **eskwargs):
    task = IndexingTask(
        partial(_get_es_client, *esargs, **eskwargs),
        partial(_get_mongo_client, backend_url),
        ids, mode
    )
    task.name = str(name)
    return task.dispatch()


class Mode(Enum):
    INDEX = 'index'
    PURGE = 'purge'  # same as 'index' in this module
    MERGE = 'merge'
    RESUME = 'resume'

class IndexingTask():

    def __init__(self, es, mongo, ids, mode='index'):

        assert callable(es)
        assert callable(mongo)

        self.ids = ids
        self.mode = Mode(mode)

        # these are functions to create clients,
        # each also associated with an organizational
        # structure in the corresponding database,
        # functioning as the source or destination
        # of batch document manipulation.
        self.backend = SimpleNamespace()
        self.backend.es = es  # wrt an index
        self.backend.mongo = mongo  # wrt a collection

        self.logger = logging.getLogger(__name__)
        self.name = ""  # for logging only

    def _get_clients(self):
        clients = SimpleNamespace()
        clients.es = self.backend.es()
        clients.mongo = self.backend.mongo()
        return clients

    def dispatch(self):
        try:
            if self.mode in (Mode.INDEX, Mode.PURGE):
                return self.index()
            elif self.mode == Mode.MERGE:
                return self.merge()
            elif self.mode == Mode.RESUME:
                return self.resume()
        except Exception:
            self.logger.error("Batch %s indexing failed.", self.name)
            raise

    def index(self):
        clients = self._get_clients()
        docs = doc_feeder(
            clients.mongo,
            step=len(self.ids),
            inbatch=False,
            query={'_id': {
                '$in': self.ids
            }})
        cnt = clients.es.index_bulk(docs)
        return cnt

    def merge(self):
        clients = self._get_clients()

        upd_cnt = 0
        new_cnt = 0

        docs_old = {}
        docs_new = {}

        # populate docs_old
        for doc in clients.es.get_docs(self.ids):
            docs_old['_id'] = doc

        # populate docs_new
        for doc in doc_feeder(
                clients.mongo,
                step=len(self.ids),
                inbatch=False,
                query={'_id': {
                    '$in': self.ids
                }}):
            docs_new[doc['_id']] = doc
            doc.pop("_timestamp", None)

        # merge existing ids
        for key in docs_new:
            if key in docs_old:
                docs_old.update(docs_new[key])
                del docs_new[key]

        # updated docs (those existing in col *and* index)
        upd_cnt = clients.es.index_bulk(docs_old.values(), len(docs_old))
        self.logger.debug("%s documents updated in index", str(upd_cnt))

        # new docs (only in col, *not* in index)
        new_cnt = clients.es.index_bulk(docs_new.values(), len(docs_new))
        self.logger.debug("%s new documents in index", str(new_cnt))

        # need to return one: tuple(cnt,list)
        return (upd_cnt[0] + new_cnt[0], upd_cnt[1] + new_cnt[1])

    def resume(self):
        clients = self._get_clients()
        missing_ids = [x[0] for x in clients.es.mexists(self.ids) if not x[1]]
        if missing_ids:
            self.ids = missing_ids
            return self.index()
        return (0, None)

def test_clients():
    from functools import partial
    from biothings.utils.es import ESIndexer
    from pymongo import MongoClient

    def _pymongo():
        client = MongoClient()
        database = client["biothings_build"]
        return database["mynews_202012280220_vsdevjdk"]

    return (
        partial(ESIndexer, "indexer-test"),
        _pymongo
    )

def test0():
    task = IndexingTask(*test_clients(), (
        "0999b13cb8026aba",
        "1111647aaf9c70b4",
        "1c9828073bad510c"))
    task.index()

def test1():
    task = IndexingTask(*test_clients(), (
        "0999b13cb8026aba",
        "1111647aaf9c70b4",
        "1c9828073bad510c",
        "1f447d7fc6dcc2cf",
        "27e81a308e4e04da"))
    task.resume()


if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    # test0()
    # test1()
