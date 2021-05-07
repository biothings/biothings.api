import logging
import os
import time
from datetime import datetime
from enum import Enum
from types import SimpleNamespace

from biothings.utils.common import timesofar


def merge(x, dx):
    """
    Merge dictionary dx (Δx) into dictionary x.
    If __REPLACE__ key is present in any level z in dx,
    z in x is replaced, instead of merged, with z in dx.
    """
    assert isinstance(x, dict)
    assert isinstance(dx, dict)

    if "__REPLACE__" in dx.keys():
        if dx.pop("__REPLACE__"):
            x.clear()
            # merge v with "nothing" just to
            # make sure to remove any "__REPLACE__"
            _y = {}
            merge(_y, dx)
            x.update(_y)
            return

    for k, v in dx.items():
        if isinstance(v, dict):
            if not isinstance(x.get(k), dict):
                x[k] = {}
            merge(x[k], v)
        else:
            x[k] = v

def test_merge_0():
    x = {}
    y = {}
    merge(x, y)
    print(x)

def test_merge_1():
    x = {
        "index": {
            "name1": {
                "doc_type": "news",
                "happy": False
            }
        }
    }
    y = {
        "index": {
            "name1": {
                "happy": True,
                "count": 100
            }
        }
    }
    merge(x, y)
    print(x)

def test_merge_2():
    x = {"a": {"b": "c"}}
    y = {"a": {
        "__REPLACE__": True,
        "b'": {
            "__REPLACE__": False,
            "c": "d"
        }
    }}
    merge(x, y)
    print(x)

def test_merge_3():
    x = {"a": "b"}
    y = {"a": {"b": "c"}}
    merge(x, y)
    print(x)

def test_merge_4():
    x = {"a": {"__REPLACE__": True, "b": "c"}, "__REPLACE__": True}
    y = {"a": {"b": "d"}}
    merge(x, y)
    print(x)

class Stage(Enum):
    READY = 0
    STARTED = 1
    DONE = 2

class IndexJobStatusRegistrar():

    def __init__(self, indexer, collection):
        self.indexer = indexer
        self.collection = collection
        self.stage = Stage.READY
        self.t0 = 0

    @staticmethod
    def prune(collection):
        for build in collection.find():
            dirty = False
            for job in build.get("jobs", []):
                if job.get("status") == "in progress":
                    logging.warning((
                        "Found stale build '%s', "
                        "marking index status as 'cancelled'"),
                        build["_id"])
                    job["status"] = "cancelled"
                    job.pop("pid", None)
                    dirty = True
            if dirty:
                collection.replace_one({"_id": build["_id"]}, build)

    def started(self, step="index"):

        assert self.stage == Stage.READY
        self.stage = Stage.STARTED

        self.t0 = time.time()

        job = {
            "step": step,
            "status": "in progress",
            "step_started_at": datetime.now().astimezone(),
            "logfile": self.indexer.logfile,
            "pid": os.getpid()
        }
        self.collection.update(
            {"_id": self.indexer.target_name},
            {"$push": {
                "jobs": job
            }}
        )

    def failed(self, error):
        def func(job, delta_build):
            job["status"] = "failed"
            job["err"] = str(error)
        self._done(func)

    def succeed(self, **result):
        def func(job, delta_build):
            job["status"] = "success"
            merge(delta_build, result)
        self._done(func)

    def _done(self, func):

        assert self.stage == Stage.STARTED
        self.stage = Stage.DONE

        build = self.collection.find_one({'_id': self.indexer.target_name})
        assert build, "Can't find build document '%s'" % self.indexer.target_name

        job = build["jobs"][-1]
        job["time"] = timesofar(self.t0)
        job["time_in_s"] = round(time.time() - self.t0, 0)
        job.pop("pid")

        delta_build = {}
        func(job, delta_build)
        merge(build, delta_build)
        self.collection.replace_one({"_id": build["_id"]}, build)


class MainIndexJSR(IndexJobStatusRegistrar):

    def started(self):
        super().started('index')

    def succeed(self, **result):

        # after finishing the inital indexing
        # save the index metadata to field "index"

        delta_build = {
            "index": {
                self.indexer.index_name: {
                    '__REPLACE__': True,
                    'host': self.indexer.host,
                    'environment': self.indexer.env,
                    'conf_name': self.indexer.conf_name,
                    'target_name': self.indexer.target_name,
                    'index_name': self.indexer.index_name,
                    'doc_type': self.indexer.doc_type,
                    'num_shards': self.indexer.num_shards,
                    'num_replicas': self.indexer.num_replicas,
                    'created_at': datetime.now().astimezone()
                }
            }
        }
        merge(delta_build, result)
        super().succeed(**delta_build)

class PostIndexJSR(IndexJobStatusRegistrar):

    def started(self):
        super().started('post-index')


def test_registrar():
    from pymongo import MongoClient
    indexer = SimpleNamespace(
        host='localhost:9200',
        target_name="mynews_202012280220_vsdevjdk",  # must exists in DB
        index_name="__index_name__",
        doc_type='news',
        num_shards=1,
        num_replicas=0,
        logfile='/log/file',
        conf_name='bc_news',
        env='dev'
    )
    collection = MongoClient().biothings.src_build
    IndexJobStatusRegistrar.prune(collection)

    # ----------
    #  round 1
    # ----------

    job = MainIndexJSR(indexer, collection)

    input()
    job.started()
    input()
    job.failed("MockErrorA")
    input()
    try:
        job.succeed()
    except Exception as exc:
        print(exc)

    # ----------
    #  round 2
    # ----------

    job = MainIndexJSR(indexer, collection)

    input()
    job.started()
    input()
    job.succeed(index={"__index_name__": {"count": "99"}})

    # ----------
    #  round 3
    # ----------

    job = PostIndexJSR(indexer, collection)

    input()
    try:
        job.succeed()
    except Exception as exc:
        print(exc)

    input()
    job.started()

    input()
    job.succeed(index={"__index_name__": {"additionally": "done"}})


if __name__ == '__main__':
    test_registrar()
