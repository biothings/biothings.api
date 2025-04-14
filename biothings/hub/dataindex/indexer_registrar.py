import logging
import os
import time
from datetime import datetime
from enum import Enum

from biothings.utils.common import merge, timesofar


class Stage(Enum):
    READY = 0
    STARTED = 1
    DONE = 2

    def at(self, stage):
        assert self == stage


# IndexJobStateRegistrar CAN be further generalized
# to replace hub.manager.BaseStatusRegisterer


class IndexJobStateRegistrar:
    def __init__(self, collection, build_name, index_name, **context):
        self.collection = collection
        self.build_id = build_name

        self.index_name = index_name
        self.context = context

        self.stage = Stage.READY
        self.t0 = 0

    @staticmethod
    def prune(collection):
        for build in collection.find():
            dirty = False
            for job in build.get("jobs", []):
                if job.get("status") == "in progress":
                    logging.warning("Found stale build '%s', marking index status as 'cancelled'", build["_id"])

                    job["status"] = "cancelled"
                    job.pop("pid", None)
                    dirty = True

            if dirty:
                collection.replace_one({"_id": build["_id"]}, build)

    def started(self, step="index"):
        self.stage.at(Stage.READY)
        self.stage = Stage.STARTED

        self.t0 = time.time()

        job = {
            "step": step,
            "status": "in progress",
            "step_started_at": datetime.now().astimezone(),
            "pid": os.getpid(),
            **self.context,
        }
        self.collection.update(
            {"_id": self.build_id},
            {"$push": {"jobs": job}},
        )

    def failed(self, error):
        def func(job, delta_build):
            job["status"] = "failed"
            job["err"] = str(error)

        self._done(func)

    def succeed(self, result):
        def func(job, delta_build):
            job["status"] = "success"
            if result:
                delta_build["index"] = {self.index_name: result}

        self._done(func)

    def _done(self, func):
        self.stage.at(Stage.STARTED)
        self.stage = Stage.DONE

        build = self.collection.find_one({"_id": self.build_id})
        assert build, "Can't find build document '%s'" % self.build_id

        job = build["jobs"][-1]
        job["time"] = timesofar(self.t0)
        job["time_in_s"] = round(time.time() - self.t0, 0)
        job.pop("pid")

        delta_build = {}
        func(job, delta_build)
        merge(build, delta_build)
        self.collection.replace_one({"_id": build["_id"]}, build)


class PreIndexJSR(IndexJobStateRegistrar):
    def started(self):
        super().started("pre-index")

    def succeed(self, result):
        # no result registration on pre-indexing step.
        # --------------------------------------------
        # registration indicates the creation of
        # the index on the elasticsearch server.
        # thus failure at the post-index stage means
        # registration of the index state up until the
        # indexing step, but success at the pre-index
        # stage suggests no index created and thus
        # no registration at all.
        super().succeed({})


class MainIndexJSR(IndexJobStateRegistrar):
    def started(self):
        super().started("index")


class PostIndexJSR(IndexJobStateRegistrar):
    def started(self):
        super().started("post-index")
