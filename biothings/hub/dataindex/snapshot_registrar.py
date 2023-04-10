import logging
from datetime import datetime, timezone
from time import time

from biothings.utils.common import merge, timesofar

# NO CONCURRENT
# TASK SUPPORT YET

_map = {
    # "pre": PreSnapshotState,
    # "snapshot": MainSnapshotState,
    # "post": PostSnapshotState
}


def dispatch(step):
    return _map[step]


def audit(src_build, logger=None):
    if not logger:
        logger = logging.getLogger(__name__)

    for build in src_build.find():
        for num, job in enumerate(build.get("jobs", [])):
            if job.get("status") == "in progress":
                job["status"] = "canceled"

                msg = "<Job #%d in <Build '%s'> cancelled>"
                logger.warning(msg, num, build["_id"])

        src_build.replace_one({"_id": build["_id"]}, build)


class _TaskState:
    name = NotImplemented  # string representation of the step
    step = NotImplemented  # job registration display notation
    func = NotImplemented  # method name of the step to run
    regx = False  # ready to update the build doc

    def __init__(self, col, _id):
        self._col = col
        self._id = _id

    @classmethod
    def __init_subclass__(cls):
        _map[cls.name] = cls

    def started(self, **extras):
        timestamp = datetime.now().astimezone()
        self._col.update(
            {"_id": self._id},
            {
                "$push": {
                    "jobs": {
                        "step": self.step,
                        "status": "in progress",
                        "step_started_at": timestamp,
                        **extras,
                    }
                }
            },
        )

    def _finished(self, _doc, _job):
        doc = self._col.find_one({"_id": self._id})
        job = doc["jobs"][-1]
        t0 = job["step_started_at"]

        # New pymongo lib version changed the default configuration value of `tz_aware` to `False` instead of `True`.
        # Because of that the datetime value read from mongodb was not including the timezone anymore.
        # The solution is forcing the datetime value to include the UTC timezone when it is missing.
        # Reference: https://pymongo.readthedocs.io/en/stable/migrate-to-pymongo4.html?highlight=datetime#tz-aware-defaults-to-false
        if not t0.tzinfo:
            t0 = t0.replace(tzinfo=timezone.utc)
        t0 = t0.timestamp()

        job["time_in_s"] = round(time() - t0, 0)
        job["time"] = timesofar(t0)

        if self.regx:
            merge(doc, _doc)
        merge(job, _job)

        self._col.replace_one({"_id": self._id}, doc)

    def failed(self, dBuild, **dJob):
        self._finished({"snapshot": dBuild}, {"status": "failed", **dJob})

    def succeed(self, dBuild, **dJob):
        self._finished({"snapshot": dBuild}, {"status": "success", **dJob})

    def __str__(self):
        return f"<{type(self).__name__} {self._id}>"


class PreSnapshotState(_TaskState):
    name = "pre"
    step = "pre-snapshot"
    func = "pre_snapshot"


class MainSnapshotState(_TaskState):
    name = "snapshot"
    step = "snapshot"
    func = "_snapshot"
    regx = True


class PostSnapshotState(_TaskState):
    name = "post"
    step = "post-snapshot"
    func = "post_snapshot"
    regx = True


def test():
    from pymongo import MongoClient

    client = MongoClient()
    collection = client["biothings"]["src_build"]
    state = PreSnapshotState(collection, "mynews_202105261855_5ffxvchx")
    state.started(logfile="__TEST_LOCATION__")
    state.succeed({"test_snapshot_01": {}})

    state.started(logfile="__TEST_LOCATION__")
    state.failed(ValueError("__TEST_EXCEPTION__"))


if __name__ == "__main__":
    test()
