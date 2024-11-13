import datetime
import os
import time
from functools import partial
from pathlib import Path

try:
    import aiocron
except ImportError:
    # Suppress import error when we just run CLI
    pass

from biothings import config
from biothings.utils.common import timesofar
from biothings.utils.manager import JobManager

logger = config.logger


class UnknownResource(Exception):
    pass


class ResourceError(Exception):
    pass


class ManagerError(Exception):
    pass


class ResourceNotFound(Exception):
    pass


class BaseManager:
    def __init__(self, job_manager: JobManager, poll_schedule=None):
        self.register = {}
        self.poll_schedule = poll_schedule
        self.job_manager = job_manager
        self.clean_stale_status()

    def clean_stale_status(self):
        """
        During startup, search for action in progress which would have
        been interrupted and change the state to "canceled".
        Ex: some donwloading processes could have been interrupted, at
        startup, "downloading" status should be changed to "canceled" so
        to reflect actual state on these datasources.
        This must be overriden in subclass.
        """

    def __repr__(self):
        registered = sorted(list(self.register.keys()))
        return "<%s [%d registered]: %s>" % (
            self.__class__.__name__,
            len(self.register),
            registered,
        )

    def __getitem__(self, src_name):
        try:
            # as a main-source
            return self.register[src_name]
        except KeyError:
            try:
                # as a sub-source
                main, sub = src_name.split(".")
                srcs = self.register[main]
                # there can be many uploader for one resource (when each is dealing
                # with one specific file but upload to the same collection for instance)
                # so we want to make sure user is aware of this and not just return one
                # uploader when many are needed
                # on the other hand, if only one avail, just return it
                res = [src for src in srcs if src.name == sub]
                if not res:
                    raise KeyError(src_name)
                return res
            except (ValueError, AttributeError, KeyError):
                # nope, can't find it...
                raise KeyError(src_name)

    def poll(self, state, func, col):
        """
        Search for source in collection 'col' with a pending flag list
        containing 'state' and and call 'func' for each document found
        (with doc as only param)
        """
        if not self.poll_schedule:
            raise ManagerError("poll_schedule is not defined")

        async def check_pending(state):
            sources = [src for src in col.find({"pending": state}) if isinstance(src["_id"], str)]
            if sources:
                logger.info(
                    "Found %d resources with pending flag %s (%s)",
                    len(sources),
                    state,
                    repr([src["_id"] for src in sources]),
                )
            for src in sources:
                logger.info("Run %s for pending flag %s on source '%s'", func, state, src["_id"])
                try:
                    # first reset flag to make sure we won't call func multiple time
                    col.update({"_id": src["_id"]}, {"$pull": {"pending": state}})
                    func(src)
                except ResourceNotFound:
                    logger.error(
                        "Resource '%s' has a pending flag set to %s but is not registered in manager",
                        src["_id"],
                        state,
                    )

        return aiocron.crontab(
            self.poll_schedule,
            func=partial(check_pending, state),
            start=True,
            loop=self.job_manager.loop,
        )


class BaseStatusRegisterer:
    def load_doc(self, key_name, stage):
        """
        Find document using key_name and stage, stage being a
        key within the document matching a specific process name:
        Ex: {"_id":"123","snapshot":"abc"}
            load_doc("abc","snapshot")
        will return the document. Note key_name is first used to
        find the doc by its _id.
        Ex: with another doc {"_id" : "abc", "snapshot" : "somethingelse"}
            load_doc{"abc","snapshot")
        will return doc with _id="abc", not "123"
        """
        doc = self.collection.find_one({"_id": key_name})
        if not doc:
            doc = []
            bdocs = self.collection.find()
            for adoc in bdocs:
                if key_name in adoc.get(stage, {}):
                    doc.append(adoc)
            if len(doc) == 1:
                # we'll just return the single doc
                # otherwise it's up to the caller to do something with that
                doc = doc.pop()
        assert doc, "No document could be found"
        return doc

    @property
    def collection(self):
        """
        Return collection object used to fetch doc in which we store status
        """
        raise NotImplementedError("implement me in sub-class")

    def register_status(self, doc, stage, status, transient=False, init=False, **extra):
        assert self.collection, "No collection set"
        # stage: "snapshot", "publish", etc... depending on the what's being done
        job_info = {
            "status": status,
            "step_started_at": datetime.datetime.now().astimezone(),
            "logfile": self.logfile,
        }
        stage_info = {}
        stage_key = None
        # register status can be about different stages:
        stage_info.setdefault(stage, {}).update(extra[stage])
        stage_key = list(extra[stage].keys())
        assert len(stage_key) == 1, stage_key
        stage_key = stage_key.pop()
        if transient:
            # record some "in-progress" information
            job_info["pid"] = os.getpid()
        else:
            # only register time when it's a final state
            job_info["time"] = timesofar(self.ti)
            t1 = round(time.time() - self.ti, 0)
            job_info["time_in_s"] = t1
            stage_info.setdefault(stage, {}).setdefault(stage_key, {}).update(
                {"created_at": datetime.datetime.now().astimezone()}
            )
        if "job" in extra:
            job_info.update(extra["job"])
        # since the base is the merged collection, we register info there
        if init:
            # init timer for this step
            self.ti = time.time()
            self.collection.update({"_id": doc["_id"]}, {"$push": {"jobs": job_info}})
            # now refresh/sync
            doc = self.collection.find_one({"_id": doc["_id"]})
        else:
            # merge extra at root level
            doc["jobs"] and doc["jobs"].append(job_info)

            def merge_index_info(target, d):
                if not isinstance(target, dict):
                    # previous value wasn't a dict, just replace
                    target = d
                elif "__REPLACE__" in d.keys():
                    d.pop("__REPLACE__")
                    target = d
                else:
                    if status == "success":
                        # remove 'err' key to avoid merging success results with errors
                        target.pop("err", None)
                    for k, v in d.items():
                        if isinstance(v, dict):
                            if k in target:
                                target[k] = merge_index_info(target[k], v)
                            else:
                                v.pop("__REPLACE__", None)
                                # merge v with "nothing" just to make sure to remove any "__REPLACE__"
                                v = merge_index_info({}, v)
                                target[k] = v
                        else:
                            target[k] = v
                return target

            doc = merge_index_info(doc, stage_info)
            self.collection.replace_one({"_id": doc["_id"]}, doc)
