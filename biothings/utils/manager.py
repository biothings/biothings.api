import importlib
import threading
import re
import copy
import asyncio
import os
import types
import glob
import time
import datetime
from functools import wraps, partial
from pprint import pformat
from collections import OrderedDict
import concurrent.futures

import aiocron
import dill as pickle
import psutil

from biothings import config
logger = config.logger

from biothings.utils.mongo import get_src_conn
from biothings.utils.common import timesofar, get_random_string, sizeof_fmt


def track(func):
    # only wraps do_work defined later
    # seems to create a pickled dict for process/thread info (pinfo) and
    # some other metadata
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        job_id = args[0]
        ptype = args[1]  # tracking process or thread ?
        # we're looking for some "pinfo" value (process info) to later
        # reporting. If we can't find any, we'll try our best to figure out
        # what this is about...
        # func is the do_work wrapper, we want the actual partial
        # is first arg a callable (func) or pinfo ?
        if callable(args[2]):
            innerfunc = args[2]
            innerargs = args[3:]
            pinfo = None
        else:
            innerfunc = args[3]
            innerargs = args[4:]
            # we want to let the original as-is, as it can still contain
            # usefull information like predicates
            pinfo = copy.deepcopy(args[2])

        # predicates can't be pickles/dilled
        pinfo.pop("__predicates__", None)
        # just informative, so stringify is just ok there)
        # make sure we can pickle the whole thing (and it's
        innerargs = [str(arg) for arg in innerargs]
        if type(innerfunc) == partial:
            fname = innerfunc.func.__name__
        elif type(innerfunc) == types.MethodType:
            fname = innerfunc.__self__.__class__.__name__
        else:
            fname = innerfunc.__name__

        firstarg = innerargs and innerargs[0] or ""
        if not pinfo:
            pinfo = {"category": None,
                     "source": None,
                     "step": None,
                     "description": "%s %s" % (fname, firstarg)}

        pinfo["started_at"] = time.time()
        worker = {
            'func_name': fname,
            'args': innerargs,
            'kwargs': kwargs,
            'job': pinfo
        }
        results = None
        exc = None
        trace = None
        pidfile = None
        try:
            _id = None
            if ptype == "thread":
                _id = "%s" % threading.current_thread().getName()
            else:
                _id = os.getpid()
            # add random chars: 2 jobs handled by the same slot (pid or thread)
            # would override filename otherwise
            fn = "%s_%s" % (_id, job_id)
            # despite saying "job" "id" this is pid/thread name
            worker["job"]["id"] = _id
            pidfile = os.path.join(config.RUN_DIR, "%s.pickle" % fn)
            pickle.dump(worker, open(pidfile, "wb"))
            results = func(*args, **kwargs)
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            logger.error("err %s\n%s", e, trace)
            # we want to store exception so for now, just make a reference
            exc = e
        finally:
            if pidfile and os.path.exists(pidfile):
                logger.debug("Remove PID file '%s'", pidfile)
                os.unlink(pidfile)
        # now raise original exception
        if exc:
            raise exc
        return results
    return func_wrapper

@track
def do_work(job_id, ptype, pinfo=None, func=None, *args, **kwargs):
    # purpose: to be wrapped by @track
    # only used in defer_to_process / defer_to_thread in JobManager
    # pinfo is optional, and func is not. and args and kwargs must
    # be after func. just to say func is mandatory, despite what the
    # signature says
    assert func
    # need to wrap calls otherwise multiprocessing could have
    # issue pickling directly the passed func because of some import
    # issues ("can't pickle ... object is not the same as ...")
    return func(*args, **kwargs)


def find_process(pid):
    # It seems that it's only used once to find the hub process
    # I wonder why not just try to use psutil.Process(pid)
    g = psutil.process_iter()
    for p in g:
        if p.pid == pid:
            break
    # apparently a non-existent pid could trigger a NameError
    return p


class UnknownResource(Exception):
    pass
class ResourceError(Exception):
    pass
class ManagerError(Exception):
    pass
class ResourceNotFound(Exception):
    pass

class BaseManager(object):

    def __init__(self, job_manager, poll_schedule=None):
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
        return "<%s [%d registered]: %s>" % (self.__class__.__name__, len(self.register), registered)

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
        '''
        Search for source in collection 'col' with a pending flag list
        containing 'state' and and call 'func' for each document found
        (with doc as only param)
        '''
        if not self.poll_schedule:
            raise ManagerError("poll_schedule is not defined")

        @asyncio.coroutine
        def check_pending(state):
            sources = [src for src in col.find({'pending': state}) if isinstance(src['_id'], str)]
            if sources:
                logger.info(
                    "Found %d resources with pending flag %s (%s)",
                    len(sources), state, repr([src["_id"] for src in sources])
                )
            for src in sources:
                logger.info("Run %s for pending flag %s on source '%s'", func, state, src["_id"])
                try:
                    # first reset flag to make sure we won't call func multiple time
                    col.update({"_id": src["_id"]}, {"$pull": {"pending": state}})
                    func(src)
                except ResourceNotFound:
                    logger.error("Resource '%s' has a pending flag set to %s but is not registered in manager",
                                 src["_id"], state)

        return aiocron.crontab(
            self.poll_schedule, func=partial(check_pending, state),
            start=True, loop=self.job_manager.loop
        )


class BaseStatusRegisterer(object):

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
        doc = self.collection.find_one({'_id': key_name})
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
            'status': status,
            'step_started_at': datetime.datetime.now().astimezone(),
            'logfile': self.logfile
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
            job_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            job_info["time"] = timesofar(self.ti)
            t1 = round(time.time() - self.ti, 0)
            job_info["time_in_s"] = t1
            stage_info.setdefault(stage, {}).setdefault(stage_key, {}).update({"created_at": datetime.datetime.now().astimezone()})
        if "job" in extra:
            job_info.update(extra["job"])
        # since the base is the merged collection, we register info there
        if init:
            # init timer for this step
            self.ti = time.time()
            self.collection.update({'_id': doc["_id"]}, {"$push": {'jobs': job_info}})
            # now refresh/sync
            doc = self.collection.find_one({'_id': doc["_id"]})
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


class BaseSourceManager(BaseManager):
    """
    Base class to provide source management: discovery, registration
    Actual launch of tasks must be defined in subclasses
    """

    # define the class manager will look for. Set in a subclass
    SOURCE_CLASS = None

    def __init__(self, job_manager, datasource_path="dataload.sources", *args, **kwargs):
        super(BaseSourceManager, self).__init__(job_manager, *args, **kwargs)
        self.conn = get_src_conn()
        self.default_src_path = datasource_path
        self.poll_schedule = None

    def filter_class(self, klass):
        """
        Gives opportunity for subclass to check given class and decide to
        keep it or not in the discovery process. Returning None means "skip it".
        """
        # keep it by default
        return klass

    def register_classes(self, klasses):
        """
        Register each class in self.register dict. Key will be used
        to retrieve the source class, create an instance and run method from it.
        It must be implemented in subclass as each manager may need to access
        its sources differently,based on different keys.
        """
        raise NotImplementedError("implement me in sub-class")

    def find_classes(self, src_module, fail_on_notfound=True):
        """
        Given a python module, return a list of classes in this module, matching
        SOURCE_CLASS (must inherit from)
        """
        # try to find a uploader class in the module
        klasses = []
        for attr in dir(src_module):
            something = getattr(src_module, attr)
            # not interested in classes coming from biothings.hub.*, these would typically come
            # from "from biothings.hub.... import aclass" statements and would be incorrectly registered
            # we only look for classes defined straight from the actual module
            if type(something) == type and issubclass(something, self.__class__.SOURCE_CLASS) and \
                    not something.__module__.startswith("biothings.hub"):
                klass = something
                if not self.filter_class(klass):
                    continue
                logger.debug("Found a class based on %s: '%s'", self.__class__.SOURCE_CLASS.__name__, klass)
                klasses.append(klass)
        if not klasses:
            if fail_on_notfound:
                raise UnknownResource("Can't find a class based on %s in module '%s'" % (self.__class__.SOURCE_CLASS.__name__, src_module))
        return klasses

    def register_source(self, src, fail_on_notfound=True):
        """Register a new data source. src can be a module where some classes
        are defined. It can also be a module path as a string, or just a source name
        in which case it will try to find information from default path.
        """
        if isinstance(src, str):
            try:
                src_m = importlib.import_module(src)
                src_m = importlib.reload(src_m)
            except ImportError:
                try:
                    src_m = importlib.import_module("%s.%s" % (self.default_src_path, src))
                except ImportError:
                    msg = "Can't find module '%s', even in '%s'" % (src, self.default_src_path)
                    logger.error(msg)
                    raise UnknownResource(msg)

        elif isinstance(src, dict):
            # source is comprised of several other sub sources
            assert len(src) == 1, "Should have only one element in source dict '%s'" % src
            _, sub_srcs = list(src.items())[0]
            for src in sub_srcs:
                self.register_source(src, fail_on_notfound)
            return
        else:
            src_m = src
        # first try to find classes defined in __init__.py (in package) explicitely
        klasses = self.find_classes(src_m, fail_on_notfound)
        # then if none found, try to search within the package's modules
        if not klasses:
            try:
                src_m_path = src_m.__path__[0]
                for d in os.listdir(src_m_path):
                    if d.endswith("__pycache__"):
                        continue
                    modpath = os.path.join(src_m.__name__, d).replace(".py", "").replace(os.path.sep, ".")
                    try:
                        m = importlib.import_module(modpath)
                        klasses.extend(self.find_classes(m, fail_on_notfound))
                    except (SyntaxError, ImportError) as e:
                        logger.debug("Couldn't import %s: %s", modpath, e)
                        continue
            except TypeError as e:
                logger.warning("Can't register source '%s', something's wrong with path: %s", src_m, e)
        logger.debug("Found classes to register: %s", repr(klasses))

        self.register_classes(klasses)

    def register_sources(self, sources):
        assert not isinstance(sources, str), "sources argument is a string, should pass a list"
        self.register.clear()
        for src in sources:
            try:
                # batch registration, we'll silently ignore not-found sources
                self.register_source(src, fail_on_notfound=False)
            except (UnknownResource, ResourceError) as e:
                logger.info("Can't register source '%s', skip it; %s", src, e)
                import traceback
                logger.error(traceback.format_exc())


class JobManager(object):
    # TODO: Add class docstring
    COLUMNS = ["pid", "source", "category", "step", "description", "mem", "cpu", "started_at", "duration"]
    HEADER = dict(zip(COLUMNS, [c.upper() for c in COLUMNS]))  # upper() for column titles
    HEADERLINE = "{pid:^10}|{source:^35}|{category:^10}|{step:^20}|{description:^30}|{mem:^10}|{cpu:^6}|{started_at:^20}|{duration:^10}"
    DATALINE = HEADERLINE.replace("^", "<")

    def __init__(self, loop, process_queue=None, thread_queue=None, max_memory_usage=None,
                 num_workers=None, num_threads=None, default_executor="thread", auto_recycle=True):
        if not os.path.exists(config.RUN_DIR):
            logger.info("Creating RUN_DIR directory '%s'", config.RUN_DIR)
            os.makedirs(config.RUN_DIR)
        self.loop = loop  # usu. it's the asyncio event loop
        self.num_workers = num_workers
        if self.num_workers == 0:
            logger.debug("Adjusting number of worker to 1")
            self.num_workers = 1
        self.num_threads = num_threads or self.num_workers
        self.process_queue = process_queue or concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers)
        # notes on fixing BPE (BrokenProcessPool Exception):
        # whenever a process exits unexpectedly, BPE is raised, and while that
        # all the processes in the pool gets a SIGTERM from the management
        # thread (see _queue_management_worker in concurrent.futures.process)
        # TODO: limit the number of threads (as argument) ?
        self.thread_queue = thread_queue or concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads)

        # FIXME: In Py38 using an executor that is not a ThreadPoolExecutor is
        #  deprecated. And it seems in Py39 , it must be a ThreadPoolExecutor,
        #  using a ProcessPoolExecutor will trigger an error.
        #  However, loop.run_in_executor still accepts ProcessPoolExecutor
        #  see https://bugs.python.org/issue34075
        if default_executor == "thread":
            self.loop.set_default_executor(self.thread_queue)
        else:
            self.loop.set_default_executor(self.process_queue)
        # this lock is acquired when defer_to_process/thread is invoked
        # and released when the inner coroutine is run
        #  purpose being: "control job submission", as it only creates a new
        #  "task" when the previous one has completed checking its constraints
        self.ok_to_run = asyncio.Semaphore()
        # auto-creata RUN_DIR
        if not os.path.exists(config.RUN_DIR):
            os.makedirs(config.RUN_DIR)

        if max_memory_usage == "auto":
            # try to find a nice limit...
            limited = int(psutil.virtual_memory().available * .6)
            logger.info("Auto-setting memory usage limit to %s", sizeof_fmt(limited))
            max_memory_usage = limited
        elif max_memory_usage:
            logger.info("Setting memory usage to %s", sizeof_fmt(max_memory_usage))
        else:
            logger.info("No memory limit set")
        self.max_memory_usage = max_memory_usage
        self.avail_memory = int(psutil.virtual_memory().available)
        self._phub = None
        # Process obj. for hub (process which JobManager is in)
        self.auto_recycle = auto_recycle  # active
        self.auto_recycle_setting = auto_recycle  # keep setting if we need to restore it its orig value
        self.jobs = {}  # all active jobs (thread/process)
        self._pchildren = []
        self.clean_staled()

    def stop(self, force=False, recycling=False, wait=1):
        @asyncio.coroutine
        def do():
            try:
                # shutting down the process queue can take a while
                # if some processes are still running (it'll wait until they're done)
                # we'll wait in a thread to prevent the hub from being blocked
                logger.info("Shutting down current process queue...")
                pinfo = {"__skip_check__": True,  # skip sanity check, mem check to make sure
                                                  # this worker will be run
                         "category": "admin",
                         "source": "maintenance",
                         "step": "",
                         "description": "Stopping process queue"}
                j = yield from self.defer_to_thread(pinfo, self.process_queue.shutdown)
                yield from j
                if recycling:
                    # now replace
                    logger.info("Replacing process queue with new one")
                    self.process_queue = concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers)
                else:
                    self.process_queue = None
            except Exception as e:
                logger.error("Error while recycling the process queue: %s", e)
                raise

        @asyncio.coroutine
        def kill():
            nonlocal wait
            if wait < 1:
                wait = 1  # wait a little bit so job manager has time to stop if nothing is running
            logger.warning("Wait %s seconds before killing queue processes", wait)
            yield from asyncio.sleep(wait)
            logger.warning("Can't wait anymore, killing running processed in the queue !")
            for proc in self.pchildren:
                logger.warning("Killing %s", proc)
                proc.kill()

        def done(f):
            f.result()  # consume future's result to potentially raise exception
        fut = asyncio.ensure_future(do())
        fut.add_done_callback(done)
        if force:
            futkill = asyncio.ensure_future(kill())
        return fut

    def clean_staled(self):
        # clean old/staled files
        children_pids = [p.pid for p in self.pchildren]
        active_tids = [t.getName() for t in self.thread_queue._threads]
        pid_pat = re.compile(".*/(\d+)_.*\.pickle")  # see track() for filename format
        for fn in glob.glob(os.path.join(config.RUN_DIR, "*.pickle")):
            pid = pid_pat.findall(fn)
            if not pid:
                continue
            try:
                pid = int(pid[0].split("_")[0])
            except IndexError:
                logger.warning("Invalid PID file '%s', skip it", fn)
                raise
            if pid not in children_pids:
                logger.info("Removing staled pid file '%s'", fn)
                os.unlink(fn)
        tid_pat = re.compile(".*/(Thread\w*-\d+)_.*\.pickle")
        for fn in glob.glob(os.path.join(config.RUN_DIR, "*.pickle")):
            try:
                tid = tid_pat.findall(fn)[0].split("_")[0]
            except IndexError:
                logger.warning("Invalid TID file '%s', skip it", fn)
                raise
            if not tid:
                continue
            if tid not in active_tids:
                logger.info("Removing staled thread file '%s'", fn)
                os.unlink(fn)

    def recycle_process_queue(self):
        """
        Replace current process queue with a new one. When processes
        are used over and over again, memory tends to grow as python
        interpreter keeps some data (...). Calling this method will
        perform a clean shutdown on current queue, waiting for running
        processes to terminate, then discard current queue and replace
        it a new one.
        """
        return self.stop(recycling=True)

    @asyncio.coroutine
    def check_constraints(self, pinfo=None):
        mem_req = pinfo and pinfo.get("__reqs__", {}).get("mem") or 0
        t0 = time.time()
        waited = False
        sleep_time = 5
        if mem_req:
            logger.info(
                "Job {cat:%s,source:%s,step:%s} requires %s memory, checking if available",
                pinfo.get("category"), pinfo.get("source"), pinfo.get("step"), sizeof_fmt(mem_req))
        if self.max_memory_usage:
            hub_mem = self.hub_memory
            while hub_mem >= self.max_memory_usage:
                if self.auto_recycle:
                    pworkers = self.get_pid_files()
                    tworkers = self.get_thread_files()
                    if len(pworkers) == 0 and len(tworkers) == 0:
                        logger.info("No worker running, recycling the process queue...")
                        fut = self.recycle_process_queue()

                        def recycled(f):
                            res = f.result()
                            # still out of memory ?
                            avail_mem = self.max_memory_usage - self.hub_memory
                            if avail_mem <= 0:
                                logger.error(
                                    "After recycling process queue, "
                                    "memory usage is still too high (needs at least %s more)"
                                    "now turn auto-recycling off to prevent infinite recycling...",
                                    sizeof_fmt(abs(avail_mem))
                                )
                                self.auto_recycle = False

                        fut.add_done_callback(recycled)
                logger.info(
                    "Hub is using too much memory to launch job {cat:%s,source:%s,step:%s}"\
                    " (%s used, more than max allowed %s), wait a little (job's already been postponed for %s)",
                    pinfo.get("category"), pinfo.get("source"), pinfo.get("step"),
                    sizeof_fmt(hub_mem), sizeof_fmt(self.max_memory_usage), timesofar(t0)
                )
                yield from asyncio.sleep(sleep_time)
                waited = True
                hub_mem = self.hub_memory
        if mem_req:
            # max allowed mem is either the limit we gave and the os limit
            max_mem = self.max_memory_usage and self.max_memory_usage or self.avail_memory
            # TODO: check projected memory (jobs with mem requirements currently running
            # as those jobs may not have reached their max mem usage yet)
            hub_mem = self.hub_memory
            while mem_req >= (max_mem - hub_mem):
                logger.info(
                    "Job {cat:%s,source:%s,step:%s} needs %s to run, not enough to launch it "
                    "(hub consumes %s while max allowed is %s), wait a little  (job's already been postponed for %s)",
                    pinfo.get("category"), pinfo.get("source"), pinfo.get("step"), sizeof_fmt(mem_req),
                    sizeof_fmt(hub_mem), sizeof_fmt(max_mem), timesofar(t0)
                )
                yield from asyncio.sleep(sleep_time)
                waited = True
                # refresh limites and usage (manager can be modified from hub
                # thus memory usage can be modified on-the-fly
                hub_mem = self.hub_memory
                max_mem = self.max_memory_usage and self.max_memory_usage or self.avail_memory
        pendings = len(self.process_queue._pending_work_items.keys()) - config.HUB_MAX_WORKERS
        while pendings >= config.MAX_QUEUED_JOBS:
            if not waited:
                logger.info(
                    "Can't run job {cat:%s,source:%s,step:%s} right now, too much pending jobs in the queue (max: %s), will retry until possible",
                    pinfo.get("category"), pinfo.get("source"), pinfo.get("step"), config.MAX_QUEUED_JOBS
                )
            yield from asyncio.sleep(sleep_time)
            pendings = len(self.process_queue._pending_work_items.keys()) - config.HUB_MAX_WORKERS
            waited = True
        # finally check custom predicates
        predicates = pinfo and pinfo.get("__predicates__", [])
        failed_predicate = None
        while True:
            for predicate in predicates:
                if not predicate(self):
                    failed_predicate = predicate
                    break  # for loop (most inner one)
                else:
                    # reset flag
                    failed_predicate = None
            if failed_predicate:
                logger.info("Can't run job {cat:%s,source:%s,step:%s} right now, predicate %s failed, will retry until possible",
                            pinfo.get("category"), pinfo.get("source"), pinfo.get("step"), failed_predicate)
                yield from asyncio.sleep(sleep_time)
                waited = True
            else:
                break  # while loop
        if waited:
            logger.info("Job {cat:%s,source:%s,step:%s} now can be launched (total waiting time: %s)",
                        pinfo.get("category"), pinfo.get("source"), pinfo.get("step"), timesofar(t0))
            # auto-recycle could have been temporarily disabled until more mem is assigned.
            # if we've been able to run the job, it means we had enough mem so restore
            # recycling setting (if auto_recycle was False, it's ignored
            if self.auto_recycle_setting:
                self.auto_recycle = self.auto_recycle_setting

    @asyncio.coroutine
    def defer_to_process(self, pinfo=None, func=None, *args):

        @asyncio.coroutine
        def run(future, job_id):
            nonlocal pinfo
            yield from self.check_constraints(pinfo)
            self.ok_to_run.release()
            # pinfo can contain predicates hardly pickleable during run_in_executor
            # but we also need not to touch the original one
            copy_pinfo = copy.deepcopy(pinfo)
            copy_pinfo.pop("__predicates__", None)
            self.jobs[job_id] = copy_pinfo

            try:
                # test to see if Executor still alive
                _ = self.process_queue.submit(int, 1)
            except concurrent.futures.process.BrokenProcessPool as e:
                # recreate if not
                logger.warning("Broken Process Pool: %s, restarting.", e)
                self.process_queue = concurrent.futures.ProcessPoolExecutor(
                    max_workers=self.num_workers
                )
            res = self.loop.run_in_executor(
                self.process_queue,
                partial(do_work, job_id, "process", copy_pinfo, func, *args)
            )
            # do_work will create and clean up the pickle files unless
            # the worker process gets killed unexpectedly

            def ran(f):
                try:
                    # consume future, just to trigger potential exceptions
                    r = f.result()
                finally:
                    # whatever the result we want to make sure to clean the job registry
                    # to keep it sync with actual running jobs
                    # -- actually it can't the job_id is added in
                    # defer_to_process, but this is inside the try-finally
                    # block indefer_to_process.run.ran (names are hard, I know)
                    self.jobs.pop(job_id)
            res.add_done_callback(ran)
            res = yield from res
            # process could generate other parallelized jobs and return a Future/Task
            # If so, we want to make sure we get the results from that task
            if type(res) == asyncio.Task:
                res = yield from res
            future.set_result(res)
        yield from self.ok_to_run.acquire()
        f = asyncio.Future()

        def runned(innerf, job_id):
            if innerf.exception():
                f.set_exception(innerf.exception())
        job_id = get_random_string()
        fut = asyncio.ensure_future(run(f, job_id))
        fut.add_done_callback(partial(runned, job_id=job_id))
        return f

    @asyncio.coroutine
    def defer_to_thread(self, pinfo=None, func=None, *args):

        skip_check = pinfo.get("__skip_check__", False)

        @asyncio.coroutine
        def run(future, job_id):
            if not skip_check:
                yield from self.check_constraints(pinfo)
                self.ok_to_run.release()
            self.jobs[job_id] = pinfo
            res = self.loop.run_in_executor(
                self.thread_queue,
                partial(do_work, job_id, "thread", pinfo, func, *args)
            )

            def ran(f):
                try:
                    r = f.result()
                finally:
                    # whatever the result we want to make sure to clean the job registry
                    # to keep it sync with actual running jobs
                    self.jobs.pop(job_id)
            res.add_done_callback(ran)
            res = yield from res
            # thread could generate other parallelized jobs and return a Future/Task
            # If so, we want to make sure we get the results from that task
            if type(res) == asyncio.Task:
                res = yield from res
            future.set_result(res)
        if not skip_check:
            yield from self.ok_to_run.acquire()
        f = asyncio.Future()

        def runned(innerf, job_id):
            if innerf.exception():
                f.set_exception(innerf.exception())
        job_id = get_random_string()
        fut = asyncio.ensure_future(run(f, job_id))
        fut.add_done_callback(partial(runned, job_id=job_id))
        return f

    def submit(self, pfunc, schedule=None):
        """
        Helper to submit and run tasks. Tasks will run async'ly.
        pfunc is a functools.partial
        schedule is a string representing a cron schedule, task will then be scheduled
        accordingly.
        """
        logger.info("Building task: %s", pfunc)
        if schedule:
            logger.info("Scheduling task %s: %s", pfunc, schedule)
            cron = aiocron.crontab(schedule, func=pfunc, start=True, loop=self.loop)
            return cron
        else:
            ff = asyncio.ensure_future(pfunc())
            return ff

    def schedule(self, crontab, func, *args, **kwargs):
        """
        Helper to create a cron job from a callable "func". *argd, and **kwargs
        are passed to func. "crontab" follows aicron notation.
        """
        # we need to dynamically create a wrapper coroutine with a name
        # that makes sense, taken from func, otherwise all scheduled jobs would
        # have the same wrapping coroutine name
        if isinstance(func,partial):
            func_name = func.func.__name__
        else:
            func_name = func.__name__
        strcode = """
@asyncio.coroutine
def %s():
    func(*args, **kwargs)
""" % func_name
        code = compile(strcode, "<string>", "exec")
        command_globals = {}
        command_locals = {
            "asyncio": asyncio,
            "func": func,
            "args": args,
            "kwargs": kwargs
        }
        eval(code, command_locals, command_globals)
        run_func = command_globals[func_name]
        job = self.submit(run_func,schedule=crontab)

        return job

    @property
    def hub_process(self):
        if not self._phub:
            self._phub = find_process(os.getpid())
        return self._phub

    @property
    def pchildren(self):
        if not self._pchildren:
            self._pchildren = self.hub_process.children()
        return self._pchildren

    @property
    def hub_memory(self):
        total_mem = 0
        try:
            procs = [self.hub_process] + self.pchildren
            for proc in procs:
                total_mem += proc.memory_info().rss
        except psutil.NoSuchProcess:
            # observed multiple time: hub main pid doesn't exist, like it was replace, not sure why,... OS ?
            self._phub = None
            self._pchildren = None

        return total_mem

    def get_pid_files(self, child=None):
        pids = {}
        try:
            pat = re.compile(".*/(\d+)_.*\.pickle")  # see track() for filename format
            children_pids = [p.pid for p in self.pchildren]
            for fn in glob.glob(os.path.join(config.RUN_DIR, "*.pickle")):
                try:
                    pid = int(pat.findall(fn)[0].split("_")[0])
                    if not child or child.pid == pid:
                        try:
                            worker = pickle.load(open(fn, "rb"))
                        except FileNotFoundError:
                            # it's possible that, as this point, the pickle file
                            # doesn't exist anymore (process is done and file was unlinked)
                            # just ignore go to next one
                            continue
                        proc = self.pchildren[children_pids.index(pid)]

                        worker["process"] = {
                            "mem": proc.memory_info().rss,
                            "cpu": proc.cpu_percent()
                        }
                        pids[pid] = worker
                except IndexError:
                    # weird though... should have only pid files there...
                    pass
        finally:
            return pids

    def get_thread_files(self):
        tids = {}
        try:
            # see track() for filename format
            pat = re.compile(".*/(Thread\w*-\d+)_.*\.pickle")
            threads = self.thread_queue._threads
            active_tids = [t.getName() for t in threads]
            for fn in glob.glob(os.path.join(config.RUN_DIR, "*.pickle")):
                try:
                    tid = pat.findall(fn)[0].split("_")[0]
                    worker = pickle.load(open(fn, "rb"))
                    worker["process"] = self.hub_process  # misleading... it's the hub process
                    tids[tid] = worker
                except IndexError:
                    # weird though... should have only pid files there...
                    pass
        finally:
            return tids

    def extract_pending_info(self, pending):
        info = pending.fn.args[2]
        assert isinstance(info, dict)
        return info

    def extract_worker_info(self, worker):
        info = OrderedDict()
        proc = worker.get("process", worker)
        err = worker.get("err") and " !" or ""
        info["pid"] = str(worker["job"]["id"]) + err
        info["source"] = norm(worker["job"].get("source") or "", 25)
        info["category"] = norm(worker["job"].get("category") or "", 10)
        info["step"] = norm(worker["job"].get("step") or "", 20)
        info["description"] = norm(worker["job"].get("description") or "", 30)
        info["mem"] = sizeof_fmt(proc.get("memory", {}).get("size", 0.0))
        info["cpu"] = "%.1f%%" % proc.get("cpu", {}).get("percent", 0.0)
        info["started_at"] = worker["job"]["started_at"]
        if worker.get("duration"):
            info["duration"] = worker["duration"]
        else:
            info["duration"] = timesofar(worker["job"]["started_at"])
        # for now, don't display files used by the process
        info["files"] = []
        #if proc:
        #    for pfile in proc.open_files():
        #        # skip 'a' (logger)
        #        if pfile.mode == 'r':
        #            finfo = OrderedDict()
        #            finfo["path"] = pfile.path
        #            finfo["read"] = sizeof_fmt(pfile.position)
        #            size = os.path.getsize(pfile.path)
        #            finfo["size"] = sizeof_fmt(size)
        #            #info["files"].append(finfo)
        return info

    def print_workers(self, workers):
        if workers:
            out = []
            out.append(self.__class__.HEADERLINE.format(**self.__class__.HEADER))
            for pid in workers:
                worker = workers[pid]
                info = self.extract_worker_info(worker)
                tt = datetime.datetime.fromtimestamp(info["started_at"]).timetuple()
                info["started_at"] = time.strftime("%Y/%m/%d %H:%M:%S", tt)
                try:
                    out.append(self.__class__.DATALINE.format(**info))
                except (TypeError, KeyError) as e:
                    out.append(e)
                    out.append(pformat(info))

            return "\n".join(out)
        else:
            return ""

    def print_pending_info(self, num, info):
        assert isinstance(info, dict)
        info["cpu"] = ""
        info["mem"] = ""
        info["pid"] = ""
        info["duration"] = ""
        info["source"] = norm(info["source"], 35)
        info["category"] = norm(info["category"], 10)
        info["step"] = norm(info["step"], 20)
        info["description"] = norm(info["description"], 30)
        info["started_at"] = ""
        out = []
        try:
            out.append(self.__class__.DATALINE.format(**info))
        except (TypeError, KeyError) as e:
            out.append(e)
            out.append(pformat(info))

        return out

    def get_process_summary(self):
        running_pids = self.get_pid_files()
        res = {}
        for child in self.pchildren:
            try:
                mem = child.memory_info().rss
                pio = child.io_counters()
                # TODO: cpu as reported here isn't reliable, the only to get something
                # consistent to call cpu_percent() with a waiting time argument to integrate
                # CPU activity over this time, but this is a blocking call and freeze the hub
                # (an async implementation might possible though). Currently, pchildren is list
                # set at init time where process object are stored, so subsequent cpu_percent()
                # calls should report CPU activity since last call (between /job_manager & top()
                # calls), but it constently return CPU > 100% even when no thread running (that
                # could have been the explination but it's not).
                cpu = child.cpu_percent()
                res[child.pid] = {
                    "memory": {
                        "size": child.memory_info().rss,
                        "percent": child.memory_percent(),
                    },
                    "cpu": {
                        # override status() when we have cpu activity to avoid
                        # having a "sleeping" process that's actually running something
                        # (prob happening because delay between status and cpu_percent(), like a race condition)
                        "status": cpu > 0.0 and "running" or child.status(),
                        "percent": cpu
                    },
                    "io": {
                        "read_count": pio.read_count,
                        "write_count": pio.write_count,
                        "read_bytes": pio.read_bytes,
                        "write_bytes": pio.write_bytes
                    }
                }

                if child.pid in running_pids:
                    # something is running on that child process
                    worker = running_pids[child.pid]
                    res[child.pid]["job"] = {
                        "started_at": worker["job"]["started_at"],
                        "duration": timesofar(worker["job"]["started_at"], 0),
                        "func_name": worker["func_name"],
                        "category": worker["job"]["category"],
                        "description": worker["job"]["description"],
                        "source": worker["job"]["source"],
                        "step": worker["job"]["step"],
                        "id": worker["job"]["id"],
                    }
            except psutil.NoSuchProcess as e:
                print("child not found %s %s" % (child, e))
                continue

        return res

    def get_thread_summary(self):
        running_tids = self.get_thread_files()
        tchildren = self.thread_queue._threads
        res = {}
        for child in tchildren:
            res[child.name] = {
                "is_alive": child.isAlive(),
                "is_daemon": child.isDaemon(),
            }

            if child.name in running_tids:
                # something is running on that child process
                worker = running_tids[child.name]
                res[child.name]["job"] = {
                    "started_at": worker["job"]["started_at"],
                    "duration": timesofar(worker["job"]["started_at"], 0),
                    "func_name": worker["func_name"],
                    "category": worker["job"]["category"],
                    "description": worker["job"]["description"],
                    "source": worker["job"]["source"],
                    "step": worker["job"]["step"],
                    "id": worker["job"]["id"],
                }

        return res

    def get_summary(self, child=None):
        pworkers = self.get_pid_files(child)
        tworkers = self.get_thread_files()
        ppendings = self.get_pending_processes()
        tpendings = {}  # TODO:
        return {
            "process": {
                "running": list(pworkers.keys()),
                "pending": list(ppendings.keys()),
                "all": self.get_process_summary(),
                "max": self.process_queue._max_workers
            },
            "thread": {
                "running": list(tworkers.keys()),
                "pending": list(tpendings.keys()),
                "all": self.get_thread_summary(),
                "max": self.thread_queue._max_workers,
            },
            "memory": self.hub_memory,
            "available_system_memory": self.avail_memory,
            "max_memory_usage": self.max_memory_usage,
            "hub_pid": self.hub_process.pid
        }

    def get_pending_summary(self, getstr=False):
        running = len(self.get_pid_files())
        return "%d pending job(s)" % (len(self.process_queue._pending_work_items) - running)

    def get_pending_processes(self):
        # pendings are kept in queue while running, until result is there so we need
        # to adjust the actual real pending jobs. also, pending job are get() from the
        # queue following FIFO order. finally, worker ID is incremental. So...
        pendings = sorted(self.process_queue._pending_work_items.items())
        running = len(self.get_pid_files())
        actual_pendings = dict(pendings[running:])
        return actual_pendings

    def show_pendings(self, running=None):
        out = []
        out.append(self.get_pending_summary())
        actual_pendings = self.get_pending_processes()
        if actual_pendings:
            out.append(self.__class__.HEADERLINE.format(**self.__class__.HEADER))
            for num, pending in actual_pendings.items():
                info = self.extract_pending_info(pending)
                try:
                    self.print_pending_info(num, info)
                except Exception as e:
                    out.append(e)
                    out.append(pformat(pending))

        return "\n".join(out)

    def top(self, action="summary"):
        pending = False
        done = False
        run = False
        pid = None
        child = None
        #if action:
        #    try:
        #        # want to see details for a specific process ?
        #        pid = int(action)
        #        child = [p for p in pchildren if p.pid == pid][0]
        #    except ValueError:
        #        pass
        pworkers = self.get_pid_files(child)
        tworkers = self.get_thread_files()
        done_jobs = glob.glob(os.path.join(config.RUN_DIR, "done", "*.pickle"))
        out = []
        if child:
            return pworkers[child.pid]
        elif action == "pending":
            return self.show_pendings(running=len(pworkers))
        elif action == "summary":
            res = self.get_summary()
            # TODO: delete the follow two lines
            # pworkers = dict([(pid, proc) for pid, proc in res["process"]["all"].items() if pid in res["process"]["running"]])
            # tworkers = dict([(tid, thread) for tid, thread in res["thread"]["all"].items() if tid in res["thread"]["running"]])
            pworkers = {pid: proc for pid, proc in res["process"]["all"].items() if pid in res["process"]["running"]}
            tworkers = {tid: thread for tid, thread in res["thread"]["all"].items() if tid in res["thread"]["running"]}

            out.append(self.print_workers(pworkers))
            out.append(self.print_workers(tworkers))
            out.append("%d running job(s)" % (len(pworkers) + len(tworkers)))
            out.append("%s, type 'top(pending)' for more" % self.get_pending_summary())
            if done_jobs:
                out.append("%s finished job(s), type 'top(done)' for more" % len(done_jobs))
        else:
            raise ValueError("Unknown action '%s'" % action)

        return "\n".join(out)

    def job_info(self):
        summary = self.get_summary()
        prunning = summary["process"]["running"]
        trunning = summary["thread"]["running"]
        ppending = summary["process"]["pending"]
        tpending = summary["thread"]["pending"]
        return {
            "queue": {
                "process": summary["process"],
                "thread": summary["thread"],
            },
            "memory": summary["memory"],
            "available_system_memory": summary["available_system_memory"],
            "max_memory_usage": summary["max_memory_usage"],
            "hub_pid": summary["hub_pid"]
        }

# just a helper to clean/prepare job's values printing
def norm(value, maxlen):
    if len(value) > maxlen:
        value = "...%s" % value[-maxlen+3:]
    return value
