import asyncio
import concurrent.futures
import copy
import datetime
import glob
import io
import logging
import multiprocessing
import os
import re
import sys
import threading
import time
import types
import warnings
from collections import OrderedDict
from functools import partial, wraps
from pprint import pformat

try:
    import aiocron
except ImportError:
    # Suppress import error when we just run CLI
    pass

try:
    import dill as pickle
except ImportError:
    # Suppress import error when we just run CLI
    pass

import psutil

import biothings.hub  # noqa
from biothings import config
from biothings.utils.common import get_random_string, sizeof_fmt, timesofar

logger = config.logger

# default of JobManager's deprecated process_queue argument, to tell whether it
# was passed at all
_UNSET = object()


def _warn_deprecated(old, new):
    # stacklevel=3 points the warning at the code using the deprecated name
    warnings.warn(
        "%s is deprecated and will be removed in a future major release, use %s instead" % (old, new),
        DeprecationWarning,
        stacklevel=3,
    )


def track(func):
    # only wraps do_work defined later
    # seems to create a pickled dict for process/thread info (pinfo) and
    # some other metadata
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        job_id = args[0]
        ptype = args[1]  # tracking a "worker" or a "thread" job
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
        if isinstance(innerfunc, partial):
            fname = innerfunc.func.__name__
        elif isinstance(innerfunc, types.MethodType):
            fname = innerfunc.__self__.__class__.__name__
        else:
            fname = innerfunc.__name__

        firstarg = innerargs and innerargs[0] or ""
        if not pinfo:
            pinfo = {
                "category": None,
                "source": None,
                "step": None,
                "description": "%s %s" % (fname, firstarg),
            }

        pinfo["started_at"] = time.time()
        worker = {"func_name": fname, "args": innerargs, "kwargs": kwargs, "job": pinfo}
        results = None
        exc = None
        trace = None
        pidfile = None
        try:
            _id = None
            if ptype == "thread":
                _id = "%s" % threading.current_thread().name
            elif multiprocessing.current_process().name == "MainProcess":
                # free-threaded workers mode: worker jobs run in threads of
                # the hub process, the pid alone would collide
                _id = "%s-%s" % (os.getpid(), threading.current_thread().name)
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
    # only used in defer_to_worker / defer_to_thread in JobManager
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


def norm(value, maxlen):
    """just a helper to clean/prepare job's values printing"""
    if len(value) > maxlen:
        value = "...%s" % value[-maxlen + 3 :]
    return value


# streams a forked worker inherited from the hub and replaced: kept referenced so
# they're never flushed, closed or garbage-collected in the worker, as a hub thread
# may have held their internal lock at fork time, and that thread doesn't exist in
# the worker to release it
_inherited_streams = []


def _fresh_stream(stream):
    """Return a new text stream, with its own lock, on stream's file descriptor,
    or None if stream isn't a text stream backed by one."""
    if not isinstance(stream, io.TextIOWrapper):
        return None
    try:
        fd = stream.fileno()  # doesn't take the stream's lock
    except (OSError, ValueError):  # no file descriptor, or closed
        return None
    fresh = open(fd, "w", encoding=stream.encoding, errors=stream.errors, closefd=False)
    fresh.reconfigure(line_buffering=stream.line_buffering, write_through=stream.write_through)
    return fresh


def _reset_inherited_streams():
    """
    Process pool initializer for forked workers: replace the stdout/stderr and
    log file streams inherited from the hub with fresh ones, so a lock held by a
    hub thread at fork time can't block the worker on its first log write.

    It runs before the worker takes any job, and must never raise (that would
    break the whole pool) nor write to an inherited stream (that could block).
    Anything it doesn't recognize is left as is.
    """
    swapped = {}  # id(inherited stream) -> fresh stream
    failed = []
    stderr = sys.__stderr__

    def swap(inherited, what):
        if id(inherited) in swapped:
            return swapped[id(inherited)]
        try:
            fresh = _fresh_stream(inherited)
        except Exception as e:
            failed.append("%s (%s)" % (what, type(e).__name__))
            return None
        if fresh is not None:
            # referenced before any reference to it is dropped, see above
            _inherited_streams.append(inherited)
            swapped[id(inherited)] = fresh
        return fresh

    # original streams first: sys.stdout/stderr usually are the same objects,
    # but may be redirected (e.g. to a logger) while the worker is forked
    for name in ("__stdout__", "__stderr__", "stdout", "stderr"):
        try:
            fresh = swap(getattr(sys, name), "sys.%s" % name)
            if fresh is not None:
                setattr(sys, name, fresh)
        except Exception as e:
            failed.append("sys.%s (%s)" % (name, type(e).__name__))

    for ref in list(getattr(logging, "_handlerList", [])):
        handler = ref()
        try:
            if handler is None or isinstance(getattr(type(handler), "stream", None), property):
                continue  # e.g. logging.lastResort, which looks up sys.stderr when writing
            inherited = getattr(handler, "stream", None)
            if id(inherited) not in swapped and not isinstance(handler, logging.FileHandler):
                continue  # not writing to stdout/stderr or to a log file
            fresh = swap(inherited, "%s %r" % (type(handler).__name__, handler.name))
            if fresh is not None:
                handler.stream = fresh
        except Exception as e:
            failed.append("%s (%s)" % (type(handler).__name__, type(e).__name__))

    if failed and id(stderr) in swapped:
        try:
            # created above, so no other thread can be holding its lock
            swapped[id(stderr)].write(
                "Worker %s kept streams inherited from the hub, writing to them could block: %s\n"
                % (os.getpid(), ", ".join(failed))
            )
            swapped[id(stderr)].flush()
        except Exception:
            pass


class JobManager:
    # TODO: Add class docstring
    COLUMNS = [
        "pid",
        "source",
        "category",
        "step",
        "description",
        "mem",
        "cpu",
        "started_at",
        "duration",
    ]
    HEADER = dict(zip(COLUMNS, [c.upper() for c in COLUMNS]))  # upper() for column titles
    HEADERLINE = "{pid:^10}|{source:^35}|{category:^10}|{step:^20}|{description:^30}|{mem:^10}|{cpu:^6}|{started_at:^20}|{duration:^10}"
    DATALINE = HEADERLINE.replace("^", "<")

    def _free_threaded_mode(self):
        """True when running a free-threaded (no-GIL) Python build and the
        config opted in to run CPU-bound workers in threads instead of
        processes (HUB_FREE_THREADED_WORKERS)."""
        if not getattr(config, "HUB_FREE_THREADED_WORKERS", False):
            return False
        return hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()

    def _create_worker_queue(self):
        """Create the executor behind defer_to_worker(): a thread pool in
        free-threaded mode, a forked process pool otherwise."""
        if self._free_threaded_mode():
            # the GIL is disabled: CPU-bound workers can run in threads,
            # sharing the hub's heap, with no fork and no job pickling
            logger.info("Free-threaded mode: using a thread pool for CPU-bound workers")
            return concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix="FTWorker")
        kwargs = {}
        # since Python 3.14, multiprocessing uses `forkserver` as the default, instead of 'fork'
        # on POSIX systems. This breaks our current biothings JobManager when creating dynamic
        # classes in worker processes (e.g. AssistedDumper_<src_name> class), as the 'forkserver'
        # context does not inherit resources from the parent process.
        # This is a quick fix to force using 'fork' context for ProcessPoolExecutor in 3.14,
        # consistent with previous Python versions.
        # REF: https://docs.python.org/3.14/library/multiprocessing.html#contexts-and-start-methods
        # TODO: we should consider refactoring the code to be compatible with 'forkserver' context in the future.
        try:
            kwargs["mp_context"] = multiprocessing.get_context("fork")
            # forked workers inherit the hub's stream objects, including any lock
            # a hub thread held at that moment: give them fresh ones first
            kwargs["initializer"] = _reset_inherited_streams
        except ValueError:
            pass
        return concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers, **kwargs)

    def __init__(
        self,
        loop,
        worker_queue=None,
        thread_queue=None,
        max_memory_usage=None,
        num_workers=None,
        num_threads=None,
        auto_recycle=True,
        *,
        process_queue=_UNSET,
    ):
        if not os.path.exists(config.RUN_DIR):
            logger.info("Creating RUN_DIR directory '%s'", config.RUN_DIR)
            os.makedirs(config.RUN_DIR)
        self.loop = loop  # usu. it's the asyncio event loop
        self.num_workers = num_workers
        if self.num_workers == 0:
            logger.debug("Adjusting number of worker to 1")
            self.num_workers = 1
        self.num_threads = num_threads or self.num_workers
        if process_queue is not _UNSET:
            _warn_deprecated("JobManager's process_queue argument", "worker_queue")
            if worker_queue is not None and process_queue is not None:
                raise TypeError("JobManager got both worker_queue and its deprecated alias process_queue")
            worker_queue = worker_queue or process_queue
        # runs defer_to_worker() jobs: worker processes, or worker threads in
        # free-threaded mode (see worker_mode)
        self.worker_queue = worker_queue or self._create_worker_queue()
        # notes on fixing BPE (BrokenProcessPool Exception):
        # whenever a process exits unexpectedly, BPE is raised, and while that
        # all the processes in the pool gets a SIGTERM from the management
        # thread (see _queue_management_worker in concurrent.futures.process)
        # TODO: limit the number of threads (as argument) ?
        self.thread_queue = thread_queue or concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads)

        #  In Py38 using an executor that is not a ThreadPoolExecutor is
        #  deprecated. And it seems in Py39 , it must be a ThreadPoolExecutor,
        #  using a ProcessPoolExecutor will trigger an error.
        #  However, loop.run_in_executor still accepts ProcessPoolExecutor
        #  see https://bugs.python.org/issue34075
        self.loop.set_default_executor(self.thread_queue)
        # this lock is acquired when defer_to_worker/thread is invoked
        # and released when the inner coroutine is run
        #  purpose being: "control job submission", as it only creates a new
        #  "task" when the previous one has completed checking its constraints
        self.ok_to_run = asyncio.Semaphore()
        # auto-creata RUN_DIR
        if not os.path.exists(config.RUN_DIR):
            os.makedirs(config.RUN_DIR)

        if max_memory_usage == "auto":
            # try to find a nice limit...
            limited = int(psutil.virtual_memory().available * 0.6)
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
        if auto_recycle and self.worker_mode == "thread":
            # free-threaded workers share the hub's heap, recycling the
            # executor wouldn't reclaim any memory
            logger.info("Free-threaded workers: disabling worker queue auto-recycling")
            auto_recycle = False
        self.auto_recycle = auto_recycle  # active
        self.auto_recycle_setting = auto_recycle  # keep setting if we need to restore it its orig value
        self.jobs = {}  # all active jobs (thread/process)
        # _worker_job_ids is for storing Job IDs of calls deferred in the
        # worker queue, so that when Executor is recreated, staled Job IDs can
        # be removed. Free-threaded mode also counts pending jobs with it.
        # FIXME: drop this when structure of pinfo is clear so we can rely on
        #  that instead of storing _worker_job_ids
        self._worker_job_ids = set()
        self._pchildren = []
        logger.info(
            "Job manager: worker queue runs jobs in %s mode with up to %s workers, thread queue with up to %s threads",
            self.worker_mode,
            getattr(self.worker_queue, "_max_workers", "?"),
            getattr(self.thread_queue, "_max_workers", "?"),
        )
        self.clean_staled()

    @property
    def worker_mode(self):
        """How the worker queue runs defer_to_worker() jobs: "process" for
        worker processes, "thread" for worker threads (free-threaded mode),
        None when there's no worker queue or it's another kind of executor."""
        if isinstance(self.worker_queue, concurrent.futures.ThreadPoolExecutor):
            return "thread"
        if isinstance(self.worker_queue, concurrent.futures.ProcessPoolExecutor):
            return "process"
        return None

    @property
    def process_queue(self):
        """Deprecated alias of worker_queue."""
        _warn_deprecated("JobManager.process_queue", "JobManager.worker_queue")
        return self.worker_queue

    @process_queue.setter
    def process_queue(self, executor):
        _warn_deprecated("JobManager.process_queue", "JobManager.worker_queue")
        self.worker_queue = executor

    def stop(self, force=False, recycling=False, wait=1):
        async def do():
            try:
                # shutting down the worker queue can take a while
                # if some workers are still running (it'll wait until they're done)
                # we'll wait in a thread to prevent the hub from being blocked
                logger.info("Shutting down current worker queue...")
                pinfo = {
                    "__skip_check__": True,  # skip sanity check, mem check to make sure
                    # this worker will be run
                    "category": "admin",
                    "source": "maintenance",
                    "step": "",
                    "description": "Stopping worker queue",
                }
                # await on coroutine
                j = await self.defer_to_thread(pinfo, self.worker_queue.shutdown)
                # await on the future to be done
                await j
                if recycling:
                    # now replace
                    logger.info("Replacing worker queue with new one")
                    self.worker_queue = self._create_worker_queue()
                else:
                    self.worker_queue = None
            except Exception as e:
                logger.error("Error while recycling the worker queue: %s", e)
                raise

        async def kill():
            nonlocal wait
            if wait < 1:
                wait = 1  # wait a little bit so job manager has time to stop if nothing is running
            logger.warning("Wait %s seconds before killing queue processes", wait)
            await asyncio.sleep(wait)
            logger.warning("Can't wait anymore, killing running processed in the queue !")
            for proc in self.pchildren:
                logger.warning("Killing %s", proc)
                proc.kill()

        task = self.loop.create_task(do())
        # retrieve a potential exception so asyncio doesn't complain when the
        # caller (e.g. hub shell's restart) doesn't await the returned task
        task.add_done_callback(lambda t: t.cancelled() or t.exception())
        if force:
            self.loop.create_task(kill())
        return task

    def clean_staled(self):
        # clean old/staled files
        children_pids = {p.pid for p in self.pchildren}
        active_tids = {t.name for t in self.thread_queue._threads}
        ft_active = {t.name for t in getattr(self.worker_queue, "_threads", None) or []}
        ft_pat = re.compile(r"(?P<pid>\d+)-(?P<tid>FTWorker_\d+)")

        # Inspect one snapshot. A worker may create another tracking file while
        # cleanup is running; it can be considered on the next pass instead of
        # being misclassified between separate process/thread scans.
        for fn in glob.glob(os.path.join(config.RUN_DIR, "*.pickle")):
            stem = os.path.splitext(os.path.basename(fn))[0]
            try:
                worker_id, _job_id = stem.rsplit("_", 1)
            except ValueError:
                logger.warning("Unrecognized worker file '%s', skip it", fn)
                continue

            stale = False
            worker_type = None
            if worker_id.isdigit():
                worker_type = "pid"
                stale = int(worker_id) not in children_pids
            elif match := ft_pat.fullmatch(worker_id):
                worker_type = "free-threaded worker"
                stale = int(match.group("pid")) != os.getpid() or match.group("tid") not in ft_active
            elif worker_id.startswith("Thread"):
                worker_type = "thread"
                stale = worker_id not in active_tids
            else:
                logger.warning("Unrecognized worker file '%s', skip it", fn)
                continue

            if stale:
                logger.info("Removing staled %s file '%s'", worker_type, fn)
                try:
                    os.unlink(fn)
                except FileNotFoundError:
                    # The worker may have finished and removed its own tracking
                    # file after the snapshot was taken.
                    pass

    def recycle_worker_queue(self):
        """
        Replace current worker queue with a new one. When worker processes
        are used over and over again, memory tends to grow as python
        interpreter keeps some data (...). Calling this method will
        perform a clean shutdown on current queue, waiting for running
        workers to terminate, then discard current queue and replace
        it a new one. Free-threaded workers share the hub's memory, so
        recycling them doesn't free any.
        """
        return self.stop(recycling=True)

    def recycle_process_queue(self):
        """Deprecated alias of recycle_worker_queue()."""
        _warn_deprecated("JobManager.recycle_process_queue()", "JobManager.recycle_worker_queue()")
        return self.recycle_worker_queue()

    async def check_constraints(self, pinfo=None):
        mem_req = pinfo and pinfo.get("__reqs__", {}).get("mem") or 0
        t0 = time.time()
        waited = False
        sleep_time = 5
        if mem_req:
            logger.info(
                "Job {cat:%s,source:%s,step:%s} requires %s memory, checking if available",
                pinfo.get("category"),
                pinfo.get("source"),
                pinfo.get("step"),
                sizeof_fmt(mem_req),
            )
        if self.max_memory_usage:
            hub_mem = self.hub_memory
            while hub_mem >= self.max_memory_usage:
                if self.auto_recycle:
                    pworkers = self.get_worker_files()
                    tworkers = self.get_thread_files()
                    if len(pworkers) == 0 and len(tworkers) == 0:
                        logger.info("No worker running, recycling the worker queue...")
                        await self.recycle_worker_queue()
                        # still out of memory ?
                        avail_mem = self.max_memory_usage - self.hub_memory
                        if avail_mem <= 0:
                            logger.error(
                                "After recycling worker queue, "
                                "memory usage is still too high (needs at least %s more)"
                                "now turn auto-recycling off to prevent infinite recycling...",
                                sizeof_fmt(abs(avail_mem)),
                            )
                            self.auto_recycle = False
                logger.info(
                    "Hub is using too much memory to launch job {cat:%s,source:%s,step:%s}"
                    " (%s used, more than max allowed %s), wait a little (job's already been postponed for %s)",
                    pinfo.get("category"),
                    pinfo.get("source"),
                    pinfo.get("step"),
                    sizeof_fmt(hub_mem),
                    sizeof_fmt(self.max_memory_usage),
                    timesofar(t0),
                )
                await asyncio.sleep(sleep_time)
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
                    pinfo.get("category"),
                    pinfo.get("source"),
                    pinfo.get("step"),
                    sizeof_fmt(mem_req),
                    sizeof_fmt(hub_mem),
                    sizeof_fmt(max_mem),
                    timesofar(t0),
                )
                await asyncio.sleep(sleep_time)
                waited = True
                # refresh limites and usage (manager can be modified from hub
                # thus memory usage can be modified on-the-fly
                hub_mem = self.hub_memory
                max_mem = self.max_memory_usage and self.max_memory_usage or self.avail_memory
        pendings = self._pending_jobs_count() - config.HUB_MAX_WORKERS
        while pendings >= config.MAX_QUEUED_JOBS:
            if not waited:
                logger.info(
                    "Can't run job {cat:%s,source:%s,step:%s} right now, too much pending jobs in the queue (max: %s), will retry until possible",
                    pinfo.get("category"),
                    pinfo.get("source"),
                    pinfo.get("step"),
                    config.MAX_QUEUED_JOBS,
                )
            await asyncio.sleep(sleep_time)
            pendings = self._pending_jobs_count() - config.HUB_MAX_WORKERS
            waited = True
        # finally check custom predicates
        predicates = pinfo and pinfo.get("__predicates__", [])
        failed_predicate = None
        predicates_t0 = None
        warn_after = None
        while True:
            for predicate in predicates:
                if not predicate(self):
                    failed_predicate = predicate
                    break  # for loop (most inner one)
                else:
                    # reset flag
                    failed_predicate = None
            if failed_predicate:
                logger.info(
                    "Can't run job {cat:%s,source:%s,step:%s} right now, predicate %s failed, will retry until possible",
                    pinfo.get("category"),
                    pinfo.get("source"),
                    pinfo.get("step"),
                    failed_predicate,
                )
                if predicates_t0 is None:
                    predicates_t0 = time.time()
                    warn_after = getattr(config, "HUB_PREDICATE_WAIT_WARNING", None)
                await asyncio.sleep(sleep_time)
                waited = True
                if warn_after and time.time() - predicates_t0 >= warn_after:
                    # this job holds ok_to_run while it waits, no other job can be admitted
                    logger.warning(
                        "Job {cat:%s,source:%s,step:%s} has been waiting %s for predicate %s, "
                        "no other job can start until it does",
                        pinfo.get("category"),
                        pinfo.get("source"),
                        pinfo.get("step"),
                        timesofar(predicates_t0),
                        getattr(failed_predicate, "__name__", failed_predicate),
                        extra={"notify": True},
                    )
                    warn_after = None  # once per job
            else:
                break  # while loop
        if waited:
            logger.info(
                "Job {cat:%s,source:%s,step:%s} now can be launched (total waiting time: %s)",
                pinfo.get("category"),
                pinfo.get("source"),
                pinfo.get("step"),
                timesofar(t0),
            )
            # auto-recycle could have been temporarily disabled until more mem is assigned.
            # if we've been able to run the job, it means we had enough mem so restore
            # recycling setting (if auto_recycle was False, it's ignored
            if self.auto_recycle_setting:
                self.auto_recycle = self.auto_recycle_setting

    def _ensure_worker_queue_alive(self):
        if not isinstance(self.worker_queue, concurrent.futures.ProcessPoolExecutor):
            # thread pools (free-threaded workers mode) can't break this way
            return
        try:
            # test to see if Executor still alive
            _ = self.worker_queue.submit(int, 1)
        except concurrent.futures.process.BrokenProcessPool as e:
            # recreate if not
            # we don't need to care about the remaining tasks because
            # they'd all be SIGTERM'd anyways. But ...
            logger.warning("Broken Process Pool: %s, restarting.", e)
            self.worker_queue = self._create_worker_queue()
            for stale_id in self._worker_job_ids:
                self.jobs.pop(stale_id, None)  # in the rare case that
                # somehow they de-sync
            self._worker_job_ids.clear()

    def _pending_jobs_count(self):
        """Number of jobs submitted to the worker queue and not done yet
        (including the ones currently running in a worker)."""
        if isinstance(self.worker_queue, concurrent.futures.ThreadPoolExecutor):
            # free-threaded workers mode: a thread pool doesn't track running
            # jobs and its idle threads stay alive, so count the jobs deferred
            # to it and not reaped yet
            return len(self._worker_job_ids)
        return len(self.worker_queue._pending_work_items)

    async def _reap(self, fut, job_id, worker=False):
        """Await an executor future and clean up the job registry, keeping it
        in sync with actually running jobs. Returns the worker's result,
        raises its exception."""
        try:
            res = await fut
            # the worker could itself generate other parallelized jobs and
            # return a Future/Task; if so, make sure we get its results too
            if isinstance(res, asyncio.Task):
                res = await res
            return res
        finally:
            self.jobs.pop(job_id, None)
            if worker:
                self._worker_job_ids.discard(job_id)

    async def defer_to_worker(self, pinfo=None, func=None, *args, **kwargs):
        """Submit func to the worker queue, as soon as job constraints
        (memory, queue depth, predicates) allow it. Workers are processes, or
        threads in free-threaded mode (see worker_mode). Blocks until the job
        is admitted and submitted, then returns an asyncio.Task resolving to
        the worker's result (awaiting it raises the worker's exception, if any)."""
        # ok_to_run serializes admission: only one job at a time checks its
        # constraints, providing submission backpressure to the pipelines
        async with self.ok_to_run:
            await self.check_constraints(pinfo)
            # pinfo can contain predicates hardly pickleable during run_in_executor
            # but we also need not to touch the original one
            copy_pinfo = copy.deepcopy(pinfo)
            copy_pinfo.pop("__predicates__", None)
            self._ensure_worker_queue_alive()
            job_id = get_random_string()
            self.jobs[job_id] = copy_pinfo
            self._worker_job_ids.add(job_id)
            fut = self.loop.run_in_executor(
                self.worker_queue,
                partial(do_work, job_id, "worker", copy_pinfo, func, *args, **kwargs),
            )
            # do_work will create and clean up the pickle files unless
            # the worker process gets killed unexpectedly
        return asyncio.create_task(self._reap(fut, job_id, worker=True))

    async def defer_to_process(self, pinfo=None, func=None, *args, **kwargs):
        """Deprecated alias of defer_to_worker(), whose workers may be threads."""
        _warn_deprecated("JobManager.defer_to_process()", "JobManager.defer_to_worker()")
        return await self.defer_to_worker(pinfo, func, *args, **kwargs)

    async def defer_to_thread(self, pinfo=None, func=None, *args, **kwargs):
        """Same as defer_to_worker(), but always runs func in the thread
        queue, inside the hub process."""
        skip_check = pinfo.get("__skip_check__", False)

        def submit():
            job_id = get_random_string()
            self.jobs[job_id] = pinfo
            fut = self.loop.run_in_executor(
                self.thread_queue,
                partial(do_work, job_id, "thread", pinfo, func, *args, **kwargs),
            )
            return fut, job_id

        if skip_check:
            fut, job_id = submit()
        else:
            async with self.ok_to_run:
                await self.check_constraints(pinfo)
                fut, job_id = submit()
        return asyncio.create_task(self._reap(fut, job_id))

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
        # the wrapper coroutine takes its name from func, otherwise all
        # scheduled jobs would have the same wrapping coroutine name
        if isinstance(func, partial):
            func_name = func.func.__name__
        else:
            func_name = func.__name__

        async def run_func():
            func(*args, **kwargs)

        run_func.__name__ = func_name
        run_func.__qualname__ = func_name
        return self.submit(run_func, schedule=crontab)

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
            pat = re.compile(r".*/(\d+)_.*\.pickle")  # see track() for filename format
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
                            "cpu": proc.cpu_percent(),
                        }
                        pids[pid] = worker
                except IndexError:
                    # weird though... should have only pid files there...
                    pass
        except Exception:
            pass
        return pids

    def get_worker_files(self, child=None):
        """Tracking files of the jobs running in the worker queue, keyed by
        worker pid, or by worker thread name in free-threaded mode."""
        if self.worker_mode != "thread":
            return self.get_pid_files(child)
        workers = {}
        pat = re.compile(r".*/\d+-(FTWorker_\d+)_.*\.pickle")  # see track() for filename format
        for fn in glob.glob(os.path.join(config.RUN_DIR, "*.pickle")):
            match = pat.match(fn)
            if not match:
                continue
            try:
                with open(fn, "rb") as f:
                    workers[match.group(1)] = pickle.load(f)
            except Exception:
                # the job may have just finished and removed its file, or may
                # still be writing it
                continue
        return workers

    def get_thread_files(self):
        tids = {}
        try:
            # see track() for filename format; free-threaded worker files
            # ("<pid>-FTWorker_<n>_<jobid>") belong to get_worker_files()
            pat = re.compile(r".*/(Thread\w*-\d+)_.*\.pickle")
            # threads = self.thread_queue._threads
            # active_tids = [t.getName() for t in threads]
            for fn in glob.glob(os.path.join(config.RUN_DIR, "*.pickle")):
                try:
                    tid = pat.findall(fn)[0].split("_")[0]
                    worker = pickle.load(open(fn, "rb"))
                    worker["process"] = self.hub_process  # misleading... it's the hub process
                    tids[tid] = worker
                except IndexError:
                    # weird though... should have only pid files there...
                    pass
        except Exception:
            pass
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
        # if proc:
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
        if self.worker_mode == "thread":
            return self._get_worker_thread_summary()
        running_pids = self.get_pid_files()
        res = {}
        for child in self.pchildren:
            try:
                # mem = child.memory_info().rss
                child.memory_info().rss
                try:
                    pio = child.io_counters()
                except AttributeError:
                    # workaround for OS w/o this feature
                    # namely macOS
                    pio = type(
                        "",
                        (),
                        {
                            "read_count": -1,
                            "write_count": -1,
                            "read_bytes": -1,
                            "write_bytes": -1,
                        },
                    )()
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
                        "percent": cpu,
                    },
                    "io": {
                        "read_count": pio.read_count,
                        "write_count": pio.write_count,
                        "read_bytes": pio.read_bytes,
                        "write_bytes": pio.write_bytes,
                    },
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

    def _get_worker_thread_summary(self):
        """get_process_summary() for free-threaded mode: one entry per worker
        thread, shaped like a worker process entry since API clients (Studio)
        read its cpu, memory and job fields."""
        running_tids = self.get_worker_files()
        res = {}
        for thread in list(self.worker_queue._threads):
            worker = running_tids.get(thread.name)
            res[thread.name] = {
                # a thread's memory and CPU usage can't be measured apart from
                # the hub's, report zeros
                "memory": {"size": 0, "percent": 0.0},
                "cpu": {"status": "running" if worker else "sleeping", "percent": 0.0},
                "io": {"read_count": -1, "write_count": -1, "read_bytes": -1, "write_bytes": -1},
            }
            if worker:
                res[thread.name]["job"] = {
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

    def get_thread_summary(self):
        running_tids = self.get_thread_files()
        tchildren = list(self.thread_queue._threads)
        res = {}
        for child in tchildren:
            res[child.name] = {
                "is_alive": child.is_alive(),
                "is_daemon": child.daemon,
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
        pworkers = self.get_worker_files(child)
        tworkers = self.get_thread_files()
        ppendings = self.get_pending_processes()
        tpendings = {}  # TODO:
        return {
            # the worker queue, kept under "process" for API clients (Studio);
            # "mode" tells whether its workers are processes or threads
            "process": {
                "running": list(pworkers.keys()),
                "pending": list(ppendings.keys()),
                "all": self.get_process_summary(),
                "max": self.worker_queue._max_workers,
                "mode": self.worker_mode,
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
            "hub_pid": self.hub_process.pid,
        }

    def get_pending_summary(self, getstr=False):
        running = len(self.get_worker_files())
        return "%d pending job(s)" % (self._pending_jobs_count() - running)

    def get_pending_processes(self):
        if not isinstance(self.worker_queue, concurrent.futures.ProcessPoolExecutor):
            # free-threaded workers mode: no per-job introspection available
            return {}
        # pendings are kept in queue while running, until result is there so we need
        # to adjust the actual real pending jobs. also, pending job are get() from the
        # queue following FIFO order. finally, worker ID is incremental. So...
        pendings = sorted(self.worker_queue._pending_work_items.items())
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
        # pending = False
        # done = False
        # run = False
        # pid = None
        child = None
        # if action:
        #    try:
        #        # want to see details for a specific process ?
        #        pid = int(action)
        #        child = [p for p in pchildren if p.pid == pid][0]
        #    except ValueError:
        #        pass
        pworkers = self.get_worker_files(child)
        tworkers = self.get_thread_files()
        done_jobs = glob.glob(os.path.join(config.RUN_DIR, "done", "*.pickle"))
        out = []
        if child:
            return pworkers[child.pid]
        elif action == "pending":
            return self.show_pendings(running=len(pworkers))
        elif action == "summary":
            res = self.get_summary()
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
        # prunning = summary["process"]["running"]
        # trunning = summary["thread"]["running"]
        # ppending = summary["process"]["pending"]
        # tpending = summary["thread"]["pending"]
        return {
            "queue": {
                "process": summary["process"],
                "thread": summary["thread"],
            },
            "memory": summary["memory"],
            "available_system_memory": summary["available_system_memory"],
            "max_memory_usage": summary["max_memory_usage"],
            "hub_pid": summary["hub_pid"],
        }
