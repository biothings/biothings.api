import asyncio
import concurrent.futures
import copy
import datetime
import enum
import inspect
import multiprocessing
import os
import re
import sys
import threading
import time
import traceback
from collections import OrderedDict
from functools import partial
from pathlib import Path
from pprint import pformat
from typing import Callable, Optional

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


@enum.unique
class PoolType(enum.Enum):
    """Enumeration for us to determine our pool type.

    Used in the job operation to allow us to calculate
    a pool identifier based off metadata from either the
    process for a process pool or thread name for a thread
    pool
    """

    THREAD = enum.auto()
    PROCESS = enum.auto()


def job_pool_operation(
    callback: partial, job_id: str, pool_identifier: PoolType, job_directory: Path, pinfo: Optional[dict] = None
):
    """Operation executor for anything submitted to our Process/Thread Pool."""

    function_name = callback.func.__name__
    if pinfo is None:
        pinfo = {"category": None, "source": None, "step": None, "description": function_name}
    else:
        pinfo = dict(pinfo)

    # predicates can't be pickles/dilled
    pinfo.pop("__predicates__", None)

    worker = {
        "func_name": function_name,
        "args": [str(arg) for arg in callback.args],
        "kwargs": {str(k): str(v) for k, v in callback.keywords.items()},
        "job": pinfo,
    }

    pinfo["started_at"] = time.time()

    results = None
    trace = None
    pidfile = None

    try:
        worker_id = None
        if pool_identifier == PoolType.THREAD:
            worker_id = threading.current_thread().name
        elif pool_identifier == PoolType.PROCESS:
            worker_id = os.getpid()
        else:
            logger.warning("Unable to determine pool identifier: %s", pool_identifier)
            worker_id = "Unknown"
        worker["job"]["id"] = worker_id

        # add random chars: 2 jobs handled by the same slot (pid or thread)
        # would override filename otherwise
        fn = f"{worker_id}_{job_id}"
        pidfile = job_directory.joinpath(f"{fn}.pickle")
        with open(pidfile, "wb") as pid_handle:
            pickle.dump(worker, pid_handle)
        results = callback()
    except Exception as exc:
        trace = traceback.format_exc()
        logger.error("job manager operation error %s\n%s", exc, trace)
        raise exc
    finally:
        if pidfile and os.path.exists(pidfile):
            logger.debug("Remove PID file '%s'", pidfile)
            os.unlink(pidfile)
    return results


def norm(value, maxlen):
    """just a helper to clean/prepare job's values printing"""
    if len(value) > maxlen:
        value = "...%s" % value[-maxlen + 3 :]
    return value


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
    HEADER = dict(zip(COLUMNS, [c.upper() for c in COLUMNS]))
    HEADERLINE = "{pid:^10}|{source:^35}|{category:^10}|{step:^20}|{description:^30}|{mem:^10}|{cpu:^6}|{started_at:^20}|{duration:^10}"
    DATALINE = HEADERLINE.replace("^", "<")

    def _get_process_executor(self):
        kwargs = {}
        if sys.version_info >= (3, 7):
            # since Python 3.14, multiprocessing uses `forkserver` as the default, instead of 'fork'
            # on POSIX systems. This breaks our current biothings JobManager when creating dynamic
            # classes in worker processes (e.g. AssistedDumper_<src_name> class), as the 'forkserver'
            # context does not inherit resources from the parent process.
            # This is a quick fix to force using 'fork' context for ProcessPoolExecutor in 3.14,
            # consistent with previous Python versions.
            # REF: https://docs.python.org/3.14/library/multiprocessing.html#contexts-and-start-methods
            # TODO: we should consider refactoring the code to be compatible with 'forkserver' context in the future.
            try:
                kwargs["mp_context"] = multiprocessing.get_context("forkserver")
            except ValueError:
                pass
        return concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers, **kwargs)

    def __init__(
        self,
        loop,
        process_queue: concurrent.futures.ProcessPoolExecutor = None,
        thread_queue: concurrent.futures.ThreadPoolExecutor = None,
        max_memory_usage: str | int = None,
        num_workers: int = None,
        num_threads: int = None,
        auto_recycle: bool = True,
    ):

        # TODO Should specify the RUN_DIR as an argument to the job manager
        self.job_directory = Path(config.RUN_DIR)
        if not self.job_directory.exists():
            logger.info("Creating `RUN_DIR` directory [%s]", self.job_directory)
            self.job_directory.mkdir(parents=True, exist_ok=True)

        self.num_workers = num_workers
        if self.num_workers == 0:
            logger.debug("Adjusting number of worker to 1")
            self.num_workers = 1

        self.loop = loop  # usu. it's the asyncio event loop
        self.num_threads = num_threads or self.num_workers
        self.process_queue = process_queue or self._get_process_executor()
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

        # this lock is acquired when defer_to_process/thread is invoked
        # and released when the inner coroutine is run
        #  purpose being: "control job submission", as it only creates a new
        #  "task" when the previous one has completed checking its constraints
        self.ok_to_run = asyncio.Semaphore()

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
        self.auto_recycle = auto_recycle  # active
        self.auto_recycle_setting = auto_recycle  # keep setting if we need to restore it its orig value

        self.jobs = {}  # all active jobs (thread/process)
        # _process_job_ids is for storing Job IDs of calls deferred in process
        # executor, so that when Executor is recreated, staled Job IDs can
        # be removed

        # FIXME: drop this when structure of pinfo is clear so we can rely on
        #  that instead of storing _process_job_ids
        self._process_job_ids = set()
        self._pchildren = []
        self.clean_staled()

    @property
    def hub_process(self) -> psutil.Process:
        """JobManager property that stores the psutil.Process object."""
        if not self._phub:
            pid = os.getpid()
            try:
                self._phub = psutil.Process(pid)
            except psutil.NoSuchProcess as process_not_found:
                logger.exception(process_not_found)
                logger.error("Unable to get process information for hub process via PID: %s", pid)
                raise process_not_found

        return self._phub

    @property
    def pchildren(self) -> list[psutil.Process]:
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

    def stop(self, force: bool = False, recycling: bool = False, wait: int = 1) -> asyncio.Task:
        """JobManager method for handling graceful process queue shutdown.

        shutting down the process queue can take a while
        if some processes are still running (it'll wait until they're done)
        we'll wait in a thread to prevent the hub from being blocked
        """

        async def graceful_shutdown():
            try:
                logger.info("Shutting down current process queue...")
                pinfo = {
                    "__skip_check__": True,  # skip sanity check, mem check to make sure this worker will be run
                    "category": "admin",
                    "source": "maintenance",
                    "step": "",
                    "description": "Stopping process queue",
                }
                stop_function = partial(self.process_queue.shutdown)
                shutdown_task = await self.defer_to_thread(pinfo, stop_function)
                await shutdown_task

                if recycling:
                    logger.info("Replacing process queue with new one")
                    self.process_queue = self._get_process_executor()
                else:
                    self.process_queue = None
            except Exception as gen_exc:
                logger.error("Error while recycling the process queue: %s", gen_exc)
                raise

        async def force_shutdown(wait: int = 1):
            # wait a little bit so job manager has time to stop if nothing is running
            wait = max(wait, 1)
            logger.warning("Wait %s seconds before killing queue processes", wait)
            await asyncio.sleep(wait)
            logger.warning("Can't wait anymore, killing running processed in the queue !")
            for proc in self.pchildren:
                logger.warning("Killing %s", proc)
                proc.kill()

        if force:
            shutdown_task = asyncio.create_task(force_shutdown(wait=wait))
        else:
            shutdown_task = asyncio.create_task(graceful_shutdown())
        return shutdown_task

    def clean_staled(self):
        # clean old/staled files
        children_pids = [p.pid for p in self.pchildren]
        active_tids = [t.getName() for t in self.thread_queue._threads]
        pid_pat = re.compile(r".*/(\d+)_.*\.pickle")  # see track() for filename format

        for fn in self.job_directory.glob("*.pickle"):
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
        tid_pat = re.compile(r".*/(Thread\w*-\d+)_.*\.pickle")

        for fn in self.job_directory.glob("*.pickle"):
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
                    pworkers = self.get_pid_files()
                    tworkers = self.get_thread_files()
                    if len(pworkers) == 0 and len(tworkers) == 0:
                        logger.info("No worker running, recycling the process queue...")

                        # Replace current process queue with a new one. When processes
                        # are used over and over again, memory tends to grow as python
                        # interpreter keeps some data (...). Calling this method will
                        # perform a clean shutdown on current queue, waiting for running
                        # processes to terminate, then discard current queue and replace
                        # it a new one.
                        try:
                            stop_task = self.stop(recycling=True)
                            await stop_task
                        except Exception as gen_exc:
                            raise gen_exc
                        finally:
                            # check availabel memory
                            avail_mem = self.max_memory_usage - self.hub_memory
                            if avail_mem <= 0:
                                logger.error(
                                    "After recycling process queue, "
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

        pendings = len(self.process_queue._pending_work_items.keys()) - config.HUB_MAX_WORKERS
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
                logger.info(
                    "Can't run job {cat:%s,source:%s,step:%s} right now, predicate %s failed, will retry until possible",
                    pinfo.get("category"),
                    pinfo.get("source"),
                    pinfo.get("step"),
                    failed_predicate,
                )
                await asyncio.sleep(sleep_time)
                waited = True
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

    def _check_broken_process_pool(self):
        """Checks if our ProcessPool is still alive.

        If we receive a BrokenProcessPool exception, we clear the entire
        queue in order to restart the pool before proceeding

        We use a banal submission to evaluate this, assuming that if we
        cannot execute the submission then the pool must be broken, but
        BrokenProcessPool is only raised when a worker in the pool terminates
        uncleanly. In the future we should probably look at the
        BrokenExecutor exception as that indicates something is wrong with
        the pool itself
        https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.BrokenExecutor
        """
        try:
            # test to see if Executor still alive
            self.process_queue.submit(int, 1)
        except concurrent.futures.process.BrokenProcessPool as e:
            # recreate if not
            # we don't need to care about the remaining tasks because
            # they'd all be SIGTERM'd anyways. But ...
            logger.warning("Broken Process Pool: %s, restarting.", e)
            self.process_queue = self._get_process_executor()
            for stale_id in self._process_job_ids:
                self.jobs.pop(stale_id, None)  # in the rare case that
                # somehow they de-sync
            self._process_job_ids.clear()

    async def defer_to_process(self, pinfo: dict, func: partial) -> asyncio.Task:
        """Main API method for the JobManager to run a process in the process pool.

        Parameters
        ----------
        pinfo : dict
            The process information dictionary containing various process metadata.
            Has the following attributes:
                >>> pid
                >>> source
                >>> category
                >>> step
                >>> description
                >>> mem
                >>> cpu
                >>> started_at
                >>> duration

            Lesser used / hidden attributes also found in pinfo
                >>> __predicates__
                >>> __skip_check__
                >>> __reqs__

        func : partial
            The callable function object that represents the callback to execute in
            the new process context. All arguments to the callback should be contained
            within a Partial object so that the `defer_to_process` method can directly
            call it

        Returns
        -------
        None
        """
        # lock is released in run coroutine
        await self.ok_to_run.acquire()
        job_id = get_random_string()

        async def _internal_process_runner(callback: partial, job_id: str, pinfo: dict):
            try:
                await self.check_constraints(pinfo)
                self.ok_to_run.release()
                # pinfo can contain predicates hardly pickleable during run_in_executor
                # but we also need not to touch the original one
                copy_pinfo: dict = copy.deepcopy(pinfo)
                copy_pinfo.pop("__predicates__", None)
                self.jobs[job_id] = copy_pinfo
                self._process_job_ids.add(job_id)

                self._check_broken_process_pool()

                process_job_info = {
                    "callback": callback,
                    "job_id": job_id,
                    "pool_identifier": PoolType.PROCESS,
                    "job_directory": self.job_directory,
                    "pinfo": copy_pinfo,
                }
                logger.debug("Pushing job[%s] to process queue [%s]", job_id, callback.func.__name__)

                process_result = self.process_queue.submit(job_pool_operation, **process_job_info)

                # process could generate other parallelized jobs and return a Future/Task
                if isinstance(process_result, asyncio.Task):
                    process_result = await process_result
                return process_result

            except Exception as gen_exc:
                logger.exception(gen_exc)
                logger.error("Error occured in process executor")
                raise gen_exc
            finally:
                self.ok_to_run.release()
                removed_job = self.jobs.pop(job_id, None)
                self._process_job_ids.discard(job_id)
                logger.debug("Removing job[%s] from tracking: %s", job_id, removed_job)

        process_task = asyncio.create_task(_internal_process_runner(func, job_id, pinfo))
        return process_task

    async def defer_to_thread(self, pinfo=None, func: Callable = None, *args) -> asyncio.Task:
        """Main API method for the JobManager to run a thread in the thread pool.

        Parameters
        ----------
        pinfo : dict
            The process information dictionary containing various process metadata.
            Has the following attributes:
                >>> pid
                >>> source
                >>> category
                >>> step
                >>> description
                >>> mem
                >>> cpu
                >>> started_at
                >>> duration

            Lesser used / hidden attributes also found in pinfo
                >>> __predicates__
                >>> __skip_check__
                >>> __reqs__

        func : partial
            The callable function object that represents the callback to execute in
            the new process context. All arguments to the callback should be contained
            within a Partial object so that the `defer_to_thread` method can directly
            call it

        Returns
        -------
        None
        """
        job_id = get_random_string()
        if not pinfo.get("__skip_check__", False):
            await self.ok_to_run.acquire()

        async def _internal_thread_runner(callback: partial, job_id: str, pinfo: dict):
            try:
                if not pinfo.get("__skip_check__", False):
                    await self.check_constraints(pinfo)
                    self.ok_to_run.release()

                self.jobs[job_id] = pinfo

                logger.debug("Pushing job[%s] to thread queue [%s]", job_id, callback.func.__name__)
                thread_job_info = {
                    "callback": callback,
                    "job_id": job_id,
                    "pool_identifier": PoolType.THREAD,
                    "job_directory": self.job_directory,
                    "pinfo": pinfo,
                }

                thread_future = self.thread_queue.submit(job_pool_operation, **thread_job_info)
                async_future = asyncio.wrap_future(thread_future, loop=asyncio.get_running_loop())
                thread_result = await async_future

                # now handle async results
                if inspect.isawaitable(thread_result) or hasattr(thread_result, "__await__"):
                    thread_result = await thread_result

                return thread_result

            except Exception as gen_exc:
                logger.exception(gen_exc)
                raise
            finally:
                if not pinfo.get("__skip_check__", False):
                    self.ok_to_run.release()
                removed_job = self.jobs.pop(job_id, None)
                logger.debug("Removing job[%s] from tracking: %s", job_id, removed_job)

        thread_task = asyncio.create_task(_internal_thread_runner(func, job_id, pinfo))
        return thread_task

    def submit(self, pfunc: partial, schedule: str = None):
        """
        Helper to submit and run tasks. Tasks will run asynchronously
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

    def get_pid_files(self, child=None):
        pids = {}
        try:
            pat = re.compile(r".*/(\d+)_.*\.pickle")  # see track() for filename format
            children_pids = [p.pid for p in self.pchildren]
            for fn in self.job_directory.glob("*.pickle"):
                try:
                    pid = int(pat.findall(fn)[0].split("_")[0])
                    if not child or child.pid == pid:
                        try:
                            with open(fn, "rb") as pickle_handle:
                                worker = pickle.load(pickle_handle)
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

    def get_thread_files(self):
        tids = {}
        try:
            # see track() for filename format
            pat = re.compile(r".*/(Thread\w*-\d+)_.*\.pickle")
            for fn in self.job_directory.glob("*.pickle"):
                try:
                    tid = pat.findall(fn)[0].split("_")[0]
                    with open(fn, "rb") as pickle_handle:
                        worker = pickle.load(pickle_handle)
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

    def get_thread_summary(self):
        running_tids = self.get_thread_files()
        tchildren = self.thread_queue._threads
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
        pworkers = self.get_pid_files(child)
        tworkers = self.get_thread_files()
        ppendings = self.get_pending_processes()
        tpendings = {}  # TODO:
        return {
            "process": {
                "running": list(pworkers.keys()),
                "pending": list(ppendings.keys()),
                "all": self.get_process_summary(),
                "max": self.process_queue._max_workers,
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
        child = None
        pworkers = self.get_pid_files(child)
        tworkers = self.get_thread_files()
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

            done_jobs = self.job_directory.joinpath("done").glob("*.pickle")
            if done_jobs:
                out.append("%s finished job(s), type 'top(done)' for more" % len(done_jobs))
        else:
            raise ValueError("Unknown action '%s'" % action)

        return "\n".join(out)

    def job_info(self):
        summary = self.get_summary()
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
