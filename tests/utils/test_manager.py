import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

import biothings.utils.manager as manager_module
from biothings.utils.common import get_loop
from biothings.utils.manager import JobManager

PINFO = {
    "category": "test",
    "source": "test_manager",
    "step": "unit",
    "description": "job manager test",
}


def square(x, offset=0):
    # module-level so it stays picklable for the process executor
    return x * x + offset


def boom():
    raise ValueError("worker failure")


def make_manager(**kwargs):
    # must be called from within a running loop (async test)
    return JobManager(asyncio.get_running_loop(), num_workers=2, **kwargs)


def shutdown(manager):
    if manager.process_queue:
        manager.process_queue.shutdown(wait=False)
    manager.thread_queue.shutdown(wait=False)


class TestJobManager:
    def test_init_with_default_executor(self):
        # Given
        loop = get_loop()

        # Action
        manager = JobManager(loop)

        # Asserts
        assert loop == manager.loop
        assert isinstance(manager.loop._default_executor, ThreadPoolExecutor)

        # Given
        thread_executor = ThreadPoolExecutor()
        process_executor = ProcessPoolExecutor()

        # Action
        manager = JobManager(
            loop,
            process_queue=process_executor,
            thread_queue=thread_executor,
        )

        # Asserts
        assert manager.loop._default_executor == thread_executor
        assert manager.loop._default_executor != process_executor

    @pytest.mark.asyncio
    async def test_defer_to_thread_result_and_cleanup(self):
        manager = make_manager()
        try:
            job = await manager.defer_to_thread(dict(PINFO), square, 4, offset=1)
            assert isinstance(job, asyncio.Task)
            assert await job == 17
            assert manager.jobs == {}
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_defer_to_thread_exception_and_cleanup(self):
        manager = make_manager()
        try:
            job = await manager.defer_to_thread(dict(PINFO), boom)
            with pytest.raises(ValueError, match="worker failure"):
                await job
            # registry must be cleaned even when the worker fails
            assert manager.jobs == {}
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_defer_to_thread_skip_check(self):
        manager = make_manager()
        try:

            async def never_admit(pinfo=None):
                raise AssertionError("check_constraints must not be called with __skip_check__")

            manager.check_constraints = never_admit
            pinfo = dict(PINFO, __skip_check__=True)
            job = await manager.defer_to_thread(pinfo, square, 3)
            assert await job == 9
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_defer_to_process_result_and_exception(self):
        manager = make_manager()
        try:
            job = await manager.defer_to_process(dict(PINFO), square, 5)
            assert await job == 25
            failing = await manager.defer_to_process(dict(PINFO), boom)
            with pytest.raises(ValueError, match="worker failure"):
                await failing
            assert manager.jobs == {}
            assert manager._process_job_ids == set()
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_admission_is_serialized(self):
        # ok_to_run must let only one job at a time run its constraint checks
        manager = make_manager()
        try:
            events = []
            release = asyncio.Event()

            async def slow_check(pinfo=None):
                events.append("enter-%s" % pinfo["description"])
                if pinfo["description"] == "first":
                    await release.wait()
                events.append("exit-%s" % pinfo["description"])

            manager.check_constraints = slow_check
            first = asyncio.create_task(manager.defer_to_thread(dict(PINFO, description="first"), square, 1))
            second = asyncio.create_task(manager.defer_to_thread(dict(PINFO, description="second"), square, 2))
            await asyncio.sleep(0.1)
            # second job's check can't start while the first one is admitted
            assert events == ["enter-first"]
            release.set()
            await asyncio.gather(first, second)
            assert events == ["enter-first", "exit-first", "enter-second", "exit-second"]
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_nested_task_result_is_awaited(self):
        # a worker may return an asyncio.Task; awaiting the job must return
        # that nested task's result, not the task object
        manager = make_manager()
        try:

            async def nested():
                return "nested-result"

            nested_task = asyncio.create_task(nested())
            job = await manager.defer_to_thread(dict(PINFO), lambda: nested_task)
            assert await job == "nested-result"
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_stop_recycling_replaces_process_queue(self):
        manager = make_manager()
        try:
            old_queue = manager.process_queue
            await manager.stop(recycling=True)
            assert manager.process_queue is not None
            assert manager.process_queue is not old_queue
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_free_threaded_workers_opt_in(self, monkeypatch):
        import sys

        monkeypatch.setattr(manager_module.config, "HUB_FREE_THREADED_WORKERS", True, raising=False)
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
        manager = make_manager()
        try:
            assert isinstance(manager.process_queue, ThreadPoolExecutor)
            # recycling is pointless with a shared heap, must be off
            assert manager.auto_recycle is False
            job = await manager.defer_to_process(dict(PINFO), square, 6)
            assert await job == 36
            # introspection helpers must not raise in thread mode
            manager.get_pending_summary()
            manager.get_pending_processes()
            manager.get_summary()
            manager.top()
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_free_threaded_workers_distinct_run_files(self, monkeypatch, tmp_path):
        import glob
        import os
        import sys
        import time

        monkeypatch.setattr(manager_module.config, "HUB_FREE_THREADED_WORKERS", True, raising=False)
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
        monkeypatch.setattr(manager_module.config, "RUN_DIR", str(tmp_path), raising=False)
        manager = make_manager()
        try:
            jobs = [await manager.defer_to_process(dict(PINFO), time.sleep, 0.5) for _ in range(2)]
            await asyncio.sleep(0.2)
            run_files = [os.path.basename(f) for f in glob.glob(str(tmp_path / "*.pickle"))]
            # one tracking file per worker thread, named pid-threadname_jobid
            assert len(run_files) == 2
            assert all(f.startswith("%d-FTWorker" % os.getpid()) for f in run_files)
            await asyncio.gather(*jobs)
            assert glob.glob(str(tmp_path / "*.pickle")) == []
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_clean_staled_ignores_unrecognized_run_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(manager_module.config, "RUN_DIR", str(tmp_path), raising=False)
        unrecognized = tmp_path / "unrecognized.pickle"
        unrecognized.write_bytes(b"not a worker tracking file")

        manager = make_manager()
        try:
            assert unrecognized.exists()
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_predicates_block_admission(self):
        manager = make_manager()
        try:
            allowed = False

            def predicate(jm):
                return allowed

            pinfo = dict(PINFO, __predicates__=[predicate])
            task = asyncio.create_task(manager.defer_to_thread(pinfo, square, 2))
            await asyncio.sleep(0.1)
            assert not task.done()
            allowed = True
            job = await task
            assert await job == 4
        finally:
            shutdown(manager)
