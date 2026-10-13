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
    if manager.worker_queue:
        manager.worker_queue.shutdown(wait=False)
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
            worker_queue=process_executor,
            thread_queue=thread_executor,
        )

        # Asserts
        assert manager.loop._default_executor == thread_executor
        assert manager.loop._default_executor != process_executor
        assert manager.worker_queue is process_executor
        assert manager.worker_mode == "process"

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
    async def test_defer_to_worker_result_and_exception(self):
        manager = make_manager()
        try:
            job = await manager.defer_to_worker(dict(PINFO), square, 5)
            assert await job == 25
            failing = await manager.defer_to_worker(dict(PINFO), boom)
            with pytest.raises(ValueError, match="worker failure"):
                await failing
            assert manager.jobs == {}
            assert manager._worker_job_ids == set()
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_defer_to_process_is_deprecated_alias(self):
        manager = make_manager()
        try:
            with pytest.warns(DeprecationWarning, match="defer_to_worker"):
                job = await manager.defer_to_process(dict(PINFO), square, 3)
            assert await job == 9
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_process_queue_names_are_deprecated_aliases(self):
        executor = ThreadPoolExecutor()
        with pytest.warns(DeprecationWarning, match="worker_queue"):
            manager = make_manager(process_queue=executor)
        try:
            assert manager.worker_queue is executor
            with pytest.warns(DeprecationWarning, match="worker_queue"):
                assert manager.process_queue is executor
            with pytest.warns(DeprecationWarning), pytest.raises(TypeError):
                make_manager(worker_queue=executor, process_queue=executor)
            with pytest.warns(DeprecationWarning, match="recycle_worker_queue"):
                await manager.recycle_process_queue()
            assert manager.worker_queue is not executor
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_process_mode_reported_in_job_info(self):
        manager = make_manager()
        try:
            assert manager.worker_mode == "process"
            queue = manager.job_info()["queue"]
            # the worker queue keeps its "process" key for API clients (Studio)
            assert queue["process"]["mode"] == "process"
            assert queue["process"]["max"] == 2
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
    async def test_stop_recycling_replaces_worker_queue(self):
        manager = make_manager()
        try:
            old_queue = manager.worker_queue
            await manager.stop(recycling=True)
            assert manager.worker_queue is not None
            assert manager.worker_queue is not old_queue
        finally:
            shutdown(manager)

    @pytest.mark.asyncio
    async def test_free_threaded_workers_opt_in(self, monkeypatch):
        import sys

        monkeypatch.setattr(manager_module.config, "HUB_FREE_THREADED_WORKERS", True, raising=False)
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
        manager = make_manager()
        try:
            assert isinstance(manager.worker_queue, ThreadPoolExecutor)
            assert manager.worker_mode == "thread"
            # recycling is pointless with a shared heap, must be off
            assert manager.auto_recycle is False
            job = await manager.defer_to_worker(dict(PINFO), square, 6)
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
            jobs = [await manager.defer_to_worker(dict(PINFO), time.sleep, 0.5) for _ in range(2)]
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
    async def test_free_threaded_jobs_reported_with_worker_queue(self, monkeypatch, tmp_path):
        import sys
        import time

        monkeypatch.setattr(manager_module.config, "HUB_FREE_THREADED_WORKERS", True, raising=False)
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
        monkeypatch.setattr(manager_module.config, "RUN_DIR", str(tmp_path), raising=False)
        manager = make_manager()
        try:
            job = await manager.defer_to_worker(dict(PINFO), time.sleep, 0.5)
            await asyncio.sleep(0.2)
            summary = manager.get_summary()
            # a worker thread's job counts with the worker queue, not the thread queue
            assert summary["process"]["mode"] == "thread"
            assert summary["process"]["running"] == ["FTWorker_0"]
            assert summary["thread"]["running"] == []
            # shaped like a worker process entry, the way Studio reads it
            entry = summary["process"]["all"]["FTWorker_0"]
            assert entry["cpu"]["status"] == "running"
            assert entry["memory"]["size"] == 0
            assert entry["job"]["description"] == PINFO["description"]
            assert "1 running job(s)" in manager.top()
            assert manager.get_pending_summary() == "0 pending job(s)"
            await job
            # idle worker threads aren't pending jobs
            assert manager.get_pending_summary() == "0 pending job(s)"
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


class TestCLIJobManager:
    @pytest.mark.asyncio
    async def test_defer_to_worker_runs_inline(self):
        import threading

        pytest.importorskip("typer")  # importing biothings.cli requires it
        from biothings.cli.manager import CLIJobManager

        manager = CLIJobManager(loop=asyncio.get_running_loop())
        job = await manager.defer_to_worker(dict(PINFO), threading.get_ident)
        # CLI jobs run on the event loop's thread, not in a worker
        assert await job == threading.get_ident()

    @pytest.mark.asyncio
    async def test_defer_to_process_is_deprecated_alias(self):
        pytest.importorskip("typer")  # importing biothings.cli requires it
        from biothings.cli.manager import CLIJobManager

        manager = CLIJobManager(loop=asyncio.get_running_loop())
        with pytest.warns(DeprecationWarning, match="defer_to_worker"):
            job = await manager.defer_to_process(dict(PINFO), square, 3)
        assert await job == 9
