"""
Integration test for evaluating the various pieces
required to execute a dump job within our hub
"""

import asyncio
import concurrent.futures
import functools
import time

import psutil
import pytest

from biothings.utils.manager import JobManager


def long_running_job(duration: float) -> float:
    duration = float(min(duration, 5.0))
    time.sleep(duration)
    return duration


@pytest.mark.asyncio
async def test_job_summary_generation(tmpdir):
    from biothings import config

    prior_run_config = config.RUN_DIR
    config.RUN_DIR = str(tmpdir)

    try:
        # construct our manager instances
        job_manager = JobManager(
            loop=asyncio.get_running_loop(),
            process_queue=None,
            thread_queue=None,
            max_memory_usage=None,
            num_workers=None,
            num_threads=None,
            auto_recycle=True,
        )

        jprocess = job_manager.hub_process
        assert isinstance(jprocess, psutil.Process)

        assert isinstance(jprocess.pid, int) and jprocess.pid > 0
        assert jprocess.name() == "python3"
        assert jprocess.status() == "running"

        process_callback = functools.partial(long_running_job, 5.0)
        pinfo = {}
        result = await job_manager.defer_to_process(pinfo, process_callback)
        assert isinstance(result, asyncio.Task)
        assert not result.done()
        prior_pending_processes = job_manager.get_pending_processes()
        prior_job_summary = job_manager.get_summary()

        assert not prior_pending_processes
        assert prior_job_summary.get("process", None)
        assert prior_job_summary.get("thread", None)
        assert prior_job_summary.get("memory", None)
        assert prior_job_summary.get("available_system_memory", None)
        assert prior_job_summary.get("max_memory_usage", False) is None
        assert prior_job_summary.get("hub_pid", None)

        assert len(prior_job_summary["process"]["running"]) == 0
        assert len(prior_job_summary["process"]["pending"]) == 0
        assert len(prior_job_summary["thread"]["running"]) == 0
        assert len(prior_job_summary["thread"]["pending"]) == 0

        executed_jobs = await asyncio.gather(result)
        await asyncio.sleep(1)

        ongoing_pending_processes = job_manager.get_pending_processes()
        assert len(ongoing_pending_processes) == 1

        ongoing_process = ongoing_pending_processes[1]
        assert ongoing_process.fn.__name__ == "job_pool_operation"
        assert not ongoing_process.future.done()

        await asyncio.sleep(5)

        completed_pending_processes = job_manager.get_pending_processes()
        completed_job_summary = job_manager.get_summary()

        assert not completed_pending_processes
        assert completed_job_summary.get("process", None)
        assert completed_job_summary.get("thread", None)
        assert completed_job_summary.get("memory", None)
        assert completed_job_summary.get("available_system_memory", None)
        assert completed_job_summary.get("max_memory_usage", False) is None
        assert completed_job_summary.get("hub_pid", None)

        assert len(completed_job_summary["process"]["running"]) == 0
        assert len(completed_job_summary["process"]["pending"]) == 0
        assert len(completed_job_summary["thread"]["running"]) == 0
        assert len(completed_job_summary["thread"]["pending"]) == 0

        assert len(executed_jobs) == 1
        assert isinstance(executed_jobs[0], concurrent.futures.Future)
        assert executed_jobs[0].done()
        assert executed_jobs[0].result() == 5.0

        job_manager.stop()

    except Exception as gen_exc:
        raise gen_exc
    finally:
        config.RUN_DIR = prior_run_config
