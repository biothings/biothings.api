"""
Tests for the DumperManager
"""

import asyncio
import logging

import pytest

from biothings.hub.dataload.dumper import BaseDumper, DumperManager
from biothings.utils.manager import JobManager


class GatedDumper(BaseDumper):
    # dump() waits for the test to open the gate, then returns or raises outcome
    SRC_NAME = "test_gated_source"
    gate = None
    outcome = None

    async def dump(self, *args, **kwargs):
        await self.gate.wait()
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


class OtherGatedDumper(GatedDumper):
    SRC_NAME = "test_other_gated_source"


@pytest.fixture(autouse=True)
def gated_dumper():
    GatedDumper.gate = asyncio.Event()
    GatedDumper.outcome = "dumped"


def make_dumper_manager():
    # must be called from within a running loop (async test)
    return DumperManager(JobManager(asyncio.get_running_loop(), num_workers=1))


def shutdown(dumper_manager):
    dumper_manager.job_manager.worker_queue.shutdown(wait=False)
    dumper_manager.job_manager.thread_queue.shutdown(wait=False)


@pytest.mark.asyncio
async def test_dump_skipped_while_same_source_is_dumping(caplog):
    manager = make_dumper_manager()
    try:
        running = asyncio.create_task(manager.create_and_dump(GatedDumper))
        await asyncio.sleep(0.1)
        with caplog.at_level(logging.WARNING):
            # without the guard, this second dump would wait for the gate too
            skipped = await asyncio.wait_for(manager.create_and_dump(GatedDumper), timeout=10)
        assert skipped.startswith("Skipping dump, another dump of 'test_gated_source' has been running for")
        warnings = [record for record in caplog.records if "Skipping dump" in record.getMessage()]
        assert len(warnings) == 1
        assert warnings[0].notify is True
        assert not running.done()

        GatedDumper.gate.set()
        assert await running == "dumped"
        # once the running dump is done, the source can be dumped again
        assert await manager.create_and_dump(GatedDumper) == "dumped"
    finally:
        shutdown(manager)


@pytest.mark.asyncio
async def test_failed_or_cancelled_dump_lets_the_source_be_dumped_again():
    manager = make_dumper_manager()
    try:
        cancelled = asyncio.create_task(manager.create_and_dump(GatedDumper))
        await asyncio.sleep(0.1)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled

        GatedDumper.outcome = RuntimeError("download failed")
        GatedDumper.gate.set()
        with pytest.raises(RuntimeError):
            await manager.create_and_dump(GatedDumper)

        GatedDumper.outcome = "dumped"
        assert await manager.create_and_dump(GatedDumper) == "dumped"
    finally:
        shutdown(manager)


@pytest.mark.asyncio
async def test_different_sources_dump_at_the_same_time():
    manager = make_dumper_manager()
    try:
        first = asyncio.create_task(manager.create_and_dump(GatedDumper))
        other = asyncio.create_task(manager.create_and_dump(OtherGatedDumper))
        await asyncio.sleep(0.1)
        # both are running, neither was skipped
        assert not first.done()
        assert not other.done()

        GatedDumper.gate.set()
        assert await asyncio.gather(first, other) == ["dumped", "dumped"]
    finally:
        shutdown(manager)
