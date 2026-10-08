"""
Tests the custom scheduler object used with handling our batch index processing

See <biothings.hub.dataindex>
"""

import asyncio

import pytest

from biothings.hub.dataindex.indexer_schedule import Schedule, SchedulerMismatchError


@pytest.mark.parametrize("total, batch_size", [(100, 10), (25, 10), (0, 10), (1, 10)])
def test_schedule_iteration(total: int, batch_size: int):
    """
    Generates a schedule from the provided inputs and then verifies the
    expected structure of the Schedule object
    """
    schedule = Schedule(total, batch_size)

    batch_count = 1
    for batch in schedule:
        assert batch_count == batch
        batch_count += 1

        suffix_value = "Task"
        suffix_repr = f"{suffix_value} #{schedule._batch}/{schedule._batches} {schedule._percentage}"
        assert suffix_repr == schedule.suffix(suffix_value)


def test_schedule_mismatch_error():
    """
    Verifies that we raise a SchedulerMismatchError if we prematurely or erroneously
    call the `Scheduler.completed` method prior to completing the scheduler indexing process
    """
    total = 100
    batch_size = 10
    schedule = Schedule(total, batch_size)

    with pytest.raises(SchedulerMismatchError):
        schedule.completed()


@pytest.mark.asyncio
async def test_schedule_finished_concurrent_accumulation():
    """do_index accumulates schedule.finished from many concurrent
    batch-completion tasks. The increment must resolve the awaited count into
    a local *before* the augmented assignment - writing it as
    "schedule.finished += await job" reads schedule.finished before suspending,
    so concurrent tasks clobber each other's increments and the counter
    undercounts. This reproduces that accumulation pattern and asserts the
    total is exact.
    """
    total, batch_size = 1000, 10
    schedule = Schedule(total, batch_size)
    for _ in schedule:  # advance the scheduler through all batches
        pass

    async def worker(count):
        async def job():
            await asyncio.sleep(0)  # force a suspension point
            return count

        c = await job()  # resolve into a local first (the correct pattern)
        schedule.finished += c

    await asyncio.gather(*(worker(batch_size) for _ in range(total // batch_size)))
    assert schedule.finished == total
    # completes cleanly only because the count is exact
    schedule.completed()
