import random

from biothings.hub.dataindex.indexer_schedule import Schedule, SchedulerMismatchError

import pytest


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

    cutoff_point = random.randint(2, 8)
    batch_count = 1
    for batch in schedule:
        assert batch_count == batch
        batch_count += 1

        if batch_count == cutoff_point:
            break

        suffix_value = "Task"
        suffix_repr = f"{suffix_value} #{schedule._batch}/{schedule._batches} {schedule._percentage}"
        assert suffix_repr == schedule.suffix(suffix_value)

    with pytest.raises(SchedulerMismatchError):
        schedule.completed()
