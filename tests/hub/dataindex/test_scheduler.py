from biothings.hub.dataindex.indexer_schedule import Schedule

import pytest


@pytest.mark.parametrize("total, batch_size", [(100, 10), (25, 10), (0, 10), (1, 10)])
def test_schedule_iteration(total: int, batch_size: int):
    """
    Generates a schedule from the provided inputs and then verifies the
    expected structure of the Schedule object
    """
    schedule = Schedule(total, batch_size)
    # schedule.__next__()
    # self._state = self.suffix("running, on batch") + ","

    batch_count = 1
    for batch in schedule:
        assert batch_count == batch
        batch_count += 1
        print(schedule)
        print(schedule.suffix("Task"))



