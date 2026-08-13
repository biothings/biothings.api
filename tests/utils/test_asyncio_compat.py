import asyncio

import pytest

from biothings.utils.asyncio_compat import ExceptionGroup, TaskGroup
from biothings.utils.common import first_exception


@pytest.mark.asyncio
async def test_taskgroup_cancels_siblings_and_groups_failures():
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def sibling():
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    async def fail():
        await started.wait()
        raise ValueError("worker failure")

    with pytest.raises(ExceptionGroup) as raised:
        async with TaskGroup() as group:
            group.create_task(sibling())
            group.create_task(fail())

    assert isinstance(first_exception(raised.value), ValueError)
    assert cancelled.is_set()
