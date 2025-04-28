from concurrent.futures import ThreadPoolExecutor

from biothings.utils.common import get_loop_with_max_workers


def test_get_loop():
    # Given
    max_workers = 2

    # Action
    loop = get_loop_with_max_workers(max_workers=max_workers)

    # Asserts
    assert isinstance(loop._default_executor, ThreadPoolExecutor)
    assert loop._default_executor._max_workers == max_workers
