from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from biothings.utils.manager import JobManager
from biothings.hub import get_loop


class TestJobManager():
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
            loop, process_queue=process_executor, thread_queue=thread_executor
        )

        # Asserts
        assert manager.loop._default_executor == thread_executor
        assert manager.loop._default_executor != process_executor
