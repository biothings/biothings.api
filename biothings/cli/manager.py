"""
Basic interface to represent the regular JobManager for our biothings-cli module
"""

import asyncio
from typing import Any, Callable


class CLIJobManager:
    """
    This is the minimal JobManager used in CLI mode to run async jobs, with the compatible methods as JobManager.
    It won't use a dedicated ProcessPool or ThreadPool, and will just run async job directly in the asyncio loop
    (which runs jobs in threads by default).
    """

    def __init__(self, loop: asyncio.events.AbstractEventLoop = None):
        self.loop = self._initialize_event_loop(loop)

    def _initialize_event_loop(self, loop: asyncio.events.AbstractEventLoop = None):
        """
        Handles the initialization of the event loop
        """
        try:
            if loop is None:
                loop = asyncio.get_running_loop()
            return loop
        except RuntimeError as runtime_error:
            raise runtime_error

    async def defer_to_process(self, pinfo: dict = None, func: Callable = None, *args, **kwargs) -> asyncio.Future:
        """
        Keep the same signature as JobManager.defer_to_process.

        The passed pinfo is ignored.

        defer_to_process will still run func in the thread using defer_to_thread method.
        """
        return await self.defer_to_thread(pinfo, func, *args, **kwargs)

    async def defer_to_thread(self, pinfo: dict = None, func: Callable = None, *args, **kwargs) -> asyncio.Future:
        """
        Keep the same signature as JobManager.defer_to_thread.

        The passed pinfo is ignored
        """
        future = self.loop.create_future()

        self.loop.create_task(CLIJobManager._execute_future(future, func, *args, **kwargs))
        return future

    @staticmethod
    async def _execute_future(future, func, *args, **kwargs):
        res = func(*args, **kwargs)
        future.set_result(res)
