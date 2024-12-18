"""
Basic interface to represent the regular JobManager
for our biothings-cli module
"""

from biothings.utils.common import get_loop


class CLIJobManager:
    """
    This is the minimal JobManager used in CLI mode to run async jobs, with the compatible methods as JobManager.
    It won't use a dedicated ProcessPool or ThreadPool, and will just run async job directly in the asyncio loop
    (which runs jobs in threads by default).
    """

    def __init__(self, loop=None):
        if loop is None:
            loop = get_loop()
        self.loop = loop

    async def defer_to_process(self, pinfo=None, func=None, *args, **kwargs):
        """
        Keep the same signature as JobManager.defer_to_process.

        The passed pinfo is ignored.

        defer_to_process will still run func in the thread using defer_to_thread method.
        """
        fut = await self.defer_to_thread(pinfo, func, *args, **kwargs)
        return fut

    async def defer_to_thread(self, pinfo=None, func=None, *args):
        """
        Keep the same signature as JobManager.defer_to_thread.

        The passed pinfo is ignored
        """

        async def run(fut, func):
            res = func()
            fut.set_result(res)

        fut = self.loop.create_future()
        self.loop.create_task(run(fut, func))
        return fut
