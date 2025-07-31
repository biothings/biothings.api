"""
Temporary stopgap to leverage this minimal instance
due to our limitation in the multiprocessing starting
method

Once we support setting new processes to spawn instead of fork,
we can eliminate this manager
"""

from biothings.utils.common import get_loop


class CLIJobManager:
    """This is the minimal JobManager used in CLI mode to run async jobs, with the compatible methods as JobManager.
    It won't use a dedicated ProcessPool or ThreadPool, and will just run async job directly in the asyncio loop
    (which runs jobs in threads by default).
    """

    def __init__(self, loop=None):
        self.loop = loop or get_loop()

    async def defer_to_process(self, pinfo=None, func=None, *args, **kwargs):
        """keep the same signature as JobManager.defer_to_process. The passed pinfo is ignored.
        defer_to_process will still run func in the thread using defer_to_thread method.
        """
        fut = await self.defer_to_thread(pinfo, func, *args, **kwargs)
        return fut

    async def defer_to_thread(self, pinfo=None, func=None, *args):
        """keep the same signature as JobManager.defer_to_thread. The passed pinfo is ignored"""

        async def run(fut, func):
            try:
                res = func()
                fut.set_result(res)
            except Exception as gen_exc:
                fut.set_exception(gen_exc)

        fut = self.loop.create_future()
        self.loop.create_task(run(fut, func))
        return fut
