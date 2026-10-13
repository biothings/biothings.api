"""
Temporary stopgap to leverage this minimal instance
due to our limitation in the multiprocessing starting
method

Once we support setting new processes to spawn instead of fork,
we can eliminate this manager
"""

import warnings

from biothings.utils.common import get_loop


class CLIJobManager:
    """This is the minimal JobManager used in CLI mode to run async jobs, with the compatible methods as JobManager.
    It won't use a worker or thread pool: whichever method defers a job, the job runs inline on the asyncio loop's
    thread, one job at a time.
    """

    def __init__(self, loop=None):
        self.loop = loop or get_loop()

    async def defer_to_worker(self, pinfo=None, func=None, *args, **kwargs):
        """keep the same signature as JobManager.defer_to_worker. The passed pinfo is ignored.
        Unlike in JobManager, func runs inline on the event loop, like with defer_to_thread.
        """
        fut = await self.defer_to_thread(pinfo, func, *args, **kwargs)
        return fut

    async def defer_to_process(self, pinfo=None, func=None, *args, **kwargs):
        """Deprecated alias of defer_to_worker()."""
        warnings.warn(
            "CLIJobManager.defer_to_process() is deprecated and will be removed in a future major release, "
            "use CLIJobManager.defer_to_worker() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.defer_to_worker(pinfo, func, *args, **kwargs)

    async def defer_to_thread(self, pinfo=None, func=None, *args, **kwargs):
        """keep the same signature as JobManager.defer_to_thread. The passed pinfo is ignored.
        Unlike in JobManager, func doesn't run in a thread: it runs inline on the event loop, blocking it until
        func returns.
        """

        async def run(fut, func, *args, **kwargs):
            try:
                res = func(*args, **kwargs)
                fut.set_result(res)
            except Exception as gen_exc:
                fut.set_exception(gen_exc)

        fut = self.loop.create_future()
        self.loop.create_task(run(fut, func, *args, **kwargs))
        return fut
