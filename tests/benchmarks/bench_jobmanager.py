"""Benchmark JobManager CPU-bound workers: fork process pool vs free-threaded thread pool.

Runs merge_struct-heavy batches (the hub's merge pipeline hot loop) through
JobManager.defer_to_process and reports wall time and peak RSS.

Three configurations to compare, all runnable on a single 3.14t build
(PYTHON_GIL=1 re-enables the GIL to emulate a regular build):

  (a) baseline, fork process pool, GIL on:
      PYTHON_GIL=1 python tests/benchmarks/bench_jobmanager.py
  (b) free-threaded thread pool, GIL off (requires a 3.14t build):
      BENCH_FT_WORKERS=1 python tests/benchmarks/bench_jobmanager.py
  (c) control, thread pool with the GIL on (should be ~serial):
      PYTHON_GIL=1 BENCH_FT_WORKERS=1 python tests/benchmarks/bench_jobmanager.py

Acceptance (from the asyncio-modernization plan): (b) within ~1.3x of (a)
wall time, with materially lower RSS; (c) shows little to no speedup over a
single worker, proving the win comes from disabling the GIL.
"""

import argparse
import asyncio
import logging
import os
import sys
import tempfile
import time
from functools import partial


def setup_config(workers):
    # minimal hub configuration so biothings.utils.manager can be imported
    # and JobManager instantiated outside a real hub
    from biothings.utils.common import DummyConfig

    config = DummyConfig("config")
    config.RUN_DIR = tempfile.mkdtemp(prefix="bench_jobmanager_")
    config.HUB_MAX_WORKERS = workers
    config.MAX_QUEUED_JOBS = 10000
    config.HUB_FREE_THREADED_WORKERS = bool(os.environ.get("BENCH_FT_WORKERS"))
    config.DATA_SRC_SERVER = "localhost"
    config.DATA_SRC_PORT = 27017
    config.DATA_SRC_DATABASE = "bench_src"
    config.DATA_SRC_SERVER_USERNAME = ""
    config.DATA_SRC_SERVER_PASSWORD = ""
    config.DATA_TARGET_SERVER = "localhost"
    config.DATA_TARGET_PORT = 27017
    config.DATA_TARGET_DATABASE = "bench_target"
    config.DATA_TARGET_SERVER_USERNAME = ""
    config.DATA_TARGET_SERVER_PASSWORD = ""
    config.DATA_HUB_DB_DATABASE = "bench_hubdb"
    config.HUB_DB_BACKEND = {"module": "biothings.utils.sqlite3", "sqlite_db_folder": config.RUN_DIR}
    config.DATA_PLUGIN_FOLDER = os.path.join(config.RUN_DIR, "plugins")
    config.DATA_ARCHIVE_ROOT = os.path.join(config.RUN_DIR, "archive")
    config.LOG_FOLDER = os.path.join(config.RUN_DIR, "logs")
    config.ACTIVE_DATASOURCES = []
    config.HUB_ENV = ""
    config.logger = logging.getLogger("bench")
    sys.modules["config"] = config
    sys.modules["biothings.config"] = config
    return config


def make_doc(key, width):
    return {
        "_id": "doc%s" % key,
        "source_%s" % key: {
            "values": list(range(width)),
            "nested": {"tags": ["tag%d" % i for i in range(width)], "props": {"k%d" % i: i for i in range(width)}},
        },
        "shared": {"xrefs": ["x%s_%d" % (key, i) for i in range(width)]},
    }


def merge_batch(num_docs, width, batch_num):
    # mimics merger_worker's inner loop: repeated recursive dict merging
    from biothings.utils.dataload import merge_struct

    merged = {}
    for i in range(num_docs):
        merged = merge_struct(merged, make_doc(i % 25, width))
    return num_docs


def peak_rss_sampler(stop, result):
    import psutil

    proc = psutil.Process()
    peak = 0
    while not stop.is_set():
        try:
            rss = proc.memory_info().rss + sum(c.memory_info().rss for c in proc.children(recursive=True))
            peak = max(peak, rss)
        except psutil.NoSuchProcess:
            pass
        stop.wait(0.1)
    result["peak_rss"] = peak


async def run_bench(jm, batches, num_docs, width):
    pinfo = {"category": "bench", "source": "bench", "step": "merge", "description": ""}
    done = 0

    async def watch(job):
        nonlocal done
        # note: "done += await job" would read `done` *before* suspending at
        # the await, losing concurrent updates
        res = await job
        done += res

    t0 = time.time()
    async with asyncio.TaskGroup() as tg:
        for b in range(batches):
            job = await jm.defer_to_process(dict(pinfo), partial(merge_batch, num_docs, width, b))
            tg.create_task(watch(job))
    elapsed = time.time() - t0
    assert done == batches * num_docs, "done=%s expected=%s" % (done, batches * num_docs)
    return elapsed


async def main(args):
    import concurrent.futures

    from biothings.utils.manager import JobManager

    loop = asyncio.get_running_loop()
    process_queue = None
    gil_on = not hasattr(sys, "_is_gil_enabled") or sys._is_gil_enabled()
    if os.environ.get("BENCH_FT_WORKERS") and gil_on:
        # control config (c): force a thread pool although the GIL is on;
        # JobManager itself refuses this combination by design, so inject it
        process_queue = concurrent.futures.ThreadPoolExecutor(
            max_workers=args.workers, thread_name_prefix="FTWorker"
        )
    jm = JobManager(
        loop, process_queue=process_queue, num_workers=args.workers, num_threads=args.workers, auto_recycle=False
    )

    # warmup (pool spinup, imports in workers)
    await run_bench(jm, args.workers, 50, 10)

    import threading

    stop, mem = threading.Event(), {}
    sampler = threading.Thread(target=peak_rss_sampler, args=(stop, mem), daemon=True)
    sampler.start()
    elapsed = await run_bench(jm, args.batches, args.docs, args.width)
    stop.set()
    sampler.join()

    gil = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    print("\n=== bench_jobmanager results ===")
    print("python          : %s" % sys.version.split()[0])
    print("gil enabled     : %s" % gil)
    print("executor        : %s" % type(jm.process_queue).__name__)
    print("workers         : %d" % args.workers)
    print("batches x docs  : %d x %d (width %d)" % (args.batches, args.docs, args.width))
    print("wall time       : %.2fs" % elapsed)
    print("peak rss        : %.1f MB" % (mem.get("peak_rss", 0) / 1024 / 1024))
    jm.process_queue.shutdown()
    jm.thread_queue.shutdown(wait=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batches", type=int, default=16)
    parser.add_argument("--docs", type=int, default=2000, help="documents merged per batch")
    parser.add_argument("--width", type=int, default=20, help="lists/dicts width per document")
    args = parser.parse_args()
    setup_config(args.workers)
    asyncio.run(main(args))
