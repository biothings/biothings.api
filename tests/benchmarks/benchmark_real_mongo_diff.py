"""Compare process and free-threaded JSON-diff workers on real MongoDB data.

The controller snapshots one deterministic workload, then launches fresh
runner processes against that snapshot. Both modes use the same free-threaded
Python executable: ``PYTHON_GIL=1`` selects the regular process-pool path and
``PYTHON_GIL=0`` selects the no-GIL thread-pool path.

MongoDB access is read-only, but server-side enforcement still requires a
read-only credential.
"""

import argparse
import asyncio
import gc
import hashlib
import json
import logging
import os
import pickle
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from functools import partial
from pathlib import Path

import validate_real_mongo_diff as validation

RESULT_PREFIX = "REAL_MONGO_DIFF_BENCHMARK="


def dump_pickle(path, value):
    with open(path, "wb") as handle:
        pickle.dump(value, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickle(path):
    with open(path, "rb") as handle:
        return pickle.load(handle)


def hash_ids(*id_groups):
    digest = hashlib.sha256()
    for index, group in enumerate(id_groups):
        digest.update(("group:%d:length:%d" % (index, len(group))).encode("ascii"))
        digest.update(b"\0")
        for value in group:
            digest.update(type(value).__name__.encode("utf-8"))
            digest.update(b"\0")
            digest.update(repr(value).encode("utf-8"))
            digest.update(b"\0")
    return digest.hexdigest()


def hash_documents(documents, ordered_ids):
    digest = hashlib.sha256()
    for document_id in ordered_ids:
        digest.update(pickle.dumps(documents[document_id], protocol=pickle.HIGHEST_PROTOCOL))
    return digest.hexdigest()


def worker_identity(mode, result):
    if mode == "process":
        return str(result["pid"])
    return result["thread"]


def warm_executor_slot(old_spec, new_spec, hold_seconds):
    """Import the production path and establish its cached Mongo connection."""
    from biothings.hub.databuild.backend import create_backend
    from biothings.hub.databuild.differ import diff_worker_new_vs_old, diff_worker_old_vs_new  # noqa: F401
    from biothings.utils.diff import diff_docs_jsonpatch  # noqa: F401

    for spec in (old_spec, new_spec):
        backend = create_backend(spec, follow_ref=True)
        backend.target_collection.database.command("ping")
    gc.collect()
    time.sleep(hold_seconds)
    return {
        "gil_enabled": sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True,
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
    }


def benchmark_new_worker(id_batch, old_spec, new_spec, batch_num, diff_folder):
    from biothings.hub.databuild.differ import diff_worker_new_vs_old
    from biothings.utils.diff import diff_docs_jsonpatch

    started = time.perf_counter()
    summary = diff_worker_new_vs_old(
        id_batch,
        old_spec,
        new_spec,
        batch_num,
        diff_folder,
        diff_docs_jsonpatch,
        exclude=["_timestamp"],
        selfcontained=True,
    )
    return {
        "finished": time.perf_counter(),
        "pid": os.getpid(),
        "started": started,
        "summary": summary,
        "thread": threading.current_thread().name,
    }


def benchmark_old_worker(id_batch, new_spec, batch_num, diff_folder):
    from biothings.hub.databuild.differ import diff_worker_old_vs_new

    started = time.perf_counter()
    summary = diff_worker_old_vs_new(id_batch, new_spec, batch_num, diff_folder)
    return {
        "finished": time.perf_counter(),
        "pid": os.getpid(),
        "started": started,
        "summary": summary,
        "thread": threading.current_thread().name,
    }


def process_tree_snapshot():
    import psutil

    root = psutil.Process()
    processes = [root]
    try:
        processes.extend(root.children(recursive=True))
    except psutil.Error:
        pass

    snapshot = {
        "child_rss_bytes": 0,
        "parent_rss_bytes": 0,
        "process_count": 0,
        "rss_bytes": 0,
    }
    for process in processes:
        try:
            rss = process.memory_info().rss
        except psutil.Error:
            continue
        snapshot["process_count"] += 1
        snapshot["rss_bytes"] += rss
        if process.pid == root.pid:
            snapshot["parent_rss_bytes"] += rss
        else:
            snapshot["child_rss_bytes"] += rss
    return snapshot


def process_tree_cpu_seconds():
    import psutil

    root = psutil.Process()
    processes = [root]
    try:
        processes.extend(root.children(recursive=True))
    except psutil.Error:
        pass
    total = 0.0
    for process in processes:
        try:
            cpu = process.cpu_times()
        except psutil.Error:
            continue
        total += cpu.user + cpu.system
    return total


def sample_memory(stop, peaks, interval):
    while True:
        snapshot = process_tree_snapshot()
        for key, value in snapshot.items():
            peaks[key] = max(peaks.get(key, 0), value)
        if stop.wait(interval):
            break
    snapshot = process_tree_snapshot()
    for key, value in snapshot.items():
        peaks[key] = max(peaks.get(key, 0), value)


def maximum_overlap(results):
    events = []
    for result in results:
        events.append((result["started"], 1))
        events.append((result["finished"], -1))
    active = 0
    maximum = 0
    for _timestamp, delta in sorted(events, key=lambda event: (event[0], event[1])):
        active += delta
        maximum = max(maximum, active)
    return maximum


async def submit_warmup(manager, mode, old_spec, new_spec, workers):
    pinfo = {
        "category": "benchmark",
        "source": new_spec[2],
        "step": "warm-real-mongo-jsondiff",
        "description": "",
    }
    started = time.perf_counter()
    results = []
    identities = set()
    for attempt in range(3):
        jobs = [
            await manager.defer_to_process(
                dict(pinfo, description="warm attempt %d slot %d" % (attempt + 1, index + 1)),
                partial(warm_executor_slot, old_spec, new_spec, 0.75),
            )
            for index in range(workers * 2)
        ]
        attempt_results = await asyncio.gather(*jobs)
        results.extend(attempt_results)
        identities.update(worker_identity(mode, result) for result in attempt_results)
        if len(identities) == workers:
            break
    if len(identities) != workers:
        raise RuntimeError("only %d of %d executor slots warmed: %s" % (len(identities), workers, sorted(identities)))
    return results, time.perf_counter() - started


async def execute_measured_workers(
    manager,
    old_spec,
    new_spec,
    selected_new,
    selected_old,
    batch_size,
    diff_folder,
):
    new_batches = list(validation.chunks(selected_new, batch_size))
    old_batches = list(validation.chunks(selected_old, batch_size))
    pinfo = {
        "category": "benchmark",
        "source": new_spec[2],
        "step": "real-mongo-jsondiff",
        "description": "",
    }

    total_started = time.perf_counter()
    new_started = time.perf_counter()
    new_jobs = [
        await manager.defer_to_process(
            dict(pinfo, description="new-vs-old batch %d" % batch_num),
            partial(benchmark_new_worker, id_batch, old_spec, new_spec, batch_num, diff_folder),
        )
        for batch_num, id_batch in enumerate(new_batches, 1)
    ]
    new_results = await asyncio.gather(*new_jobs)
    new_elapsed = time.perf_counter() - new_started

    old_started = time.perf_counter()
    first_old_batch = len(new_batches) + 1
    old_jobs = [
        await manager.defer_to_process(
            dict(pinfo, description="old-vs-new batch %d" % (first_old_batch + offset)),
            partial(
                benchmark_old_worker,
                id_batch,
                new_spec,
                first_old_batch + offset,
                diff_folder,
            ),
        )
        for offset, id_batch in enumerate(old_batches)
    ]
    old_results = await asyncio.gather(*old_jobs)
    old_elapsed = time.perf_counter() - old_started
    total_elapsed = time.perf_counter() - total_started

    if manager.jobs or manager._process_job_ids or manager._pending_jobs_count():
        raise AssertionError("JobManager registries were not clean after the measured run")
    return (
        new_results,
        old_results,
        {
            "new_seconds": new_elapsed,
            "old_seconds": old_elapsed,
            "total_seconds": total_elapsed,
        },
    )


def shutdown_manager(manager):
    manager.process_queue.shutdown()
    manager.thread_queue.shutdown(wait=False)


def release_hub_sqlite_state(config, manager_module):
    """Drop temporary Hub-DB objects before ProcessPoolExecutor forks."""
    root_logger = logging.getLogger()
    event_handlers = [handler for handler in root_logger.handlers if getattr(handler, "name", None) == "event_recorder"]
    for handler in event_handlers:
        root_logger.removeHandler(handler)
        if hasattr(handler, "eventcol"):
            handler.eventcol = None
        handler.close()
    config._db = None
    manager_module.config._db = None
    del event_handlers
    gc.collect()


async def run_single(args):
    metadata = json.loads(Path(args.metadata_file).read_text())
    clean_uri = validation.configure_proxy_uri(
        metadata["uri"],
        metadata.get("proxy_host"),
        metadata.get("proxy_port"),
        metadata["timeout_ms"],
    )
    old_spec = (clean_uri, metadata["database"], metadata["archive"])
    new_spec = (clean_uri, metadata["database"], metadata["current"])

    with tempfile.TemporaryDirectory(prefix="real_mongo_diff_benchmark_") as work_dir:
        config = validation.install_config(metadata["workers"], work_dir)
        config.HUB_FREE_THREADED_WORKERS = args.runner_mode == "free-threaded"

        import concurrent.futures

        import biothings.utils.manager as manager_module
        from biothings.utils import jsondiff as jsondiff_module
        from biothings.utils.manager import JobManager

        release_hub_sqlite_state(config, manager_module)
        jsondiff_module.UNORDERED_LIST = True
        jsondiff_module.USE_LIST_OPS = False
        gil_enabled = not hasattr(sys, "_is_gil_enabled") or sys._is_gil_enabled()
        if args.runner_mode == "process" and not gil_enabled:
            raise RuntimeError("process baseline requires PYTHON_GIL=1")
        if args.runner_mode == "free-threaded" and gil_enabled:
            raise RuntimeError("free-threaded mode requires PYTHON_GIL=0")

        manager = JobManager(
            asyncio.get_running_loop(),
            num_workers=metadata["workers"],
            num_threads=metadata["workers"],
            auto_recycle=False,
        )
        expected_executor = (
            concurrent.futures.ProcessPoolExecutor
            if args.runner_mode == "process"
            else concurrent.futures.ThreadPoolExecutor
        )
        if not isinstance(manager.process_queue, expected_executor):
            raise RuntimeError(
                "expected %s, got %s" % (expected_executor.__name__, type(manager.process_queue).__name__)
            )
        if args.runner_mode == "process" and manager.process_queue._mp_context.get_start_method() != "fork":
            raise RuntimeError("process benchmark requires JobManager's fork context")

        try:
            warm_results, warmup_seconds = await submit_warmup(
                manager,
                args.runner_mode,
                old_spec,
                new_spec,
                metadata["workers"],
            )
            if any(result["gil_enabled"] != gil_enabled for result in warm_results):
                raise RuntimeError("worker GIL state did not match the runner")
            if manager.jobs or manager._process_job_ids or manager._pending_jobs_count():
                raise AssertionError("JobManager registries were not clean after warmup")
            workload = load_pickle(args.ids_file)
            if hash_ids(workload["common"], workload["add"], workload["delete"]) != metadata["id_sha256"]:
                raise AssertionError("workload ID hash did not match metadata")
            selected_new = workload["common"] + workload["add"]
            selected_old = workload["common"] + workload["delete"]

            gc.collect()
            baseline = process_tree_snapshot() if args.measure_memory else None
            peaks = dict(baseline) if baseline is not None else None
            expected_process_count = metadata["workers"] + 1 if args.runner_mode == "process" else 1
            if baseline is not None and baseline["process_count"] != expected_process_count:
                raise RuntimeError(
                    "memory snapshot saw %d processes; expected %d"
                    % (baseline["process_count"], expected_process_count)
                )
            stop = None
            sampler = None
            if args.measure_memory:
                stop = threading.Event()
                sampler = threading.Thread(
                    target=sample_memory,
                    args=(stop, peaks, metadata["memory_sample_seconds"]),
                    daemon=True,
                    name="memory-sampler",
                )
                sampler.start()
            cpu_before = process_tree_cpu_seconds()

            diff_folder = os.path.join(work_dir, "diff")
            os.makedirs(diff_folder)
            try:
                try:
                    new_results, old_results, timings = await execute_measured_workers(
                        manager,
                        old_spec,
                        new_spec,
                        selected_new,
                        selected_old,
                        metadata["batch_size"],
                        diff_folder,
                    )
                except BaseException as error:
                    processes = getattr(manager.process_queue, "_processes", {}) or {}
                    process_states = {pid: process.exitcode for pid, process in processes.items()}
                    raise RuntimeError(
                        "measured worker phase failed; process exit codes: %s" % process_states
                    ) from error
            finally:
                if sampler is not None:
                    stop.set()
                    sampler.join()
            cpu_seconds = max(0.0, process_tree_cpu_seconds() - cpu_before)

            worker_results = new_results + old_results
            payloads = validation.load_diff_payloads(diff_folder, worker_results)
            diff_bytes = sum(result["summary"].get("diff_file", {}).get("size", 0) for result in worker_results)
            outer_identities = sorted({worker_identity(args.runner_mode, result) for result in worker_results})
            new_identities = {worker_identity(args.runner_mode, result) for result in new_results}
            outer_overlap = maximum_overlap(new_results)
            new_batch_count = len(list(validation.chunks(selected_new, metadata["batch_size"])))
            expected_parallelism = min(metadata["workers"], new_batch_count)
            if len(new_identities) != expected_parallelism or outer_overlap != expected_parallelism:
                raise AssertionError(
                    "expected %d-way new-worker use/overlap, got %d identities and %d-way overlap"
                    % (expected_parallelism, len(new_identities), outer_overlap)
                )
        finally:
            shutdown_manager(manager)

        reference = load_pickle(args.reference_file)
        if hash_documents(reference["old_documents"], selected_old) != metadata["old_documents_sha256"]:
            raise AssertionError("archive reference-document hash did not match metadata")
        if hash_documents(reference["new_documents"], selected_new) != metadata["new_documents_sha256"]:
            raise AssertionError("current reference-document hash did not match metadata")
        reconstruction = validation.validate_reconstruction(
            payloads,
            reference["old_documents"],
            reference["new_documents"],
            workload["add"],
            workload["delete"],
        )
        if reconstruction["exact_failures"] or reconstruction["semantic_failures"]:
            raise AssertionError("benchmark reconstruction failed")
        if reconstruction["updates"] == 0:
            raise AssertionError("benchmark generated no JSON patches")

        input_visits = len(selected_new) + len(selected_old)
        common_documents = len(workload["common"])
        return {
            "batch_size": metadata["batch_size"],
            "cpu_seconds": cpu_seconds,
            "diff_bytes": diff_bytes,
            "diff_files": len(payloads),
            "executor": type(manager.process_queue).__name__,
            "gil_enabled": gil_enabled,
            "input_id_visits": input_visits,
            "label": args.label,
            "measurement_kind": "memory" if args.measure_memory else "timing",
            "memory": (
                {
                    "baseline": baseline,
                    "peak": peaks,
                    "peak_minus_baseline": {"rss_bytes": max(0, peaks["rss_bytes"] - baseline["rss_bytes"])},
                }
                if args.measure_memory
                else None
            ),
            "mode": args.runner_mode,
            "outer_worker_identities": outer_identities,
            "outer_worker_max_overlap": outer_overlap,
            "python": sys.version.split()[0],
            "reconstruction": {
                "adds": reconstruction["adds"],
                "deletes": reconstruction["deletes"],
                "documents": reconstruction["documents_reconstructed"],
                "exact_failures": len(reconstruction["exact_failures"]),
                "patch_operations": reconstruction["patch_operations"],
                "semantic_failures": len(reconstruction["semantic_failures"]),
                "updates": reconstruction["updates"],
            },
            "throughput": {
                "effective_new_phase_common_docs_per_second": common_documents / timings["new_seconds"],
                "input_id_visits_per_second": input_visits / timings["total_seconds"],
                "patch_operations_per_new_second": reconstruction["patch_operations"] / timings["new_seconds"],
            },
            "timings": timings,
            "warm_worker_identities": sorted({worker_identity(args.runner_mode, result) for result in warm_results}),
            "warmup_seconds": warmup_seconds,
            "workers": metadata["workers"],
        }


def required_controller_arg(args, name):
    value = getattr(args, name)
    if value is None:
        raise ValueError("--%s is required" % name.replace("_", "-"))
    return value


def prepare_snapshot(args, suite_dir):
    from pymongo import MongoClient

    uri = required_controller_arg(args, "uri")
    database_name = required_controller_arg(args, "database")
    current_name = required_controller_arg(args, "current")
    clean_uri = validation.configure_proxy_uri(uri, args.proxy_host, args.proxy_port, args.timeout_ms)
    client = MongoClient(clean_uri)
    try:
        database = client[database_name]
        client.admin.command("ping")
        archive_name = validation.choose_archive(database, current_name, args.archive)
        current_collection = database[current_name]
        archive_collection = database[archive_name]
        estimated_counts = {
            "archive": archive_collection.estimated_document_count(),
            "current": current_collection.estimated_document_count(),
        }
        if max(estimated_counts.values()) > args.max_id_scan and not args.allow_large_id_scan:
            raise RuntimeError(
                "refusing to materialize more than %d IDs; use --allow-large-id-scan explicitly" % args.max_id_scan
            )
        hard_limit = None if args.allow_large_id_scan else args.max_id_scan
        current_ids = validation.load_ids(current_collection, hard_limit)
        archive_ids = validation.load_ids(archive_collection, hard_limit)
        common_ids = current_ids & archive_ids
        add_ids = current_ids - archive_ids
        delete_ids = archive_ids - current_ids

        common = validation.stable_sample(common_ids, args.common_sample, "common")
        add = validation.stable_sample(add_ids, args.edge_sample, "add")
        delete = validation.stable_sample(delete_ids, args.edge_sample, "delete")
        selected_new = common + add
        selected_old = common + delete
        old_documents = validation.fetch_documents(archive_collection, selected_old)
        new_documents = validation.fetch_documents(current_collection, selected_new)
        if len(old_documents) != len(selected_old) or len(new_documents) != len(selected_new):
            raise RuntimeError("collections changed while snapshot documents were fetched")

        collection_uuids = {
            "archive": validation.collection_uuid(database, archive_name),
            "current": validation.collection_uuid(database, current_name),
        }
        ids_path = os.path.join(suite_dir, "ids.pickle")
        reference_path = os.path.join(suite_dir, "reference.pickle")
        metadata_path = os.path.join(suite_dir, "metadata.json")
        workload = {"add": add, "common": common, "delete": delete}
        reference = {
            "new_documents": new_documents,
            "old_documents": old_documents,
        }
        metadata = {
            "archive": archive_name,
            "batch_size": args.batch_size,
            "collection_counts": {
                "archive": len(archive_ids),
                "current": len(current_ids),
            },
            "collection_uuids": collection_uuids,
            "current": current_name,
            "database": database_name,
            "id_classification": {
                "add": len(add_ids),
                "common": len(common_ids),
                "delete": len(delete_ids),
            },
            "id_sha256": hash_ids(common, add, delete),
            "memory_sample_seconds": args.memory_sample_seconds,
            "new_documents_sha256": hash_documents(new_documents, selected_new),
            "old_documents_sha256": hash_documents(old_documents, selected_old),
            "proxy_host": args.proxy_host,
            "proxy_port": args.proxy_port,
            "sample": {
                "add": len(add),
                "common": len(common),
                "delete": len(delete),
            },
            "timeout_ms": args.timeout_ms,
            "uri": uri,
            "workers": args.workers,
        }
        dump_pickle(ids_path, workload)
        dump_pickle(reference_path, reference)
        Path(metadata_path).write_text(json.dumps(metadata, indent=2, sort_keys=True))
        return metadata, metadata_path, ids_path, reference_path, reference
    finally:
        client.close()


def run_fresh_runner(
    mode,
    label,
    metadata_path,
    ids_path,
    reference_path,
    *,
    measure_memory,
    timeout_seconds,
):
    environment = os.environ.copy()
    environment["PYTHON_GIL"] = "1" if mode == "process" else "0"
    environment["PYTHONFAULTHANDLER"] = "1"
    environment["PYTHONHASHSEED"] = "0"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--runner-mode",
        mode,
        "--label",
        label,
        "--metadata-file",
        metadata_path,
        "--ids-file",
        ids_path,
        "--reference-file",
        reference_path,
    ]
    if measure_memory:
        command.append("--measure-memory")
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            env=environment,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            "%s runner timed out after %.1fs\nstdout:\n%s\nstderr:\n%s"
            % (mode, timeout_seconds, error.stdout or "", error.stderr or "")
        ) from error
    if completed.returncode:
        raise RuntimeError(
            "%s runner failed (%d)\nstdout:\n%s\nstderr:\n%s"
            % (mode, completed.returncode, completed.stdout, completed.stderr)
        )
    result_lines = [
        line[len(RESULT_PREFIX) :] for line in completed.stdout.splitlines() if line.startswith(RESULT_PREFIX)
    ]
    if len(result_lines) != 1:
        raise RuntimeError("could not find one runner result in stdout:\n%s" % completed.stdout)
    result = json.loads(result_lines[0])
    result["runner_wall_seconds"] = time.perf_counter() - started
    return result


def quartiles(values):
    if len(values) < 2:
        return values[0], values[0]
    q1, _median, q3 = statistics.quantiles(values, n=4, method="inclusive")
    return q1, q3


def summarize_metric(results, path):
    values = []
    for result in results:
        value = result
        for key in path:
            value = value[key]
        values.append(value)
    q1, q3 = quartiles(values)
    return {
        "iqr": q3 - q1,
        "max": max(values),
        "median": statistics.median(values),
        "min": min(values),
        "q1": q1,
        "q3": q3,
        "values": values,
    }


def verify_snapshot(metadata, reference, args):
    from pymongo import MongoClient

    clean_uri = validation.configure_proxy_uri(
        metadata["uri"],
        metadata.get("proxy_host"),
        metadata.get("proxy_port"),
        metadata["timeout_ms"],
    )
    client = MongoClient(clean_uri)
    try:
        database = client[metadata["database"]]
        uuids = {
            "archive": validation.collection_uuid(database, metadata["archive"]),
            "current": validation.collection_uuid(database, metadata["current"]),
        }
        if uuids != metadata["collection_uuids"]:
            raise RuntimeError("collection UUID changed during benchmark")

        workload = load_pickle(args.ids_file)
        selected_new = workload["common"] + workload["add"]
        selected_old = workload["common"] + workload["delete"]
        old_final = validation.fetch_documents(database[metadata["archive"]], selected_old)
        new_final = validation.fetch_documents(database[metadata["current"]], selected_new)
        if old_final != reference["old_documents"] or new_final != reference["new_documents"]:
            raise RuntimeError("selected MongoDB documents changed during benchmark")
    finally:
        client.close()


def assert_equivalent_results(results):
    def invariant(result):
        reconstruction = result["reconstruction"]
        return (
            result["input_id_visits"],
            result["diff_files"],
            reconstruction["adds"],
            reconstruction["deletes"],
            reconstruction["documents"],
            reconstruction["updates"],
            reconstruction["patch_operations"],
        )

    expected = invariant(results[0])
    mismatches = {result["label"]: invariant(result) for result in results if invariant(result) != expected}
    if mismatches:
        raise AssertionError(
            "benchmark runs produced different normalized output; expected %s, mismatches %s" % (expected, mismatches)
        )


def run_controller(args):
    import sysconfig

    if sysconfig.get_config_var("Py_GIL_DISABLED") != 1:
        raise RuntimeError("controller must run on the Python 3.14t executable")
    if args.repetitions < 1 or args.memory_repetitions < 1:
        raise ValueError("timing and memory repetitions must both be positive")
    with tempfile.TemporaryDirectory(prefix="real_mongo_diff_suite_") as suite_dir:
        metadata, metadata_path, ids_path, reference_path, reference = prepare_snapshot(args, suite_dir)
        args.ids_file = ids_path
        warmups = []
        for mode in ("process", "free-threaded"):
            for index in range(args.warmups):
                label = "warmup-%s-%d" % (mode, index + 1)
                print("running %s" % label, file=sys.stderr, flush=True)
                warmups.append(
                    run_fresh_runner(
                        mode,
                        label,
                        metadata_path,
                        ids_path,
                        reference_path,
                        measure_memory=False,
                        timeout_seconds=args.runner_timeout,
                    )
                )

        timing_runs = []
        for pair in range(args.repetitions):
            order = ("process", "free-threaded") if pair % 2 == 0 else ("free-threaded", "process")
            for mode in order:
                label = "timing-%d-%s" % (pair + 1, mode)
                print("running %s" % label, file=sys.stderr, flush=True)
                result = run_fresh_runner(
                    mode,
                    label,
                    metadata_path,
                    ids_path,
                    reference_path,
                    measure_memory=False,
                    timeout_seconds=args.runner_timeout,
                )
                result["pair"] = pair + 1
                timing_runs.append(result)
                print(
                    "%s: %.3fs" % (label, result["timings"]["total_seconds"]),
                    file=sys.stderr,
                    flush=True,
                )

        memory_runs = []
        for pair in range(args.memory_repetitions):
            order = ("free-threaded", "process") if pair % 2 == 0 else ("process", "free-threaded")
            for mode in order:
                label = "memory-%d-%s" % (pair + 1, mode)
                print("running %s" % label, file=sys.stderr, flush=True)
                result = run_fresh_runner(
                    mode,
                    label,
                    metadata_path,
                    ids_path,
                    reference_path,
                    measure_memory=True,
                    timeout_seconds=args.runner_timeout,
                )
                result["pair"] = pair + 1
                memory_runs.append(result)
                print(
                    "%s: %.1f MiB aggregate peak RSS" % (label, result["memory"]["peak"]["rss_bytes"] / 1024 / 1024),
                    file=sys.stderr,
                    flush=True,
                )

        verify_snapshot(metadata, reference, args)
        all_results = warmups + timing_runs + memory_runs
        assert_equivalent_results(all_results)
        timing_by_mode = {
            mode: [result for result in timing_runs if result["mode"] == mode] for mode in ("process", "free-threaded")
        }
        memory_by_mode = {
            mode: [result for result in memory_runs if result["mode"] == mode] for mode in ("process", "free-threaded")
        }
        timing_summaries = {}
        memory_summaries = {}
        for mode, results in timing_by_mode.items():
            timing_summaries[mode] = {
                "input_id_visits_per_second": summarize_metric(results, ("throughput", "input_id_visits_per_second")),
                "total_seconds": summarize_metric(results, ("timings", "total_seconds")),
            }
        for mode, results in memory_by_mode.items():
            memory_summaries[mode] = {
                "peak_aggregate_rss_bytes": summarize_metric(results, ("memory", "peak", "rss_bytes")),
                "rss_delta_bytes": summarize_metric(results, ("memory", "peak_minus_baseline", "rss_bytes")),
            }

        paired_ratios = []
        for pair in range(1, args.repetitions + 1):
            process_result = next(
                result for result in timing_runs if result["pair"] == pair and result["mode"] == "process"
            )
            threaded_result = next(
                result for result in timing_runs if result["pair"] == pair and result["mode"] == "free-threaded"
            )
            paired_ratios.append(
                threaded_result["timings"]["total_seconds"] / process_result["timings"]["total_seconds"]
            )

        process_median_seconds = timing_summaries["process"]["total_seconds"]["median"]
        threaded_median_seconds = timing_summaries["free-threaded"]["total_seconds"]["median"]
        process_median_rss = memory_summaries["process"]["peak_aggregate_rss_bytes"]["median"]
        threaded_median_rss = memory_summaries["free-threaded"]["peak_aggregate_rss_bytes"]["median"]
        return {
            "comparison": {
                "free_threaded_peak_aggregate_rss_saving_fraction": 1 - threaded_median_rss / process_median_rss,
                "free_threaded_to_process_median_wall_ratio": threaded_median_seconds / process_median_seconds,
                "paired_wall_ratios": paired_ratios,
                "paired_wall_ratio_median": statistics.median(paired_ratios),
            },
            "memory_runs": memory_runs,
            "metadata": {key: value for key, value in metadata.items() if key != "uri"},
            "method": {
                "alternating_order": True,
                "fresh_process_per_run": True,
                "memory_note": (
                    "Aggregate RSS is an upper-bound-style metric: process RSS sums double-count "
                    "fork-shared mappings. Timing runs do not sample memory."
                ),
                "memory_repetitions_per_mode": args.memory_repetitions,
                "repetitions_per_mode": args.repetitions,
                "same_python_binary": sys.executable,
                "timing_scope": "pre-warmed production diff-worker phase only",
                "warm_cache": True,
                "warmups_per_mode": args.warmups,
            },
            "snapshot_stable": True,
            "summaries": {
                "memory": memory_summaries,
                "timing": timing_summaries,
            },
            "timing_runs": timing_runs,
            "warmup_runs": warmups,
        }


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri")
    parser.add_argument("--database")
    parser.add_argument("--current")
    parser.add_argument("--archive")
    parser.add_argument("--proxy-host")
    parser.add_argument("--proxy-port", type=int)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--common-sample", type=int, default=50000)
    parser.add_argument("--edge-sample", type=int, default=1000)
    parser.add_argument("--timeout-ms", type=int, default=10000)
    parser.add_argument("--max-id-scan", type=int, default=250000)
    parser.add_argument("--allow-large-id-scan", action="store_true")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=6)
    parser.add_argument("--memory-repetitions", type=int, default=3)
    parser.add_argument("--memory-sample-seconds", type=float, default=0.1)
    parser.add_argument("--runner-timeout", type=float, default=120)
    parser.add_argument("--output")
    parser.add_argument("--runner-mode", choices=("process", "free-threaded"), help=argparse.SUPPRESS)
    parser.add_argument("--measure-memory", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--label", help=argparse.SUPPRESS)
    parser.add_argument("--metadata-file", help=argparse.SUPPRESS)
    parser.add_argument("--ids-file", help=argparse.SUPPRESS)
    parser.add_argument("--reference-file", help=argparse.SUPPRESS)
    return parser


if __name__ == "__main__":
    parsed_args = build_parser().parse_args()
    if parsed_args.runner_mode:
        result = asyncio.run(run_single(parsed_args))
        print(RESULT_PREFIX + json.dumps(result, sort_keys=True, default=str))
    else:
        report = run_controller(parsed_args)
        rendered = json.dumps(report, indent=2, sort_keys=True, default=str)
        if parsed_args.output:
            Path(parsed_args.output).write_text(rendered)
            print(
                json.dumps(
                    {
                        "comparison": report["comparison"],
                        "output": str(Path(parsed_args.output).resolve()),
                        "summaries": report["summaries"],
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print(rendered)
