"""Validate the production JSON-diff worker path against real MongoDB data.

This is a manual integration check whose MongoDB call path is read-only. It
samples a current source collection and one of its archives, runs the
production differ workers through JobManager, applies the generated patches,
and verifies that the sampled current state can be reconstructed from the
archive. Use a read-only MongoDB credential when write prevention must be
enforced server-side.

Example (the proxy URI options are consumed by this script, not PyMongo)::

    python tests/benchmarks/validate_real_mongo_diff.py \
        --uri 'mongodb://mongo.example.org:27017/?proxyHost=localhost&proxyPort=1080' \
        --database example_src --current example_collection
"""

import argparse
import asyncio
import copy
import hashlib
import heapq
import json
import logging
import os
import socket
import sys
import tempfile
import threading
import time
from functools import partial
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

READ_THREADS = set()
READ_THREADS_LOCK = threading.Lock()
ACTIVE_READS = 0
MAX_CONCURRENT_READS = 0
ACTIVE_READ_PAIRS = {}
OVERLAPPED_READ_PAIRS = 0
OUTER_WORKER_LOCK = threading.Lock()
ACTIVE_NEW_WORKERS = 0
MAX_CONCURRENT_NEW_WORKERS = 0


def configure_proxy_uri(uri, proxy_host=None, proxy_port=None, timeout_ms=10000):
    """Remove GUI proxy options from *uri* and install a SOCKS5 socket route."""
    parts = urlsplit(uri)
    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered == "proxyhost":
            proxy_host = proxy_host or value
        elif lowered == "proxyport":
            proxy_port = proxy_port or int(value)
        else:
            query.append((key, value))

    query_keys = {key.lower() for key, _value in query}
    if "directconnection" not in query_keys:
        query.append(("directConnection", "true"))
    if "serverselectiontimeoutms" not in query_keys:
        query.append(("serverSelectionTimeoutMS", str(timeout_ms)))
    clean_uri = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    if bool(proxy_host) != bool(proxy_port):
        raise ValueError("proxy host and port must be supplied together")
    if not proxy_host:
        return clean_uri

    import socks

    authority = parts.netloc.rsplit("@", 1)[-1]
    remote_hosts = set()
    for seed in authority.split(","):
        host = seed.rsplit(":", 1)[0].strip("[]")
        remote_hosts.add(host)

    original_getaddrinfo = socket.getaddrinfo

    def proxy_getaddrinfo(host, port, *args, **kwargs):
        if host in remote_hosts:
            # Keep the hostname unresolved so the SOCKS server performs DNS.
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (host, port))]
        return original_getaddrinfo(host, port, *args, **kwargs)

    socks.set_default_proxy(socks.SOCKS5, proxy_host, int(proxy_port), rdns=True)
    socket.socket = socks.socksocket
    socket.getaddrinfo = proxy_getaddrinfo
    return clean_uri


def install_config(workers, work_dir):
    from biothings.utils.common import DummyConfig

    config = DummyConfig("config")
    config.RUN_DIR = os.path.join(work_dir, "run")
    config.HUB_MAX_WORKERS = workers
    config.MAX_QUEUED_JOBS = 10000
    config.HUB_FREE_THREADED_WORKERS = True
    config.DATA_SRC_SERVER = "localhost"
    config.DATA_SRC_PORT = 27017
    config.DATA_SRC_DATABASE = "validation_src"
    config.DATA_SRC_SERVER_USERNAME = ""
    config.DATA_SRC_SERVER_PASSWORD = ""
    config.DATA_TARGET_SERVER = "localhost"
    config.DATA_TARGET_PORT = 27017
    config.DATA_TARGET_DATABASE = "validation_target"
    config.DATA_TARGET_SERVER_USERNAME = ""
    config.DATA_TARGET_SERVER_PASSWORD = ""
    config.DATA_HUB_DB_DATABASE = "validation_hubdb"
    config.HUB_DB_BACKEND = {"module": "biothings.utils.sqlite3", "sqlite_db_folder": work_dir}
    config.DATA_PLUGIN_FOLDER = os.path.join(work_dir, "plugins")
    config.DATA_ARCHIVE_ROOT = os.path.join(work_dir, "archive")
    config.LOG_FOLDER = os.path.join(work_dir, "logs")
    config.ACTIVE_DATASOURCES = []
    config.HUB_ENV = ""
    config.logger = logging.getLogger("real-mongo-diff-validation")
    os.makedirs(config.RUN_DIR, exist_ok=True)
    sys.modules["config"] = config
    sys.modules["biothings.config"] = config
    return config


def choose_archive(database, current_name, requested_archive=None):
    if requested_archive:
        if requested_archive not in database.list_collection_names():
            raise ValueError("archive collection does not exist: %s" % requested_archive)
        return requested_archive

    prefix = "%s_archive_" % current_name
    candidates = [name for name in database.list_collection_names() if name.startswith(prefix)]
    if not candidates:
        raise ValueError("no archive collections found with prefix %s" % prefix)

    dated = {}
    for name in candidates:
        suffix = name[len(prefix) :]
        date = suffix.split("_", 1)[0]
        if date.isdigit() and len(date) == 8:
            dated.setdefault(date, []).append(name)
    if not dated:
        raise ValueError("archive collections did not contain YYYYMMDD dates")
    newest = max(dated)
    newest_candidates = sorted(dated[newest])
    if len(newest_candidates) != 1:
        raise ValueError(
            "multiple archives exist for newest date %s; choose one with --archive: %s"
            % (newest, ", ".join(newest_candidates))
        )
    return newest_candidates[0]


def collection_uuid(database, name):
    result = database.command("listCollections", filter={"name": name})
    entries = result["cursor"]["firstBatch"]
    if len(entries) != 1:
        raise RuntimeError("could not resolve collection UUID for %s" % name)
    value = entries[0]["info"]["uuid"]
    return bytes(value).hex()


def load_ids(collection, max_ids=None):
    ids = set()
    for document in collection.find({}, {"_id": 1}, batch_size=10000):
        ids.add(document["_id"])
        if max_ids is not None and len(ids) > max_ids:
            raise RuntimeError("refusing to materialize more than %d IDs" % max_ids)
    return ids


def stable_sample(values, limit, salt):
    def key(value):
        payload = "%s\0%s\0%r" % (salt, type(value).__name__, value)
        return hashlib.sha256(payload.encode("utf-8")).digest()

    return heapq.nsmallest(min(limit, len(values)), values, key=key)


def chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def run_new_worker(id_batch, old_spec, new_spec, batch_num, diff_folder):
    global ACTIVE_NEW_WORKERS, MAX_CONCURRENT_NEW_WORKERS

    with OUTER_WORKER_LOCK:
        ACTIVE_NEW_WORKERS += 1
        MAX_CONCURRENT_NEW_WORKERS = max(MAX_CONCURRENT_NEW_WORKERS, ACTIVE_NEW_WORKERS)
    try:
        from biothings.hub.databuild.differ import diff_worker_new_vs_old
        from biothings.utils.diff import diff_docs_jsonpatch

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
        return {"worker": threading.current_thread().name, "summary": summary}
    finally:
        with OUTER_WORKER_LOCK:
            ACTIVE_NEW_WORKERS -= 1


def run_old_worker(id_batch, new_spec, batch_num, diff_folder):
    from biothings.hub.databuild.differ import diff_worker_old_vs_new

    summary = diff_worker_old_vs_new(id_batch, new_spec, batch_num, diff_folder)
    return {"worker": threading.current_thread().name, "summary": summary}


async def execute_workers(old_spec, new_spec, selected_new, selected_old, batch_size, workers, diff_folder):
    from concurrent.futures import ThreadPoolExecutor

    from biothings.utils.manager import JobManager

    manager = JobManager(
        asyncio.get_running_loop(),
        num_workers=workers,
        num_threads=workers,
        auto_recycle=False,
    )
    if not isinstance(manager.process_queue, ThreadPoolExecutor):
        raise RuntimeError("expected free-threaded ThreadPoolExecutor, got %s" % type(manager.process_queue).__name__)

    new_batches = list(chunks(selected_new, batch_size))
    old_batches = list(chunks(selected_old, batch_size))
    pinfo = {
        "category": "validation",
        "source": new_spec[2],
        "step": "real-mongo-jsondiff",
        "description": "",
    }
    started = time.perf_counter()
    try:
        new_jobs = []
        for batch_num, id_batch in enumerate(new_batches, 1):
            info = dict(pinfo, description="new-vs-old batch %d" % batch_num)
            new_jobs.append(
                await manager.defer_to_process(
                    info,
                    partial(run_new_worker, id_batch, old_spec, new_spec, batch_num, diff_folder),
                )
            )
        new_results = await asyncio.gather(*new_jobs)

        old_jobs = []
        first_old_batch = len(new_batches) + 1
        for offset, id_batch in enumerate(old_batches):
            batch_num = first_old_batch + offset
            info = dict(pinfo, description="old-vs-new batch %d" % batch_num)
            old_jobs.append(
                await manager.defer_to_process(
                    info,
                    partial(run_old_worker, id_batch, new_spec, batch_num, diff_folder),
                )
            )
        old_results = await asyncio.gather(*old_jobs)

        if manager.jobs or manager._process_job_ids or manager._pending_jobs_count():
            raise AssertionError("JobManager registries were not clean after validation")
        elapsed = time.perf_counter() - started
        return new_results, old_results, elapsed, type(manager.process_queue).__name__
    finally:
        manager.process_queue.shutdown()
        manager.thread_queue.shutdown(wait=False)


def load_diff_payloads(diff_folder, worker_results):
    from biothings.utils.common import loadobj, md5sum

    expected_files = {}
    for result in worker_results:
        file_info = result["summary"].get("diff_file")
        if file_info:
            expected_files[file_info["name"]] = file_info

    paths = sorted(Path(diff_folder).glob("*.pyobj"), key=lambda path: int(path.stem))
    if {path.name for path in paths} != set(expected_files):
        raise AssertionError("diff files did not match worker summaries")

    payloads = []
    for path in paths:
        file_info = expected_files[path.name]
        if md5sum(str(path)) != file_info["md5sum"]:
            raise AssertionError("MD5 mismatch for %s" % path.name)
        payloads.append(loadobj(str(path)))
    return payloads


def fetch_documents(collection, ids):
    documents = {}
    for id_batch in chunks(list(ids), 5000):
        documents.update({document["_id"]: document for document in collection.find({"_id": {"$in": id_batch}})})
    return documents


def filtered(document):
    from biothings.utils.common import filter_dict

    return filter_dict(copy.deepcopy(document), ["_timestamp"])


def validate_reconstruction(payloads, old_documents, new_documents, expected_adds, expected_deletes):
    from biothings.utils.jsondiff import make as jsondiff
    from biothings.utils.jsonpatch import apply_patch

    updates = {}
    adds = {}
    deletes = set()
    for payload in payloads:
        for update in payload["update"]:
            if update["_id"] in updates:
                raise AssertionError("duplicate update for %r" % update["_id"])
            updates[update["_id"]] = update["patch"]
        for document in payload["add"]:
            if document["_id"] in adds:
                raise AssertionError("duplicate add for %r" % document["_id"])
            adds[document["_id"]] = document
        deletes.update(payload["delete"])

    if set(adds) != set(expected_adds):
        raise AssertionError("add classification mismatch")
    if deletes != set(expected_deletes):
        raise AssertionError("delete classification mismatch")

    state = {document_id: filtered(document) for document_id, document in old_documents.items()}
    patch_operations = 0
    for document_id, patch in updates.items():
        state[document_id] = apply_patch(state[document_id], patch)
        patch_operations += len(patch)
    for document_id, document in adds.items():
        state[document_id] = filtered(document)
    for document_id in deletes:
        state.pop(document_id)

    expected = {document_id: filtered(document) for document_id, document in new_documents.items()}
    if set(state) != set(expected):
        raise AssertionError("reconstructed document IDs did not match current IDs")

    exact_failures = []
    semantic_failures = []
    for document_id, expected_document in expected.items():
        reconstructed = state[document_id]
        if reconstructed != expected_document:
            exact_failures.append(document_id)
        if jsondiff(copy.deepcopy(reconstructed), copy.deepcopy(expected_document)):
            semantic_failures.append(document_id)

    return {
        "updates": len(updates),
        "patch_operations": patch_operations,
        "adds": len(adds),
        "deletes": len(deletes),
        "documents_reconstructed": len(expected),
        "exact_failures": exact_failures,
        "semantic_failures": semantic_failures,
        "update_examples": list(updates)[:5],
    }


async def ordered_control(old_spec, new_spec, ids, old_documents, new_documents, batch_size, workers, work_dir):
    from biothings.utils import jsondiff as jsondiff_module

    if not ids:
        return {"documents_tested": 0, "exact_failures": []}

    jsondiff_module.UNORDERED_LIST = False
    diff_folder = os.path.join(work_dir, "ordered")
    os.makedirs(diff_folder)
    new_results, _old_results, elapsed, executor = await execute_workers(
        old_spec,
        new_spec,
        list(ids),
        [],
        batch_size,
        workers,
        diff_folder,
    )
    payloads = load_diff_payloads(diff_folder, new_results)
    updates = {}
    for payload in payloads:
        for update in payload["update"]:
            updates[update["_id"]] = update["patch"]

    from biothings.utils.jsonpatch import apply_patch

    failures = []
    for document_id in ids:
        reconstructed = filtered(old_documents[document_id])
        if document_id in updates:
            reconstructed = apply_patch(reconstructed, updates[document_id])
        if reconstructed != filtered(new_documents[document_id]):
            failures.append(document_id)
    return {
        "documents_tested": len(ids),
        "updates": len(updates),
        "exact_failures": failures,
        "elapsed_seconds": round(elapsed, 3),
        "executor": executor,
    }


async def main(args):
    global ACTIVE_READS, MAX_CONCURRENT_READS, OVERLAPPED_READ_PAIRS

    clean_uri = configure_proxy_uri(args.uri, args.proxy_host, args.proxy_port, args.timeout_ms)
    with tempfile.TemporaryDirectory(prefix="real_mongo_diff_") as work_dir:
        config = install_config(args.workers, work_dir)

        from pymongo import MongoClient, monitoring

        class ReadOnlyAuditListener(monitoring.CommandListener):
            def __init__(self):
                self.command_names = set()
                self.potential_writes = set()
                self.lock = threading.Lock()

            def started(self, event):
                command_name = event.command_name.lower()
                with self.lock:
                    self.command_names.add(command_name)
                    if command_name == "aggregate":
                        for stage in event.command.get("pipeline", []):
                            if "$out" in stage:
                                self.potential_writes.add("aggregate:$out")
                            if "$merge" in stage:
                                self.potential_writes.add("aggregate:$merge")

            def succeeded(self, event):
                pass

            def failed(self, event):
                pass

        audit_listener = ReadOnlyAuditListener()
        monitoring.register(audit_listener)

        client = MongoClient(clean_uri)
        database = client[args.database]
        client.admin.command("ping")
        archive_name = choose_archive(database, args.current, args.archive)
        current_collection = database[args.current]
        archive_collection = database[archive_name]
        estimated_counts = {
            "archive": archive_collection.estimated_document_count(),
            "current": current_collection.estimated_document_count(),
        }
        if max(estimated_counts.values()) > args.max_id_scan and not args.allow_large_id_scan:
            raise RuntimeError(
                "refusing to materialize more than %d IDs; use --allow-large-id-scan explicitly" % args.max_id_scan
            )
        uuids_before = {
            args.current: collection_uuid(database, args.current),
            archive_name: collection_uuid(database, archive_name),
        }

        hard_id_limit = None if args.allow_large_id_scan else args.max_id_scan
        current_ids = load_ids(current_collection, hard_id_limit)
        archive_ids = load_ids(archive_collection, hard_id_limit)
        common_ids = current_ids & archive_ids
        add_ids = current_ids - archive_ids
        delete_ids = archive_ids - current_ids

        common_sample = stable_sample(common_ids, args.common_sample, "common")
        add_sample = stable_sample(add_ids, args.edge_sample, "add")
        delete_sample = stable_sample(delete_ids, args.edge_sample, "delete")
        selected_new = common_sample + add_sample
        selected_old = common_sample + delete_sample

        old_documents = fetch_documents(archive_collection, selected_old)
        new_documents = fetch_documents(current_collection, selected_new)
        if len(old_documents) != len(selected_old) or len(new_documents) != len(selected_new):
            raise RuntimeError("collections changed while the sample documents were being fetched")

        import biothings.utils.diff as diff_module
        import biothings.utils.manager as manager_module
        from biothings.utils import jsondiff as jsondiff_module

        config._db = None
        manager_module.config._db = None
        jsondiff_module.UNORDERED_LIST = True
        jsondiff_module.USE_LIST_OPS = False

        original_loader = diff_module._load_documents_by_id

        def observed_loader(*loader_args, **loader_kwargs):
            global ACTIVE_READS, MAX_CONCURRENT_READS, OVERLAPPED_READ_PAIRS
            pair_key = id(loader_args[1])
            with READ_THREADS_LOCK:
                READ_THREADS.add(threading.current_thread().name)
                ACTIVE_READS += 1
                MAX_CONCURRENT_READS = max(MAX_CONCURRENT_READS, ACTIVE_READS)
                ACTIVE_READ_PAIRS[pair_key] = ACTIVE_READ_PAIRS.get(pair_key, 0) + 1
                if ACTIVE_READ_PAIRS[pair_key] == 2:
                    OVERLAPPED_READ_PAIRS += 1
            try:
                return original_loader(*loader_args, **loader_kwargs)
            finally:
                with READ_THREADS_LOCK:
                    ACTIVE_READS -= 1
                    ACTIVE_READ_PAIRS[pair_key] -= 1
                    if ACTIVE_READ_PAIRS[pair_key] == 0:
                        del ACTIVE_READ_PAIRS[pair_key]

        diff_module._load_documents_by_id = observed_loader

        if not hasattr(sys, "_is_gil_enabled") or sys._is_gil_enabled():
            raise RuntimeError("this validation requires a free-threaded Python runtime with the GIL disabled")

        old_spec = (clean_uri, args.database, archive_name)
        new_spec = (clean_uri, args.database, args.current)
        diff_folder = os.path.join(work_dir, "unordered")
        os.makedirs(diff_folder)
        new_results, old_results, elapsed, executor = await execute_workers(
            old_spec,
            new_spec,
            selected_new,
            selected_old,
            args.batch_size,
            args.workers,
            diff_folder,
        )
        worker_results = new_results + old_results
        outer_worker_threads = {result["worker"] for result in worker_results}
        if args.workers > 1 and len(selected_new) > args.batch_size and MAX_CONCURRENT_NEW_WORKERS < 2:
            raise AssertionError("outer free-threaded workers did not overlap")
        if common_sample and OVERLAPPED_READ_PAIRS == 0:
            raise AssertionError("no paired old/new MongoDB reads overlapped within a batch")
        payloads = load_diff_payloads(diff_folder, worker_results)

        old_documents_after = fetch_documents(archive_collection, selected_old)
        new_documents_after = fetch_documents(current_collection, selected_new)
        if old_documents_after != old_documents or new_documents_after != new_documents:
            raise RuntimeError("sampled documents changed while validation was running")

        validation = validate_reconstruction(
            payloads,
            old_documents,
            new_documents,
            add_sample,
            delete_sample,
        )
        if validation["semantic_failures"]:
            raise AssertionError("production-semantic reconstruction failed")
        if validation["updates"] == 0 and not args.allow_no_updates:
            raise RuntimeError("no JSON patches were generated; validation is inconclusive")

        ordered_ids = [document_id for document_id in validation["exact_failures"] if document_id in common_ids]
        ordered = await ordered_control(
            old_spec,
            new_spec,
            ordered_ids,
            old_documents,
            new_documents,
            args.batch_size,
            args.workers,
            work_dir,
        )
        if ordered["exact_failures"]:
            raise AssertionError("ordered-list reconstruction failed")

        old_documents_final = fetch_documents(archive_collection, selected_old)
        new_documents_final = fetch_documents(current_collection, selected_new)
        if old_documents_final != old_documents or new_documents_final != new_documents:
            raise RuntimeError("sampled documents changed during the ordered control")

        uuids_after = {
            args.current: collection_uuid(database, args.current),
            archive_name: collection_uuid(database, archive_name),
        }
        if uuids_before != uuids_after:
            raise RuntimeError("collection UUID changed during validation")
        if sys._is_gil_enabled():
            raise RuntimeError("an imported dependency enabled the GIL during validation")

        write_commands = {
            "aborttransaction",
            "applyops",
            "bulkwrite",
            "clonecollection",
            "clonecollectionascapped",
            "collmod",
            "committransaction",
            "compact",
            "converttocapped",
            "copydb",
            "create",
            "createindexes",
            "createrole",
            "createuser",
            "delete",
            "drop",
            "dropallrolesfromdatabase",
            "dropallusersfromdatabase",
            "dropdatabase",
            "dropindexes",
            "droprole",
            "dropuser",
            "findandmodify",
            "grantprivilegestorole",
            "grantrolestorole",
            "grantrolestouser",
            "insert",
            "mapreduce",
            "reindex",
            "renamecollection",
            "revokeprivilegesfromrole",
            "revokerolesfromrole",
            "revokerolesfromuser",
            "setfeaturecompatibilityversion",
            "update",
            "updaterole",
            "updateuser",
        }
        observed_write_commands = sorted(
            (audit_listener.command_names & write_commands) | audit_listener.potential_writes
        )
        if observed_write_commands:
            raise AssertionError("unexpected MongoDB write commands: %s" % observed_write_commands)

        parsed = urlsplit(clean_uri)
        report = {
            "server": "%s:%s" % (parsed.hostname, parsed.port or 27017),
            "database": args.database,
            "archive": archive_name,
            "current": args.current,
            "collection_counts": {"archive": len(archive_ids), "current": len(current_ids)},
            "id_classification": {
                "common": len(common_ids),
                "add": len(add_ids),
                "delete": len(delete_ids),
            },
            "sample": {
                "common": len(common_sample),
                "add": len(add_sample),
                "delete": len(delete_sample),
            },
            "runtime": {
                "python": sys.version.split()[0],
                "gil_enabled": sys._is_gil_enabled(),
                "executor": executor,
                "outer_worker_threads": sorted(outer_worker_threads),
                "max_concurrent_outer_workers": MAX_CONCURRENT_NEW_WORKERS,
                "mongo_reader_threads": sorted(READ_THREADS),
                "max_concurrent_mongo_reads": MAX_CONCURRENT_READS,
                "overlapped_old_new_read_pairs": OVERLAPPED_READ_PAIRS,
                "elapsed_seconds": round(elapsed, 3),
            },
            "mongo_audit": {
                "commands_observed": sorted(audit_listener.command_names),
                "write_commands_observed": observed_write_commands,
            },
            "diff_files": len(payloads),
            "validation": {
                **validation,
                "exact_failures": [str(value) for value in validation["exact_failures"]],
                "semantic_failures": [str(value) for value in validation["semantic_failures"]],
            },
            "ordered_control": {
                **ordered,
                "exact_failures": [str(value) for value in ordered["exact_failures"]],
            },
            "collection_uuids_stable": True,
            "sampled_documents_stable": True,
        }
        print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--archive")
    parser.add_argument("--proxy-host")
    parser.add_argument("--proxy-port", type=int)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--common-sample", type=int, default=5000)
    parser.add_argument("--edge-sample", type=int, default=1000)
    parser.add_argument("--timeout-ms", type=int, default=10000)
    parser.add_argument("--max-id-scan", type=int, default=250000)
    parser.add_argument("--allow-large-id-scan", action="store_true")
    parser.add_argument("--allow-no-updates", action="store_true")
    asyncio.run(main(parser.parse_args()))
