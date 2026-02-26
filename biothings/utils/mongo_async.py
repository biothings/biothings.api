"""
Async MongoDB utility module using PyMongo's native AsyncMongoClient (stable since pymongo 4.13.0).

This module mirrors the functionality of biothings.utils.mongo but uses the async API throughout.
All database operations are async (awaitable), making this suitable for use in async contexts
like the hub uploader without blocking the event loop.

Requires: pymongo >= 4.13.0
"""

import asyncio
import datetime
import glob
import io
import logging
import os
import time
from collections.abc import Iterable
from functools import wraps

import bson
import dateutil.parser as date_parser
from pymongo import DESCENDING, AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import AutoReconnect

from biothings.utils.backend import DocESBackend, DocMongoBackend
from biothings.utils.common import (
    dotdict,
    get_compressed_outfile,
    get_random_string,
    iter_n,
    open_compressed_file,
    timesofar,
)
from biothings.utils.hub_db import IDatabase

# stub, until set to real config module
config = None


async def handle_autoreconnect_async(func, *args, **kwargs):
    """
    Async version of the AutoReconnect retry handler.

    When the AutoReconnect exception is raised, we wait and retry.
    If the error persists after MAX_RETRY attempts, we raise.
    """
    MAX_RETRY = 30
    SLEEP_TIME = 0.5  # seconds

    retry = 0
    while retry < MAX_RETRY:
        try:
            return await func(*args, **kwargs)
        except AutoReconnect:
            retry += 1
            await asyncio.sleep(SLEEP_TIME)

    raise MaxRetryAutoReconnectException()


class MaxRetryAutoReconnectException(AutoReconnect):
    """Raised when we reach maximum retry to connect to Mongo server"""


class DummyAsyncCollection(dotdict):
    """Dummy async collection for when no real connection is available."""

    async def count_documents(self, *args, **kwargs):
        return 0

    async def estimated_document_count(self, **kwargs):
        return 0

    async def find_one(self, *args, **kwargs):
        return None

    def find(self, *args, **kwargs):
        return DummyAsyncCursor()

    async def insert_one(self, *args, **kwargs):
        return None

    async def insert_many(self, *args, **kwargs):
        return None

    async def update_one(self, *args, **kwargs):
        return None

    async def update_many(self, *args, **kwargs):
        return None

    async def delete_one(self, *args, **kwargs):
        return None

    async def delete_many(self, *args, **kwargs):
        return None

    async def replace_one(self, *args, **kwargs):
        return None

    async def drop(self):
        pass

    async def rename(self, *args, **kwargs):
        pass

    def __getitem__(self, what):
        return DummyAsyncCollection()

    @property
    def name(self):
        return ""


class DummyAsyncCursor:
    """Dummy async cursor that yields nothing."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def close(self):
        pass


class DummyAsyncDatabase(dotdict):
    """Dummy async database for when no real connection is available."""

    async def list_collection_names(self, *args, **kwargs):
        return []

    def __getitem__(self, what):
        return DummyAsyncCollection()


class AsyncCollection_(AsyncCollection):
    """
    Extended AsyncCollection with backward-compatible convenience methods.

    Mirrors the synchronous Collection class from mongo.py, providing
    legacy method signatures (insert, update, remove, save, count)
    on top of the native async pymongo AsyncCollection.
    """

    def __bool__(self):
        return self is not None

    async def insert(self, doc_or_docs, *args, **kwargs):
        if isinstance(doc_or_docs, Iterable) and not isinstance(doc_or_docs, dict):
            return await self.insert_many(doc_or_docs)
        else:
            return await self.insert_one(doc_or_docs)

    async def update(self, spec, doc, *args, **kwargs):
        if kwargs.pop("multi", None):
            return await self.update_many(spec, doc, *args, **kwargs)
        else:
            return await self.update_one(spec, doc, *args, **kwargs)

    async def remove(self, spec_or_id=None, **kwargs):
        if kwargs.pop("multi", None):
            return await self.delete_many(spec_or_id, **kwargs)
        else:
            return await self.delete_one(spec_or_id, **kwargs)

    async def save(self, doc, *args, **kwargs):
        if "_id" in doc:
            kwargs["upsert"] = True
            await self.replace_one({"_id": doc["_id"]}, doc, *args, **kwargs)
            return doc["_id"]
        else:
            res = await self.insert_one(doc, *args, **kwargs)
            return res.inserted_id

    async def count(self, _filter=None, **kwargs):
        if _filter:
            return await self.count_documents(_filter, **kwargs)
        return await self.estimated_document_count(**kwargs)

    async def collection_names(self, include_system_collections=True, session=None):
        """Convenience: delegate to the parent database's list_collection_names."""
        _filter = None
        if not include_system_collections:
            _filter = {"name": {"$regex": r"^(?!system\\.)"}}
        return await self.database.list_collection_names(session=session, filter=_filter)


class AsyncDatabase_(AsyncDatabase):
    """
    Extended AsyncDatabase with backward-compatible convenience methods.
    """

    def __bool__(self):
        return self is not None

    def __getitem__(self, name):
        return AsyncCollection_(self, name)

    async def collection_names(self, include_system_collections=True, session=None):
        _filter = None
        if not include_system_collections:
            _filter = {"name": {"$regex": r"^(?!system\\.)"}}
        return await self.list_collection_names(session=session, filter=_filter)


class AsyncDatabaseClient(AsyncMongoClient, IDatabase):
    """
    Async database client extending PyMongo's AsyncMongoClient.
    Implements the IDatabase interface for hub_db compatibility.
    """

    def __getitem__(self, name):
        return AsyncDatabase_(self, name)


def requires_config(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        global config
        if not config:
            try:
                from biothings import config as config_mod

                config = config_mod
            except ImportError:
                raise Exception("call biothings.config_for_app() first")
        return func(*args, **kwargs)

    return func_wrapper


@requires_config
def get_async_conn(server, port):
    """
    Create an AsyncDatabaseClient connection.
    Returns a DummyAsyncDatabase if config variables are missing/invalid.
    """
    try:
        if config.DATA_SRC_SERVER_USERNAME and config.DATA_SRC_SERVER_PASSWORD:
            uri = f"mongodb://{config.DATA_SRC_SERVER_USERNAME}:{config.DATA_SRC_SERVER_PASSWORD}@{server}:{port}"
        else:
            uri = f"mongodb://{server}:{port}"
        conn = AsyncDatabaseClient(uri)
        return conn
    except (AttributeError, ValueError):
        return DummyAsyncDatabase()


@requires_config
def get_async_hub_db_conn():
    conn = AsyncDatabaseClient(config.HUB_DB_BACKEND["uri"])
    return conn


@requires_config
def get_async_src_conn():
    return get_async_conn(config.DATA_SRC_SERVER, getattr(config, "DATA_SRC_PORT", 27017))


@requires_config
def get_async_src_db(conn=None):
    conn = conn or get_async_src_conn()
    return conn[config.DATA_SRC_DATABASE]


@requires_config
def get_async_src_master(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_MASTER_COLLECTION]


@requires_config
def get_async_src_dump(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config, "DATA_SRC_DUMP_COLLECTION", "src_dump")]


@requires_config
def get_async_src_build(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_BUILD_COLLECTION]


@requires_config
def get_async_src_build_config(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_BUILD_COLLECTION + "_config"]


@requires_config
def get_async_data_plugin(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_PLUGIN_COLLECTION]


@requires_config
def get_async_api(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.API_COLLECTION]


@requires_config
def get_async_cmd(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.CMD_COLLECTION]


@requires_config
def get_async_event(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config, "EVENT_COLLECTION", "event")]


@requires_config
def get_async_hub_config(conn=None):
    conn = conn or get_async_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config, "HUB_CONFIG_COLLECTION", "hub_config")]


@requires_config
async def get_async_last_command(conn=None):
    cmd = get_async_cmd(conn)
    cur = cmd.find({}, {"_id": 1}).sort("_id", DESCENDING).limit(1)
    async for doc in cur:
        return doc


@requires_config
def get_async_target_conn():
    if config.DATA_TARGET_SERVER_USERNAME and config.DATA_TARGET_SERVER_PASSWORD:
        uri = "mongodb://{}:{}@{}:{}".format(
            config.DATA_TARGET_SERVER_USERNAME,
            config.DATA_TARGET_SERVER_PASSWORD,
            config.DATA_TARGET_SERVER,
            config.DATA_TARGET_PORT,
        )
    else:
        uri = "mongodb://{}:{}".format(config.DATA_TARGET_SERVER, config.DATA_TARGET_PORT)
    conn = AsyncDatabaseClient(uri)
    return conn


@requires_config
def get_async_target_db(conn=None):
    conn = conn or get_async_target_conn()
    return conn[config.DATA_TARGET_DATABASE]


@requires_config
def get_async_target_master(conn=None):
    conn = conn or get_async_target_conn()
    return conn[config.DATA_TARGET_DATABASE][config.DATA_TARGET_MASTER_COLLECTION]


@requires_config
async def get_async_source_fullname(col_name: str):
    """
    Assuming col_name is a collection created from an upload process,
    find the main source & sub_source associated.
    """
    src_dump = get_async_src_dump()

    info = await src_dump.find_one(
        {
            "$where": 'function() {if(this.upload) {for(var index in this.upload.jobs) {if(this.upload.jobs[index].step == "%s") return this;}}}'
            % col_name
        }
    )
    if info:
        name = info["_id"]
        if name != col_name:
            return f"{name}.{col_name}"
        return name
    return col_name


async def get_async_source_fullnames(col_names):
    main_sources = set()
    for col_name in col_names:
        main_source = await get_async_source_fullname(col_name)
        if main_source:
            main_sources.add(main_source)
    return list(main_sources)


async def async_doc_feeder(
    collection,
    step=1000,
    s=None,
    e=None,
    inbatch=False,
    query=None,
    batch_callback=None,
    fields=None,
    logger=logging,
    session_refresh_interval=5,
):
    """
    An async generator returning docs in a collection, with batch query.

    This is the async equivalent of doc_feeder() from mongo.py.

    Additional filter query can be passed via `query`.
    `batch_callback` is a callback function as `fn(index, t)`, called after every batch.
    `fields` is an optional parameter to restrict the fields to return.
    `session_refresh_interval` is 5 minutes by default.
    """

    if isinstance(collection, DocMongoBackend):
        collection = collection.target_collection

    n = await collection.count_documents(query or {})
    s = s or 0
    e = e or n
    logger.debug(
        "Retrieving documents from collection '%s'. start = %d, end = %d, total = %d.", collection.name, s, e, n
    )

    cursor_index = s
    job_start_time = time.time()
    batch_start_time = time.time()

    session = None
    cur = None
    try:
        session: AsyncClientSession = await collection.database.client.start_session()
        session_uuid = session.session_id["id"].as_uuid()
        logger.debug("Session '%s' started for collection '%s'.", session_uuid, collection.name)

        cur = collection.find(query, no_cursor_timeout=True, projection=fields, session=session)
        logger.debug("Querying '%s' from collection '%s' in session '%s'.", query, collection.name, session_uuid)
        if s:
            cur.skip(s)
            logger.debug("Skipped %d documents from collection '%s'.", s, collection.name)
        if e:
            cur.limit(e - s)
            logger.debug(
                "Limited the cursor to fetch only %d documents (%d ~ %d) from collection '%s'.",
                e - s,
                s,
                e,
                collection.name,
            )

        cur.batch_size(step)

        if inbatch:
            doc_batch = []

        session_last_refresh_time = time.time()
        async for doc in cur:
            session_current_time = time.time()
            session_should_refresh = (session_current_time - session_last_refresh_time) > session_refresh_interval * 60
            if session_should_refresh:
                cmd_resp = await collection.database.command(
                    "refreshSessions", [session.session_id], session=session
                )
                logger.debug("Session '%s' refreshed, resp=%s", session_uuid, cmd_resp)
                session_last_refresh_time = session_current_time

            if inbatch:
                doc_batch.append(doc)
            else:
                yield doc

            cursor_index += 1

            if cursor_index % step == 0:
                if inbatch:
                    yield doc_batch
                    doc_batch = []

                logger.debug("Done.[%.1f%%,%s]", cursor_index * 100.0 / n, timesofar(batch_start_time))
                logger.debug("Processing %d-%d documents...", cursor_index + 1, min(cursor_index + step, e))
                if batch_callback:
                    batch_callback(cursor_index, time.time() - batch_start_time)
                if cursor_index < e:
                    batch_start_time = time.time()

        if inbatch and doc_batch:
            yield doc_batch

        logger.debug("Finished.[total time: %s]", timesofar(job_start_time))
    finally:
        if cur is not None:
            await cur.close()
        if session is not None:
            logger.debug("Session '%s' to be ended.", session.session_id["id"].as_uuid())
            await session.end_session()


def get_cache_filename(col_name):
    """Same as sync version – no I/O needed."""
    cache_folder = getattr(config, "CACHE_FOLDER", None)
    if not cache_folder:
        return None
    cache_format = getattr(config, "CACHE_FORMAT", None)
    cache_file = os.path.join(config.CACHE_FOLDER, col_name)
    cache_file = cache_format and (cache_file + ".%s" % cache_format) or cache_file
    return cache_file


async def async_invalidate_cache(col_name, col_type="src"):
    """Async version of invalidate_cache."""
    if col_type == "src":
        src_dump = get_async_src_dump()
        if "." not in col_name:
            fullname = await get_async_source_fullname(col_name)
        else:
            fullname = col_name
        assert fullname, "Can't resolve source '%s' (does it exist ?)" % col_name

        main, sub = fullname.split(".")
        doc = await src_dump.find_one({"_id": main})
        assert doc, "No such source '%s'" % main
        assert doc.get("upload", {}).get("jobs", {}).get(sub), "No such sub-source '%s'" % sub
        doc["upload"]["jobs"][sub]["started_at"] = datetime.datetime.now()
        await src_dump.update_one(
            {"_id": main}, {"$set": {"upload.jobs.%s.started_at" % sub: datetime.datetime.now()}}
        )
    elif col_type == "target":
        cache_file = get_cache_filename(col_name)
        if cache_file:
            try:
                os.remove(cache_file)
            except FileNotFoundError:
                pass


@requires_config
async def async_id_feeder(
    col, batch_size=1000, build_cache=True, logger=logging, force_use=False, force_build=False, validate_only=False
):
    """
    Async generator for all _ids in collection "col".

    This is the async equivalent of id_feeder() from mongo.py.
    Searches for a valid cache file if available, otherwise uses async_doc_feeder.
    """
    src_db = get_async_src_db()
    col_ts = None
    found_meta = True

    if isinstance(col, DocMongoBackend):
        col = col.target_collection

    try:
        if col.database.name == config.DATA_TARGET_DATABASE:
            info = await src_db["src_build"].find_one({"_id": col.name})
            if not info:
                logger.warning("Can't find information for target collection '%s'", col.name)
            else:
                col_ts = info.get("_meta", {}).get("build_date")
                col_ts = col_ts and date_parser.parse(col_ts).timestamp()
        elif col.database.name == config.DATA_SRC_DATABASE:
            src_dump = get_async_src_dump()
            info = await src_dump.find_one(
                {
                    "$where": 'function() {if(this.upload) {for(var index in this.upload.jobs) {if(this.upload.jobs[index].step == "%s") return this;}}}'
                    % col.name
                }
            )
            if not info:
                logger.warning("Can't find information for source collection '%s'", col.name)
            else:
                col_ts = info["upload"]["jobs"][col.name]["started_at"].timestamp()
        else:
            logger.warning("Can't find metadata for collection '%s' (not a target, not a source collection)", col)
            found_meta = False
            build_cache = False
    except KeyError:
        logger.warning("Couldn't find timestamp in database for '%s'", col.name)
    except Exception as e:
        logger.info("%s is not a mongo collection, _id cache won't be built (error: %s)", col, e)
        build_cache = False

    # try to find a cache file
    use_cache = False
    cache_file = None
    cache_format = getattr(config, "CACHE_FORMAT", None)
    if found_meta and getattr(config, "CACHE_FOLDER", None):
        cache_file = get_cache_filename(col.name)
        try:
            empty_size = {None: 0, "xz": 32, "gzip": 25, "bz2": 14}
            if force_build:
                logger.warning("Force building cache file")
                use_cache = False
            elif os.path.getsize(cache_file) <= empty_size.get(cache_format, 32):
                logger.warning("Cache file exists but is empty, delete it")
                os.remove(cache_file)
            elif force_use:
                use_cache = True
                logger.info("Force using cache file")
            else:
                cache_ts = os.path.getmtime(cache_file)
                if col_ts and cache_ts >= col_ts:
                    cache_dt = datetime.datetime.fromtimestamp(cache_ts).isoformat()
                    col_dt = datetime.datetime.fromtimestamp(col_ts).isoformat()
                    logger.debug("Cache is valid, cache_datetime:%s >= collection_datetime:%s", cache_dt, col_dt)
                    use_cache = True
                else:
                    logger.info("Cache is too old, discard it")
        except FileNotFoundError:
            pass

    if use_cache:
        logger.debug("Found valid cache file for '%s': %s", col.name, cache_file)
        if validate_only:
            logger.debug("Only validating cache, now return")
            yield []
            return

        with open_compressed_file(cache_file) as cache_in:
            if cache_format:
                io_cache = io.TextIOWrapper(cache_in)
            else:
                io_cache = cache_in
            for ids in iter_n(io_cache, batch_size):
                yield [_id.strip() for _id in ids if _id.strip()]
    else:
        logger.debug("No cache file found (or invalid) for '%s', use async_doc_feeder", col.name)
        cache_out = None
        cache_temp = None
        if getattr(config, "CACHE_FOLDER", None) and config.CACHE_FOLDER and build_cache:
            if not os.path.exists(config.CACHE_FOLDER):
                os.makedirs(config.CACHE_FOLDER)
            cache_temp = f"{cache_file}._tmp_"
            for tmp_cache in glob.glob(os.path.join(config.CACHE_FOLDER, f"{cache_temp}*")):
                logger.info("Removing aborted cache file '%s'", tmp_cache)
                os.remove(tmp_cache)
            cache_temp = f"{cache_temp}{get_random_string()}"
            cache_out = get_compressed_outfile(cache_temp, compress=cache_format)
            logger.info("Building cache file '%s'", cache_temp)
        else:
            logger.info("Can't build cache, cache not allowed or no cache folder")
            build_cache = False

        if isinstance(col, (AsyncCollection, AsyncCollection_)):
            async for doc_batch in async_doc_feeder(
                col, step=batch_size, inbatch=True, fields={"_id": 1}, logger=logger
            ):
                doc_ids = [str(_doc["_id"]) for _doc in doc_batch]
                if build_cache:
                    str_out = "\n".join(doc_ids) + "\n"
                    if cache_format:
                        cache_out.write(str_out.encode())
                    else:
                        cache_out.write(str_out)
                yield doc_ids
        elif isinstance(col, DocESBackend):
            ids = []
            for _id in col.get_id_list(step=batch_size):
                ids.append(_id)
                if len(ids) >= batch_size:
                    if build_cache:
                        str_out = "\n".join(ids) + "\n"
                        if cache_format:
                            cache_out.write(str_out.encode())
                        else:
                            cache_out.write(str_out)
                    yield ids
                    ids = []
            if ids:
                if build_cache:
                    str_out = "\n".join(ids) + "\n"
                    if cache_format:
                        cache_out.write(str_out.encode())
                    else:
                        cache_out.write(str_out)
                yield ids
        else:
            raise Exception("Unknown backend %s" % col)

        if build_cache:
            cache_out.close()
            cache_final = os.path.splitext(cache_temp)[0]
            try:
                os.rename(cache_temp, cache_final)
            except OSError:
                logger.exception("Couldn't set final cache filename, building cache failed")


def check_document_size(doc) -> True:
    """
    Return True if doc isn't too large for mongo DB.
    Pure computation, no async needed.
    """
    return len(bson.BSON.encode(doc)) < 16777216  # 16*1024*1024


@requires_config
async def get_async_previous_collection(new_id):
    """
    Given 'new_id', an _id from src_build, as the "new" collection,
    automatically select an "old" collection.
    """
    col = get_async_src_build()
    doc = await col.find_one({"_id": new_id})
    assert doc, "No build document found for '%s'" % new_id
    assert "build_config" in doc, "No build configuration found for document '%s'" % new_id
    assert doc["build_config"]["name"] == doc["build_config"]["_id"]

    cur = (
        col.find(
            {
                "$and": [
                    {"started_at": {"$lte": doc["started_at"]}},
                    {"build_config.name": doc["build_config"]["name"]},
                    {"archived": {"$exists": 0}},
                ]
            },
            {"_id": 1},
        )
        .sort([("started_at", -1)])
        .limit(2)
    )
    _ids = [d["_id"] async for d in cur]
    assert len(_ids) == 2, "Expecting 2 collection _ids, got: %s" % _ids
    assert _ids[0] == new_id, "Can't find collection _id '%s'" % new_id
    return _ids[1]
