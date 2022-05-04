import time, logging, os, io, glob, datetime
import dateutil.parser as dtparser
from collections import Iterable
from functools import wraps
from pymongo import MongoClient, DESCENDING
from pymongo.database import Database as PymongoDatabase
from pymongo.collection import Collection as PymongoCollection
from functools import partial
from collections import defaultdict
import bson

from biothings.utils.common import timesofar, get_random_string, iter_n, \
                                   open_compressed_file, get_compressed_outfile, \
                                   dotdict
from biothings.utils.backend import DocESBackend, DocMongoBackend
from biothings.utils.hub_db import IDatabase, ChangeWatcher
# stub, until set to real config module
config = None


class DummyCollection(dotdict):
    def count(self):
        return None
    def drop(self):
        pass
    def __getitem__(self,what):
        return DummyCollection() # ???


class DummyDatabase(dotdict):
    def collection_names(self):
        return []
    def __getitem__(self,what):
        return DummyCollection()


class Collection(PymongoCollection):
    # https://pymongo.readthedocs.io/en/4.1.1/migrate-to-pymongo4.html

    def __bool__(self):
        return self is not None

    def insert(self, doc_or_docs, *args, **kwargs):
        if isinstance(doc_or_docs, Iterable):
            return self.insert_many(doc_or_docs)
        else:
            return self.insert_one(doc_or_docs)

    def update(self, spec, doc, *args, **kwargs):
        if kwargs.pop("multi", None):
            return self.update_many(spec, doc, *args, **kwargs)
        else:
            return self.update_one(spec, doc, *args, **kwargs)

    def remove(self, spec_or_id=None, **kwargs):
        if kwargs.pop("multi", None):
            return self.delete_many(spec_or_id, **kwargs)
        else:
            return self.delete_one(spec_or_id, **kwargs)

    def save(self, doc, *args, **kwargs):
        if '_id' in doc:
            kwargs["upsert"] = True
            self.replace_one({'_id': doc['_id']}, doc, *args, **kwargs)
            return doc['_id']
        else:
            res = self.insert_one(doc, *args, **kwargs)
            return res.inserted_id

    def count(self, _filter=None, **kwargs):
        if _filter:
            return self.count_documents(_filter, **kwargs)
        return self.estimated_document_count(**kwargs)


class Database(PymongoDatabase):
    def __bool__(self):
        return self is not None

    def __getitem__(self, name):
        return Collection(self, name)

    def collection_names(self, include_system_collections=True, session=None):
        _filter = None
        if not include_system_collections:
            _filter = {"name": {"$regex": r"^(?!system\\.)"}}
        return self.list_collection_names(session=session, filter=_filter)


class DatabaseClient(MongoClient, IDatabase):
    def __getitem__(self, name):
        return Database(self, name)


def requires_config(func):
    @wraps(func)
    def func_wrapper(*args,**kwargs):
        global config
        if not config:
            try:
                from biothings import config as config_mod
                config = config_mod
            except ImportError:
                raise Exception("call biothings.config_for_app() first")
        return func(*args,**kwargs)
    return func_wrapper


@requires_config
def get_conn(server, port):
    try:
        if config.DATA_SRC_SERVER_USERNAME and config.DATA_SRC_SERVER_PASSWORD:
            uri = "mongodb://{}:{}@{}:{}".format(config.DATA_SRC_SERVER_USERNAME,
                                                 config.DATA_SRC_SERVER_PASSWORD,
                                                 server, port)
        else:
            uri = "mongodb://{}:{}".format(server, port)
        conn = DatabaseClient(uri)
        return conn
    except (AttributeError,ValueError) as e:
        # missing config variables (or invalid), we'll pretend it's a dummy access to mongo
        # (dummy here means there really shouldn't be any call to get_conn()
        # but mongo is too much tied to the code and needs more work to 
        # unlink it
        return DummyDatabase()


@requires_config
def get_hub_db_conn():
    conn = DatabaseClient(config.HUB_DB_BACKEND["uri"])
    return conn

@requires_config
def get_src_conn():
    return get_conn(config.DATA_SRC_SERVER, getattr(config,"DATA_SRC_PORT",27017))

@requires_config
def get_src_db(conn=None):
    conn = conn or get_src_conn()
    return conn[config.DATA_SRC_DATABASE]

@requires_config
def get_src_master(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_MASTER_COLLECTION]

@requires_config
def get_src_dump(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config,"DATA_SRC_DUMP_COLLECTION","src_dump")]

@requires_config
def get_src_build(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_BUILD_COLLECTION]

@requires_config
def get_src_build_config(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_SRC_BUILD_COLLECTION + "_config"]

@requires_config
def get_data_plugin(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.DATA_PLUGIN_COLLECTION]

@requires_config
def get_api(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.API_COLLECTION]

@requires_config
def get_cmd(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][config.CMD_COLLECTION]

@requires_config
def get_event(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config,"EVENT_COLLECTION","event")]

@requires_config
def get_hub_config(conn=None):
    conn = conn or get_hub_db_conn()
    return conn[config.DATA_HUB_DB_DATABASE][getattr(config,"HUB_CONFIG_COLLECTION","hub_config")]

@requires_config
def get_last_command(conn=None):
    cmd = get_cmd(conn)
    cur = cmd.find({},{"_id":1}).sort("_id",DESCENDING).limit(1)
    return next(cur)

@requires_config
def get_target_conn():
    if config.DATA_TARGET_SERVER_USERNAME and config.DATA_TARGET_SERVER_PASSWORD:
        uri = "mongodb://{}:{}@{}:{}".format(config.DATA_TARGET_SERVER_USERNAME,
                                             config.DATA_TARGET_SERVER_PASSWORD,
                                             config.DATA_TARGET_SERVER,
                                             config.DATA_TARGET_PORT)
    else:
        uri = "mongodb://{}:{}".format(config.DATA_TARGET_SERVER,config.DATA_TARGET_PORT)
    conn = DatabaseClient(uri)
    return conn


@requires_config
def get_target_db(conn=None):
    conn = conn or get_target_conn()
    return conn[config.DATA_TARGET_DATABASE]


@requires_config
def get_target_master(conn=None):
    conn = conn or get_target_conn()
    return conn[config.DATA_TARGET_DATABASE][config.DATA_TARGET_MASTER_COLLECTION]

@requires_config
def get_source_fullname(col_name):
    """
    Assuming col_name is a collection created from an upload process,
    find the main source & sub_source associated.
    """
    src_dump = get_src_dump()
    # "sources" in config is a list a collection names. src_dump _id is the name of the
    # resource but can have sub-resources with different collection names. We need
    # to query inner keys upload.job.*.step, which always contains the collection name  
    info = src_dump.find_one({"$where":"function() {if(this.upload) {for(var index in this.upload.jobs) {if(this.upload.jobs[index].step == \"%s\") return this;}}}" % col_name})
    if info:
        name = info["_id"]
        if name != col_name:
            # col_name was a sub-source name
            return "%s.%s" % (name,col_name)
        else:
            return name

def get_source_fullnames(col_names):
    main_sources = set()
    for col_name in col_names:
        main_source = get_source_fullname(col_name)
        if main_source:
            main_sources.add(main_source)
    return list(main_sources)

def doc_feeder(collection, step=1000, s=None, e=None, inbatch=False, query=None, batch_callback=None,
               fields=None, logger=logging):
    '''A iterator for returning docs in a collection, with batch query.
       additional filter query can be passed via "query", e.g.,
       doc_feeder(collection, query={'taxid': {'$in': [9606, 10090, 10116]}})
       batch_callback is a callback function as fn(cnt, t), called after every batch
       fields is optional parameter passed to find to restrict fields to return.
    '''
    if isinstance(collection,DocMongoBackend):
        collection = collection.target_collection
    cur = collection.find(query, no_cursor_timeout=True, projection=fields)
    n = collection.count_documents(query or {})
    s = s or 0
    e = e or n
    ##logger.info('Retrieving %d documents from database "%s".' % (n, collection.name))
    t0 = time.time()
    if inbatch:
        doc_li = []
    cnt = 0
    t1 = time.time()
    try:
        if s:
            cur.skip(s)
            cnt = s
            ##logger.info("Skipping %d documents." % s)
        if e:
            cur.limit(e - (s or 0))
        cur.batch_size(step)
        ##logger.info("Processing %d-%d documents..." % (cnt + 1, min(cnt + step, e)))
        for doc in cur:
            if inbatch:
                doc_li.append(doc)
            else:
                yield doc
            cnt += 1
            if cnt % step == 0:
                if inbatch:
                    yield doc_li
                    doc_li = []
                if n:
                    pass
                    ##logger.info('Done.[%.1f%%,%s]' % (cnt * 100. / n, timesofar(t1)))
                else:
                    pass
                    ##logger.info('Nothing to do...')
                if batch_callback:
                    batch_callback(cnt, time.time()-t1)
                if cnt < e:
                    t1 = time.time()
                    ##logger.info("Processing %d-%d documents..." % (cnt + 1, min(cnt + step, e)))
        if inbatch and doc_li:
            #Important: need to yield the last batch here
            yield doc_li

        #print 'Done.[%s]' % timesofar(t1)
        if n:
            pass
            ##logger.info('Done.[%.1f%%,%s]' % (cnt * 100. / n, timesofar(t1)))
        else:
            pass
            ##logger.info('Nothing to do...')
        ##logger.info("=" * 20)
        ##logger.info('Finished.[total time: %s]' % timesofar(t0))
    finally:
        cur.close()


def get_cache_filename(col_name):
    cache_folder = getattr(config,"CACHE_FOLDER",None)
    if not cache_folder:
        return # we don't even use cache, forget it
    cache_format = getattr(config,"CACHE_FORMAT",None)
    cache_file = os.path.join(config.CACHE_FOLDER,col_name)
    cache_file = cache_format and (cache_file + ".%s" % cache_format) or cache_file
    return cache_file


def invalidate_cache(col_name,col_type="src"):
    if col_type == "src":
        src_dump = get_src_dump()
        if not "." in col_name:
            fullname = get_source_fullname(col_name)
        assert fullname, "Can't resolve source '%s' (does it exist ?)" % col_name

        main,sub = fullname.split(".")
        doc = src_dump.find_one({"_id":main})
        assert doc, "No such source '%s'" % main
        assert doc.get("upload",{}).get("jobs",{}).get(sub), "No such sub-source '%s'" % sub
        # this will make the cache too old
        doc["upload"]["jobs"][sub]["started_at"] = datetime.datetime.now()
        src_dump.update_one({"_id":main},{"$set" : {"upload.jobs.%s.started_at" % sub:datetime.datetime.now()}})
    elif col_type == "target":
        # just delete the cache file
        cache_file = get_cache_filename(col_name)
        if cache_file:
            try:
                os.remove(cache_file)
            except FileNotFoundError:
                pass


# TODO: this func deals with different backend, should not be in bt.utils.mongo
# and doc_feeder should do the same as this function regarding backend support
@requires_config
def id_feeder(col, batch_size=1000, build_cache=True, logger=logging,
              force_use=False, force_build=False, validate_only=False):
    """Return an iterator for all _ids in collection "col"
       Search for a valid cache file if available, if not
       return a doc_feeder for that collection. Valid cache is
       a cache file that is newer than the collection.
       "db" can be "target" or "src".
       "build_cache" True will build a cache file as _ids are fetched, 
       if no cache file was found
       "force_use" True will use any existing cache file and won't check whether
       it's valid of not.
       "force_build" True will build a new cache even if current one exists
       and is valid.
       "validate_only" will directly return [] if the cache is valid (convenient
       way to check if the cache is valid)
    """
    src_db = get_src_db()
    ts = None
    found_meta = True

    if isinstance(col,DocMongoBackend):
        col = col.target_collection

    try:
        if col.database.name == config.DATA_TARGET_DATABASE:
            info = src_db["src_build"].find_one({"_id": col.name})
            if not info:
                logger.warning("Can't find information for target collection '%s'" % col.name)
            else:
                ts = info.get("_meta",{}).get("build_date")
                ts = ts and dtparser.parse(ts).timestamp()
        elif col.database.name == config.DATA_SRC_DATABASE:
            src_dump = get_src_dump()
            info = src_dump.find_one({"$where":"function() {if(this.upload) {for(var index in this.upload.jobs) {if(this.upload.jobs[index].step == \"%s\") return this;}}}" % col.name})
            if not info:
                logger.warning("Can't find information for source collection '%s'" % col.name)
            else:
                ts = info["upload"]["jobs"][col.name]["started_at"].timestamp()
        else:
            logging.warning("Can't find metadata for collection '%s' (not a target, not a source collection)" % col)
            found_meta = False
            build_cache = False
    except KeyError:
        logger.warning("Couldn't find timestamp in database for '%s'" % col.name)
    except Exception as e:
        logger.info("%s is not a mongo collection, _id cache won't be built (error: %s)" % (col,e))
        build_cache = False

    # try to find a cache file
    use_cache = False
    cache_file = None
    cache_format = getattr(config,"CACHE_FORMAT",None)
    if found_meta and getattr(config,"CACHE_FOLDER",None):
        cache_file = get_cache_filename(col.name)
        try:
            # size of empty file differs depending on compression
            empty_size = {None:0,"xz":32,"gzip":25,"bz2":14}
            if force_build:
                logger.warning("Force building cache file")
                use_cache = False
            # check size, delete if invalid
            elif os.path.getsize(cache_file) <= empty_size.get(cache_format,32): 
                logger.warning("Cache file exists but is empty, delete it")
                os.remove(cache_file)
            elif force_use:
                use_cache = True
                logger.info("Force using cache file")
            else:
                mt = os.path.getmtime(cache_file)
                if ts and mt >= ts:
                    dtmt = datetime.datetime.fromtimestamp(mt).isoformat()
                    dtts = datetime.datetime.fromtimestamp(ts).isoformat()
                    logging.debug("Cache is valid, modiftime_cache:%s >= col_timestamp:%s" % (dtmt,dtts))
                    use_cache = True
                else:
                    logger.info("Cache is too old, discard it")
        except FileNotFoundError:
            pass
    if use_cache:
        logger.debug("Found valid cache file for '%s': %s" % (col.name,cache_file))
        if validate_only:
            logging.debug("Only validating cache, now return")
            return []
        with open_compressed_file(cache_file) as cache_in:
            if cache_format:
                iocache = io.TextIOWrapper(cache_in)
            else:
                iocache = cache_in
            for ids in iter_n(iocache,batch_size):
                yield [_id.strip() for _id in ids if _id.strip()]
    else:
        logger.debug("No cache file found (or invalid) for '%s', use doc_feeder" % col.name)
        cache_out = None
        cache_temp = None
        if getattr(config,"CACHE_FOLDER",None) and config.CACHE_FOLDER and build_cache:
            if not os.path.exists(config.CACHE_FOLDER):
                os.makedirs(config.CACHE_FOLDER)
            cache_temp = "%s._tmp_" % cache_file
            # clean aborted cache file generation
            for tmpcache in glob.glob(os.path.join(config.CACHE_FOLDER,"%s*" % cache_temp)):
                logger.info("Removing aborted cache file '%s'" % tmpcache)
                os.remove(tmpcache)
            # use temp file and rename once done
            cache_temp = "%s%s" % (cache_temp,get_random_string())
            cache_out = get_compressed_outfile(cache_temp,compress=cache_format)
            logger.info("Building cache file '%s'" % cache_temp)
        else:
            logger.info("Can't build cache, cache not allowed or no cache folder")
            build_cache = False
        if isinstance(col, PymongoCollection):
            doc_feeder_func = partial(doc_feeder,col, step=batch_size, inbatch=True, fields={"_id":1})
        elif isinstance(col,DocMongoBackend):
            doc_feeder_func = partial(doc_feeder,col.target_collection, step=batch_size, inbatch=True, fields={"_id":1})
        elif isinstance(col,DocESBackend):
            # get_id_list directly return the _id, wrap it to match other 
            # doc_feeder_func returned vals. Also return a batch of id
            def wrap_id():
                ids = []
                for _id in col.get_id_list(step=batch_size):
                    ids.append({"_id":_id})
                    if len(ids) >= batch_size:
                        yield ids
                        ids = []
                if ids:
                    yield ids
            doc_feeder_func = partial(wrap_id)
        else:
            raise Exception("Unknown backend %s" % col)
        for doc_ids in doc_feeder_func():
            doc_ids = [str(_doc["_id"]) for _doc in doc_ids]
            if build_cache:
                strout = "\n".join(doc_ids) + "\n"
                if cache_format:
                    # assuming binary format (b/ccompressed)
                    cache_out.write(strout.encode())
                else:
                    cache_out.write(strout)
            yield doc_ids
        if build_cache:
            cache_out.close()
            cache_final = os.path.splitext(cache_temp)[0]
            try:
                os.rename(cache_temp,cache_final)
            except Exception as e:
                logger.exception("Couldn't set final cache filename, building cache failed")


def check_document_size(doc):
    """
    Return True if doc isn't too large for mongo DB
    """
    return len(bson.BSON.encode(doc)) < 16777216 #16*1024*1024

def get_previous_collection(new_id):
    """
    Given 'new_id', an _id from src_build, as the "new" collection,
    automatically select an "old" collection. By default, src_build's documents
    will be sorted according to their name (_id) and old colleciton is the one
    just before new_id.
    Note: because there can more than one build config used, the actual build config
    name is first determined using new_id collection name, then the find.sort is done
    on collections containing that build config name.
    """
    # TODO: this is not compatible with a generic hub_db backend
    # TODO: this should return a collection with status=success
    col = get_src_build()
    doc = col.find_one({"_id":new_id})
    assert doc, "No build document found for '%s'" % new_id
    assert "build_config" in doc, "No build configuration found for document '%s'" % new_id 
    assert doc["build_config"]["name"] == doc["build_config"]["_id"]
    confname = doc["build_config"]["name"]
    docs = get_src_build().find({
        "$and":[
            {"started_at":{"$lte":doc["started_at"]}},
            {"build_config.name":confname},
            {"archived":{"$exists":0}},
            ]},
        {"_id":1}).sort([("started_at",-1)]).limit(2)
    _ids = [d["_id"] for d in docs]
    assert len(_ids) == 2, "Expecting 2 collection _ids, got: %s" % _ids
    assert _ids[0] == new_id, "Can't find collection _id '%s'" % new_id
    return _ids[1]

