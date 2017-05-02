
import time, logging, os, io, glob, datetime
from functools import wraps
from pymongo import MongoClient

from biothings.utils.common import timesofar, get_random_string, iter_n, \
                                   open_compressed_file, get_compressed_outfile
# stub, until set to real config module
config = None

class Connection(MongoClient):
    """
    This class mimicks / is a mock for mongokit.Connection class,
    used to keep used interface (registering document model for instance)
    """
    def __init__(self, *args, **kwargs):
        super(Connection,self).__init__(*args,**kwargs)
        self._registered_documents = {}
    def register(self, obj):
        self._registered_documents[obj.__name__] = obj
    def __getattr__(self,key):
        if key in self._registered_documents:
            document = self._registered_documents[key]
            return document
        else:
            try:
                return self[key]
            except Exception:
                raise AttributeError(key)

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
    if config.DATA_SRC_SERVER_USERNAME and config.DATA_SRC_SERVER_PASSWORD:
        uri = "mongodb://{}:{}@{}:{}".format(config.DATA_SRC_SERVER_USERNAME,
                                             config.DATA_SRC_SERVER_PASSWORD,
                                             server, port)
    else:
        uri = "mongodb://{}:{}".format(server, port)
    conn = Connection(uri)
    return conn


@requires_config
def get_src_conn():
    return get_conn(config.DATA_SRC_SERVER, config.DATA_SRC_PORT)


@requires_config
def get_src_db(conn=None):
    conn = conn or get_src_conn()
    return conn[config.DATA_SRC_DATABASE]


@requires_config
def get_src_master(conn=None):
    conn = conn or get_src_conn()
    return conn[config.DATA_SRC_DATABASE][config.DATA_SRC_MASTER_COLLECTION]


@requires_config
def get_src_dump(conn=None):
    conn = conn or get_src_conn()
    return conn[config.DATA_SRC_DATABASE][config.DATA_SRC_DUMP_COLLECTION]


@requires_config
def get_src_build(conn=None):
    conn = conn or get_src_conn()
    return conn[config.DATA_SRC_DATABASE][config.DATA_SRC_BUILD_COLLECTION]


@requires_config
def get_target_conn():
    if config.DATA_TARGET_SERVER_USERNAME and config.DATA_TARGET_SERVER_PASSWORD:
        uri = "mongodb://{}:{}@{}:{}".format(config.DATA_TARGET_SERVER_USERNAME,
                                             config.DATA_TARGET_SERVER_PASSWORD,
                                             config.DATA_TARGET_SERVER,
                                             config.DATA_TARGET_PORT)
    else:
        uri = "mongodb://{}:{}".format(config.DATA_TARGET_SERVER,config.DATA_TARGET_PORT)
    conn = Connection(uri)
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
    cur = collection.find(query, no_cursor_timeout=True, projection=fields)
    n = cur.count()
    s = s or 0
    e = e or n
    logger.info('Retrieving %d documents from database "%s".' % (n, collection.name))
    t0 = time.time()
    if inbatch:
        doc_li = []
    cnt = 0
    t1 = time.time()
    try:
        if s:
            cur.skip(s)
            cnt = s
            logger.info("Skipping %d documents." % s)
        if e:
            cur.limit(e - (s or 0))
        cur.batch_size(step)
        logger.info("Processing %d-%d documents..." % (cnt + 1, min(cnt + step, e)))
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
                    logger.info('Done.[%.1f%%,%s]' % (cnt * 100. / n, timesofar(t1)))
                else:
                    logger.info('Nothing to do...')
                if batch_callback:
                    batch_callback(cnt, time.time()-t1)
                if cnt < e:
                    t1 = time.time()
                    logger.info("Processing %d-%d documents..." % (cnt + 1, min(cnt + step, e)))
        if inbatch and doc_li:
            #Important: need to yield the last batch here
            yield doc_li

        #print 'Done.[%s]' % timesofar(t1)
        if n:
            logger.info('Done.[%.1f%%,%s]' % (cnt * 100. / n, timesofar(t1)))
        else:
            logger.info('Nothing to do...')
        logger.info("=" * 20)
        logger.info('Finished.[total time: %s]' % timesofar(t0))
    finally:
        cur.close()

@requires_config
def id_feeder(col, batch_size=1000, build_cache=True, logger=logging,
              force_use=False, force_build=False):
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
    """
    src_db = get_src_db()
    ts = None
    found_meta = True

    try:
        if col.database.name == config.DATA_TARGET_DATABASE:
            # TODO: if col.name is present in different build config, that will pick one
            # (order not ensured) and maybe timestamp will be wrong. It's based on naming
            # convention or at least the fact we don't use the same name for 2 different
            # build config
            info = src_db["src_build"].find_one({"build.target_name" : col.name},{"build.started_at":1})
            if not info:
                logger.warning("Can't find information for target collection '%s'" % col.name)
            elif not len(info["build"]) > 0:
                logger.warning("Missing build information for targer collection '%s'" % col.name)
            else:
                ts = info["build"][-1]["started_at"].timestamp()
        elif col.database.name == config.DATA_SRC_DATABASE:
            info = src_db["src_dump"].find_one({"$where":"function() {if(this.upload) {for(var index in this.upload.jobs) {if(this.upload.jobs[index].step == \"%s\") return this;}}}" % col.name})
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
                    use_cache = True
                else:
                    logger.info("Cache is too old, discard it")
        except FileNotFoundError:
            pass
    if use_cache:
        logger.debug("Found valid cache file for '%s': %s" % (col.name,cache_file))
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
            logger.info("Can't build cache, no cache folder")
            build_cache = False
        for doc_ids in doc_feeder(col, step=batch_size, inbatch=True, fields={"_id":1}):
            doc_ids = [_doc["_id"] for _doc in doc_ids]
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
            os.rename(cache_temp,cache_final)


def src_clean_archives(keep_last=1, src=None, verbose=True, noconfirm=False):
    '''clean up archive collections in src db, only keep last <kepp_last>
       number of archive.
    '''
    from biothings.utils.dataload import list2dict
    from biothings.utils.common import ask

    src = src or get_src_db()

    archive_li = sorted([(coll.split('_archive_')[0], coll) for coll in src.collection_names()
                         if coll.find('archive') != -1])
    archive_d = list2dict(archive_li, 0, alwayslist=1)
    coll_to_remove = []
    for k, v in archive_d.items():
        print(k, end='')
        #check current collection exists
        if src[k].count() > 0:
            cnt = 0
            for coll in sorted(v)[:-keep_last]:
                coll_to_remove.append(coll)
                cnt += 1
            print("\t\t%s archived collections marked to remove." % cnt)
        else:
            print('skipped. Missing current "%s" collection!' % k)
    if len(coll_to_remove) > 0:
        print("%d archived collections will be removed." % len(coll_to_remove))
        if verbose:
            for coll in coll_to_remove:
                print('\t', coll)
        if noconfirm or ask("Continue?") == 'Y':
            for coll in coll_to_remove:
                src[coll].drop()
            print("Done.[%s collections removed]" % len(coll_to_remove))
        else:
            print("Aborted.")
    else:
        print("Nothing needs to be removed.")


def target_clean_collections(keep_last=2, target=None, verbose=True, noconfirm=False):
    '''clean up collections in target db, only keep last <keep_last> number of collections.'''
    import re
    from biothings.utils.common import ask

    target = target or get_target_db()
    coll_list = target.collection_names()

    for prefix in ('genedoc_mygene', 'genedoc_mygene_allspecies'):
        pat = prefix + '_(\d{8})_\w{8}'
        _li = []
        for coll_name in coll_list:
            mat = re.match(pat, coll_name)
            if mat:
                _li.append((mat.group(1), coll_name))
        _li.sort()   # older collection appears first
        coll_to_remove = [x[1] for x in _li[:-keep_last]]   # keep last # of newer collections
        if len(coll_to_remove) > 0:
            print('{} "{}*" collection(s) will be removed.'.format(len(coll_to_remove), prefix))
            if verbose:
                for coll in coll_to_remove:
                    print('\t', coll)
            if noconfirm or ask("Continue?") == 'Y':
                for coll in coll_to_remove:
                    target[coll].drop()
                print("Done.[%s collection(s) removed]" % len(coll_to_remove))
            else:
                print("Aborted.")
        else:
            print("Nothing needs to be removed.")


def backup_src_configs():
    import json
    import os
    from .common import get_timestamp, DateTimeJSONEncoder
    from .aws import send_s3_file

    db = get_src_db()
    for cfg in ['src_dump', 'src_master', 'src_build']:
        xli = list(db[cfg].find())
        bakfile = '/tmp/{}_{}.json'.format(cfg, get_timestamp())
        bak_f = file(bakfile, 'w')
        json.dump(xli, bak_f, cls=DateTimeJSONEncoder, indent=2)
        bak_f.close()
        bakfile_key = 'genedoc_src_config_bk/' + os.path.split(bakfile)[1]
        print('Saving to S3: "{}"... '.format(bakfile_key), end='')
        send_s3_file(bakfile, bakfile_key, overwrite=True)
        os.remove(bakfile)
        print('Done.')


def get_data_folder(src_name):
    src_dump = get_src_dump()
    src_doc = src_dump.find_one({'_id': src_name})
    if not src_doc:
        raise ValueError("Can't find any datasource information for '%s'" % src_name)
    # ensure we're not in a transient state
    assert src_doc.get("download",{}).get('status') in ['success','failed'], "Source files are not ready yet [status: \"%s\"]." % src_doc['status']
    return src_doc['data_folder']

def get_latest_build(build_name):
    src_build = get_src_build()
    doc = src_build.find_one({"_id":build_name})
    if doc and doc.get("build"):
        target = doc["build"][-1]["target_name"]
        return target
    else:
        return None

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

