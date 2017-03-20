
import sys, re, os, time, math
from datetime import datetime
import pickle
from pprint import pformat
import asyncio
from functools import partial

import biothings.utils.mongo as mongo
from biothings.utils.loggers import HipchatHandler, get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.es import ESIndexer
from biothings import config as btconfig
from biothings.utils.mongo import doc_feeder, id_feeder
from config import LOG_FOLDER, logger as logging


class IndexerException(Exception):
    pass


class IndexerManager(BaseManager):

    def __init__(self, pindexer, *args, **kwargs):
        super(IndexerManager,self).__init__(*args, **kwargs)
        self.pindexer = pindexer
        self.src_build = mongo.get_src_build()
        self.target_db = mongo.get_target_db()
        self.t0 = time.time()
        self.prepared = False
        self.setup()

    def setup(self):
        self.setup_log()

    def setup_log(self):
        self.logger = btconfig.logger

    def __getitem__(self,build_name):
        """
        Return an instance of an indexer for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self,build_name)
        return pclass()

    def sync(self):
        """Sync with src_build and register all build config"""
        for conf in self.src_build.find():
            self.register_indexer(conf)

    def register_indexer(self, conf):
        def create(conf):
            idxer = self.pindexer(build_name=conf["_id"])
            return idxer
        self.register[conf["_id"]] = partial(create,conf)

    def index(self, build_name, target_name=None, index_name=None, ids=None, **kwargs):
        """
        Trigger a merge for build named 'build_name'. Optional list of sources can be
        passed (one single or a list). target_name is the target collection name used
        to store to merge data. If none, each call will generate a unique target_name.
        """
        t0 = time.time()
        def indexed(f):
            t1 = timesofar(t0)
            try:
                self.logger.info("Done indexing target '%s' to index '%s' (%s)" % (target_name,index_name,t1))
            except Exception as e:
                import traceback
                self.logger.error("Error while running merge job, %s:\n%s" % (e,traceback.format_exc()))
                raise
        try:
            idx = self[build_name]
            idx.target_name = target_name
            job = idx.index(target_name, index_name, ids=ids, job_manager=self.job_manager, **kwargs)
            job = asyncio.ensure_future(job)
            job.add_done_callback(indexed)
            return job
        except KeyError as e:
            raise IndexerException("No such builder for '%s'" % build_name)

class Indexer(object):

    def __init__(self, build_name, es_host, target_name=None):
        self.host = es_host
        self.log_folder = LOG_FOLDER
        self.timestamp = datetime.now()
        self.build_name = build_name
        self.target_name = None
        self.index_name = None
        self.doc_type = None
        self.num_shards = None
        self.load_build_config(build_name)

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        return {"category" : "indexer",
                "source" : "%s:%s" % (self.build_name,self.index_name),
                "step" : "",
                "description" : ""}

    @asyncio.coroutine
    def index(self, target_name, index_name, job_manager, batch_size=10000, ids=None, mode=None):
        """
        Build an index named "index_name" with data from collection
        "target_collection". "ids" can be passed to selectively index documents. "mode" can have the following
        values:
        - 'purge': will delete index if it exists
        - 'add': will use existing index and add documents (usually usefull with "ids" param to complete
                 an existing index)
        - None (default): will create a new index, assuming it doesn't already exist
        """
        self.target_name = target_name
        self.index_name = index_name
        self.setup_log()
        _db = mongo.get_target_db()
        target_collection = _db[target_name]
        _mapping = self.get_mapping()
        _extra = self.get_index_creation_settings()
        _meta = {}
        # partially instantiated indexer instance for process workers
        partial_idxer = partial(ESIndexer,doc_type=self.doc_type,
                             index=index_name,
                             es_host=self.host,
                             step=batch_size,
                             number_of_shards=self.num_shards)
        # instantiate one here for index creation
        es_idxer = partial_idxer()
        if es_idxer.exists_index():
            if mode == "purge":
                es_idxer.delete_index()
            elif mode != "add":
                raise IndexerException("Index already '%s' exists, (use mode='purge' to auto-delete it or mode='add' to add more documents)" % index_name)

        if mode != "add":
            es_idxer.create_index({self.doc_type:_mapping},_extra)

        got_error = False
        jobs = []
        total = target_collection.count()
        btotal = math.ceil(total/batch_size) 
        bnum = 1
        cnt = 0
        if ids:
            self.logger.info("Indexing from '%s' with specific list of _ids, create indexer job with batch_size=%d" % (target_name, batch_size))
            id_provider = [ids]
        else:
            self.logger.info("Fetch _ids from '%s', and create indexer job with batch_size=%d" % (target_name, batch_size))
            id_provider = id_feeder(target_collection, batch_size=batch_size)
        for ids in id_provider:
            yield from asyncio.sleep(0.0)
            cnt += len(ids)
            pinfo = self.get_pinfo()
            pinfo["step"] = self.target_name
            pinfo["description"] = "#%d/%d (%.1f%%)" % (bnum,btotal,(cnt/total*100))
            self.logger.info("Creating indexer job #%d/%d, to index '%s' %d/%d (%.1f%%)" % \
                    (bnum,btotal,target_name,cnt,total,(cnt/total*100.)))
            job = yield from job_manager.defer_to_process(
                    pinfo,
                    partial(indexer_worker,
                        self.target_name,
                        ids,
                        partial_idxer,
                        bnum))
            def batch_indexed(f,batch_num):
                nonlocal got_error
                res = f.result()
                if type(res) != tuple or type(res[0]) != int:
                    got_error = Exception("Batch #%s failed while indexing collection '%s' [%s]" % (batch_num,self.target_name,f.result()))
            job.add_done_callback(partial(batch_indexed,batch_num=bnum))
            jobs.append(job)
            bnum += 1
            # raise error as soon as we know
            if got_error:
                raise got_error
        self.logger.info("%d jobs created for indexing step" % len(jobs))
        tasks = asyncio.gather(*jobs)
        def done(f):
            nonlocal got_error
            if None in f.result():
                got_error = Exception("Some batches failed")
                return
            # compute overall inserted/updated records
            # returned values looks like [(num,[]),(num,[]),...]
            cnt = sum([val[0] for val in f.result()])
            self.logger.info("Index '%s' successfully created" % index_name)
        tasks.add_done_callback(done)
        yield from tasks
        if got_error:
            raise got_error
        else:
            return {"total_%s" % self.target_name : cnt}


        es_idxer.build_index(target_collection, verbose=True)
        if es_idxer.wait_till_all_shards_ready():
            self.logger.info("Optimizing ES index...")
            es_idxer.optimize()

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, '%s_%s_index.log' % (self.index_name,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("%s_index" % self.build_name)
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not nh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(nh)
        return self.logger

    def get_index_creation_settings(self):
        """
        Override to return a dict containing some extra settings
        for index creation. Dict will be merged with mandatory settings, 
        see biothings.utils.es.ESIndexer.create_index for more.
        """
        return {}

    def get_mapping(self, enable_timestamp=True):
        '''collect mapping data from data sources.
           This is for GeneDocESBackend only.
        '''
        mapping = {}
        src_master = mongo.get_src_master()
        for collection in self.build_config['sources']:
            meta = src_master.find_one({"_id" : collection})
            if 'mapping' in meta and meta["mapping"]:
                mapping.update(meta['mapping'])
            else:
                self.logger.info('Warning: "%s" collection has no mapping data.' % collection)
        mapping = {"properties": mapping,
                   "dynamic": "false"}
        if enable_timestamp:
            mapping['_timestamp'] = {
                "enabled": True,
            }
        mapping["_meta"] = self.get_metadata()
        return mapping

    def get_metadata(self):
        stats = self.get_stats()
        versions = self.get_src_versions()
        timestamp = self.get_timestamp()
        return {"stats": stats,
                "src_version": versions,
                "timestamp": timestamp}

    def get_builds(self,target_name=None):
        target_name = target_name or self.target_name
        assert target_name, "target_name must be defined first before searching for builds"
        # try to find all build informations
        builds = [b for b in self.build_config["build"] if b["target_name"] == target_name]
        assert len(builds) > 0, "Can't find build for config '%s' and target_name '%s'" % (self.build_name,self.target_name)
        return builds

    def get_src_versions(self):
        # target (merged collection) could have been created in multiple steps
        builds = self.get_builds()
        src_version = {}
        # builds are sorted chronologically by default
        for build in builds:
            if not "src_version" in build:
                continue
            for src in build["src_version"]:
                src_version[src] = build["src_version"][src]
        if not src_version:
            raise IndexerException("Build has no source versions, can't index")
        return src_version

    def get_stats(self):
        builds = self.get_builds()
        stats = {}
        # builds are sorted chronologically by default
        for build in builds:
            if not "stats" in build:
                continue
            for stat in build["stats"]:
                stats[stat] = build["stats"][stat]
        if not stats:
            pass
            #raise IndexerException("Build has no stats, can't index")
        return stats

    def get_timestamp(self):
        # we'll keep the latest one
        build = self.get_builds()[-1]
        return build["started_at"]

    def load_build_config(self, build):
        '''Load build config from src_build collection.'''
        src_build = mongo.get_src_build()
        _cfg = src_build.find_one({'_id': build})
        if _cfg:
            self.build_config = _cfg
            if not "doc_type" in _cfg:
                raise ValueError("Missing 'doc_type' in build config")
            self.doc_type = _cfg["doc_type"]
            self.num_shards = _cfg.get("num_shards") # optional
            self.num_shards = self.num_shards and int(self.num_shards) or self.num_shards
        else:
            raise ValueError('Cannot find build config named "%s"' % build)
        return _cfg


def indexer_worker(col_name,ids,pindexer,batch_num):
    try:
        tgt = mongo.get_target_db()
        col = tgt[col_name]
        idxer = pindexer()
        cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}})
        cnt = idxer.index_bulk(cur)
        return cnt
    except Exception as e:
        logger_name = "index_%s_%s_batch_%s" % (pindexer.keywords.get("index","index"),col_name,batch_num)
        logger = get_logger(logger_name, btconfig.LOG_FOLDER)
        logger.exception(e)
        exc_fn = os.path.join(btconfig.LOG_FOLDER,"%s.pick" % logger_name)
        pickle.dump({"exc":e,"ids":ids},open(exc_fn,"wb"))
        logger.info("Exception and IDs were dumped in pickle file '%s'" % exc_fn)
        raise
