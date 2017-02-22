import sys, re, os, time
from datetime import datetime
from pprint import pformat
import asyncio
from functools import partial

import biothings.utils.mongo as mongo
from biothings.utils.loggers import HipchatHandler
from biothings.utils.manager import BaseManager
from biothings.utils.es import ESIndexer
from biothings import config as btconfig
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
            idxer = self.pindexer()
            return idxer

        self.register[conf["_id"]] = partial(create,conf)

    def index(self, build_name, target_name=None, index_name=None, **kwargs):
        """
        Trigger a merge for build named 'build_name'. Optional list of sources can be
        passed (one single or a list). target_name is the target collection name used
        to store to merge data. If none, each call will generate a unique target_name.
        """
        def indexed(f):
            try:
                pass
            except Exception as e:
                import traceback
                self.logger.error("Error while running merge job, %s:\n%s" % (e,traceback.format_exc()))
                raise
        try:
            idx = self[build_name]
            job = idx.index(build_name, target_name, index_name, job_manager=self.job_manager, **kwargs)
            job.add_done_callback(indexed)
            return job
        except KeyError as e:
            raise IndexerException("No such builder for '%s'" % build_name)

class Indexer(object):

    def __init__(self, host):
        self.host = host
        self.log_folder = LOG_FOLDER
        self.timestamp = datetime.now()

    def index(self, build_name, target_name, index_name, job_manager, purge=False):
        self.target_name = target_name
        self.index_name = index_name
        self.build_name = build_name
        self.load_build_config(build_name)
        self.setup_log()
        _db = mongo.get_target_db()
        target_collection = _db[target_name]
        _mapping = self.get_mapping()
        _meta = {}
        #src_version = self.get_src_version()
        #if src_version:
        #    _meta['src_version'] = src_version
        #if getattr(self, '_stats', None):
        #    _meta['stats'] = self._stats
        #if 'timestamp' in last_build:
        #    _meta['timestamp'] = last_build['timestamp']
        #if _meta:
        #    _mapping['_meta'] = _meta
        es_idxer = ESIndexer(doc_type="variant",
                             index=index_name,
                             es_host=self.host,
                             step=10000)
        #if build_config == 'mygene_allspecies':
        #    es_idxer.number_of_shards = 10   # default 5
        es_idxer.check()
        #es_idxer.s = 609000
        if es_idxer.exists_index():
            if purge:
                es_idxer.delete_index()
            else:
                raise IndexerException("Index already '%s' exists, (use 'purge=True' to auto-delete it)" % index_name)

        #for k in ['dbnsfp', 'clinvar', 'vcf', 'evs', 'dbsnp', 'hg19', 'snpeff']:
        #    _mapping["properties"].pop(k)
        #_mapping.pop("_timestamp")
        #_mapping.pop("properties")
        #_mapping.pop("dynamic")
        print(pformat(_mapping))
        es_idxer.create_index({"variant":_mapping})
        #es_idxer.delete_index_type(es_idxer.ES_INDEX_TYPE, noconfirm=True)
        es_idxer.build_index(target_collection, verbose=True)
        # time.sleep(10)    # pausing 10 second here
        # if es_idxer.wait_till_all_shards_ready():
        #     print "Optimizing...", es_idxer.optimize()

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

    def get_mapping(self, enable_timestamp=True):
        '''collect mapping data from data sources.
           This is for GeneDocESBackend only.
        '''
        mapping = {}
        src_master = mongo.get_src_master()
        for collection in self.build_config['sources']:
            meta = src_master.find_one({"_id" : collection})
            if 'mapping' in meta:
                mapping.update(meta['mapping'])
            else:
                logging.info('Warning: "%s" collection has no mapping data.' % collection)
        mapping = {"properties": mapping,
                   "dynamic": "false"}
        if enable_timestamp:
            mapping['_timestamp'] = {
                "enabled": True,
            }
        #allow source Compression
        #Note: no need of source compression due to "Store Level Compression"
        #mapping['_source'] = {'compress': True,}
        #                      'compress_threshold': '1kb'}
        return mapping

    def load_build_config(self, build):
        '''Load build config from src_build collection.'''
        src_build = mongo.get_src_build()
        _cfg = src_build.find_one({'_id': build})
        if _cfg:
            self.build_config = _cfg
        else:
            raise ValueError('Cannot find build config named "%s"' % build)
        return _cfg

    #def build_index2(self, build_config='mygene_allspecies', last_build_idx=-1, use_parallel=False, es_host=None, es_index_name=None, noconfirm=False):
    #    """Build ES index from last successfully-merged mongodb collection.
    #        optional "es_host" argument can be used to specified another ES host, otherwise default ES_HOST.
    #        optional "es_index_name" argument can be used to pass an alternative index name, otherwise same as mongodb collection name
    #    """
    #    self.load_build_config(build_config)
    #    assert "build" in self._build_config, "Abort. No such build records for config %s" % build_config
    #    last_build = self._build_config['build'][last_build_idx]
    #    logging.info("Last build record:")
    #    logging.info(pformat(last_build))
    #    assert last_build['status'] == 'success', \
    #        "Abort. Last build did not success."
    #    assert last_build['target_backend'] == "mongodb", \
    #        'Abort. Last build need to be built using "mongodb" backend.'
    #    assert last_build.get('stats', None), \
    #        'Abort. Last build stats are not available.'
    #    self._stats = last_build['stats']
    #    assert last_build.get('target', None), \
    #        'Abort. Last build target_collection is not available.'

    #    # Get the source collection to build the ES index
    #    # IMPORTANT: the collection in last_build['target'] does not contain _timestamp field,
    #    #            only the "genedoc_*_current" collection does. When "timestamp" is enabled
    #    #            in mappings, last_build['target'] collection won't be indexed by ES correctly,
    #    #            therefore, we use "genedoc_*_current" collection as the source here:
    #    #target_collection = last_build['target']
    #    target_collection = "genedoc_{}_current".format(build_config)
    #    _db = get_target_db()
    #    target_collection = _db[target_collection]
    #    logging.info("")
    #    logging.info('Source: %s' % target_collection.name)
    #    _mapping = self.get_mapping()
    #    _meta = {}
    #    src_version = self.get_src_version()
    #    if src_version:
    #        _meta['src_version'] = src_version
    #    if getattr(self, '_stats', None):
    #        _meta['stats'] = self._stats
    #    if 'timestamp' in last_build:
    #        _meta['timestamp'] = last_build['timestamp']
    #    if _meta:
    #        _mapping['_meta'] = _meta
    #    es_index_name = es_index_name or target_collection.name
    #    es_idxer = ESIndexer(mapping=_mapping,
    #                         es_index_name=es_index_name,
    #                         es_host=es_host,
    #                         step=5000)
    #    if build_config == 'mygene_allspecies':
    #        es_idxer.number_of_shards = 10   # default 5
    #    es_idxer.check()
    #    if noconfirm or ask("Continue to build ES index?") == 'Y':
    #        es_idxer.use_parallel = use_parallel
    #        #es_idxer.s = 609000
    #        if es_idxer.exists_index(es_idxer.ES_INDEX_NAME):
    #            if noconfirm or ask('Index "{}" exists. Delete?'.format(es_idxer.ES_INDEX_NAME)) == 'Y':
    #                es_idxer.conn.indices.delete(es_idxer.ES_INDEX_NAME)
    #            else:
    #                logging.info("Abort.")
    #                return
    #        es_idxer.create_index()
    #        #es_idxer.delete_index_type(es_idxer.ES_INDEX_TYPE, noconfirm=True)
    #        es_idxer.build_index(target_collection, verbose=False)
    #        # time.sleep(10)    # pausing 10 second here
    #        # if es_idxer.wait_till_all_shards_ready():
    #        #     print "Optimizing...", es_idxer.optimize()

