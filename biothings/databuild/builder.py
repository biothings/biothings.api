import sys, re
import os.path
import time
import copy
import importlib
from datetime import datetime
from pprint import pformat
import logging
import asyncio
from functools import partial

from biothings.utils.common import (timesofar, ask, safewfile,
                                    dump2gridfs, get_timestamp, get_random_string,
                                    setup_logfile, loadobj, get_class_from_classpath)
from biothings.utils.mongo import doc_feeder
from utils.es import ESIndexer
import biothings.databuild.backend as btbackend
from biothings.databuild.mapper import TransparentMapper


class BuilderException(Exception):
    pass


class DataBuilder(object):

    def __init__(self, build_name, source_backend, target_backend, log_folder,
                 doc_root_key=None, max_build_status=10, loop=None,
                 id_mappers=[], default_mapper_class=TransparentMapper,
                 sources=None, target_name=None,**kwargs):
        self.init_state()
        self.build_name = build_name
        self.sources = sources
        self.target_name = target_name
        if type(source_backend) == partial:
            self._partial_source_backend = source_backend
        else:
            self._state["source_backend"] = source_backend
        if type(target_backend) == partial:
            self._partial_target_backend = target_backend
        else:
            self._state["target_backend"] = target_backend
        self.doc_root_key = doc_root_key
        self.t0 = time.time()
        self.logfile = None
        self.log_folder = log_folder
        self.id_mappers = {}
        self.timestamp = datetime.now()
        self.loop = loop

        for mapper in id_mappers + [default_mapper_class()]:
            self.id_mappers[mapper.name] = mapper

        self.step = kwargs.get("step",10000)
        # max no. of records kept in "build" field of src_build collection.
        self.max_build_status = max_build_status
        self.prepared = False

    def init_state(self):
        self._state = {
                "logger" : None,
                "source_backend" : None,
                "target_backend" : None,
                "build_config" : None,
        }
    @property
    def logger(self):
        if not self._state["logger"]:
            self.prepare()
        return self._state["logger"]
    @property
    def source_backend(self):
        if self._state["source_backend"] is None:
            self.prepare()
            self._state["build_config"] = self._state["source_backend"].get_build_configuration(self.build_name)
            self._state["source_backend"].validate_sources(self.sources)
        return self._state["source_backend"]
    @property
    def target_backend(self):
        if self._state["target_backend"] is None:
            self.prepare()
        return self._state["target_backend"]
    @property
    def build_config(self):
        self.prepare()
        self._state["build_config"] = self.source_backend.get_build_configuration(self.build_name)
        return self._state["build_config"]
    @logger.setter
    def logger(self, value):
        self._state["logger"] = value
    @build_config.setter
    def build_config(self, value):
        self._state["build_config"] = value

    def prepare(self,state={}):
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self._state["source_backend"] = self._partial_source_backend()
        self._state["target_backend"] = self._partial_target_backend()
        self.setup()
        self.setup_log()

    def unprepare(self):
        """
        reset anything that's not pickable (so self can be pickled)
        return what's been reset as a dict, so self can be restored
        once pickled
        """
        # TODO: use copy ?
        state = {
            "logger" : self._state["logger"],
            "source_backend" : self._state["source_backend"],
            "target_backend" : self._state["target_backend"],
            "build_config" : self._state["build_config"],
        }
        for k in state:
            self._state[k] = None
        self.prepared = False
        return state

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, '%s_%s_build.log' % (self.build_name,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fh.setFormatter(logging_mod.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        fh.name = "logfile"
        sh = logging_mod.StreamHandler()
        sh.name = "logstream"
        self.logger = logging_mod.getLogger("%s_build" % self.build_name)
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not sh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(sh)

    def register_status(self,status,transient=False,init=False,**extra):
        assert self.build_config, "build_config needs to be specified first"
        # get it from source_backend, kind of weird...
        src_build = self.source_backend.build
        build_info = {
             'status': status,
             'started_at': datetime.now(),
             'logfile': self.logfile,
             'target_backend': self.target_backend.name,
             'target_name': self.target_backend.target_name}
        if transient:
            # record some "in-progress" information
            build_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            t1 = round(time.time() - self.t0, 0)
            build_info["time"] = timesofar(self.t0)
            build_info["time_in_s"] = t1
        # merge extra at root or "build" level
        # (to keep building data...)
        # it also means we want to merge the last one in "build" list
        _cfg = src_build.find_one({'_id': self.build_config['_id']})
        if "build" in extra:
            build_info.update(extra["build"])
            _cfg["build"][-1].update(build_info)
            src_build.replace_one({'_id': self.build_config['_id']},_cfg)
        # create a new build entre at the end and clean extra one (not needed/wanted)
        if init:
            src_build.update({'_id': self.build_config['_id']}, {"$push": {'build': build_info}})
            if len(_cfg['build']) > self.max_build_status:
                howmany = len(_cfg['build']) - self.max_build_status
                #remove any status not needed anymore
                for _ in range(howmany):
                    src_build.update({'_id': self.build_config['_id']}, {"$pop": {'build': -1}})

    def init_mapper(self,id_type):
        if self.id_mappers[id_type].need_load():
            if id_type is None:
                self.logger.info("Initializing default mapper")
            else:
                self.logger.info("Initializing mapper '%s'" % id_type)
            self.id_mappers[id_type].load()

    def generate_document_query(self, src_name):
        return None

    def get_root_document_sources(self):
        return self.build_config.get(self.doc_root_key,[])

    def setup(self,sources=None, target_name=None):
        sources = sources or self.sources
        target_name = target_name or self.target_name
        self.target_backend.set_target_name(self.target_name, self.build_name)
        # root key is optional but if set, it must exist in build config
        if self.doc_root_key and not self.doc_root_key in self.build_config:
            raise BuilderException("Root document key '%s' can't be found in build configuration" % self.doc_root_key)

    def merge(self, sources=None, target_name=None, batch_size=100000, loop=None):
        loop = loop and loop or asyncio.get_event_loop()
        self.t0 = time.time()
        # normalize
        if sources is None:
            self.target_backend.drop()
            self.target_backend.prepare()
            sources = self.build_config['sources']
        elif isinstance(sources,str):
            sources = [sources]

        if target_name:
            self.target_name = target_name
            self.target_backend.set_target_name(self.target_name)

        self.logger.info("Merging into target collection '%s'" % self.target_backend.target_collection.name)
        try:
            self.register_status("building",transient=True,init=True,
                                 build={"sources":sources})
            job = self.merge_sources(source_names=sources, batch_size=batch_size)
            job = asyncio.ensure_future(job)
            def merged(f):
                stats = f.result()
                self.target_backend.post_merge()
                _src_versions = self.source_backend.get_src_versions()
                self.register_status('success',build={"stats" : stats, "src_versions" : _src_versions})
            job.add_done_callback(merged)

        except (KeyboardInterrupt,Exception) as e:
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed",build={"err": repr(e)})
            raise

        finally:
            #do a simple validation here
            if getattr(self, '_stats', None):
                self.logger.info("Validating...")
                target_cnt = self.target_backend.count()
                if target_cnt == self._stats['total_documents']:
                    self.logger.info("OK [total count={}]".format(target_cnt))
                else:
                    self.logger.info("Warning: total count of gene documents does not match [{}, should be {}]".format(target_cnt, self._stats['total_genes']))

    def get_mapper_for_source(self,src_name,init=True):
        id_type = self.source_backend.get_src_master_docs()[src_name].get('id_type')
        try:
            init and self.init_mapper(id_type)
            mapper = self.id_mappers[id_type]
            self.logger.info("Found mapper '%s' for source '%s'" % (mapper,src_name))
            return mapper
        except KeyError:
            raise BuilderException("Found id_type '%s' but no mapper associated" % id_type)

    @asyncio.coroutine
    def merge_sources(self, source_names, batch_size=100000, loop=None):
        """
        Merge resources from given source_names or from build config.
        Identify root document sources from the list to first process them.
        """
        loop = loop and loop or asyncio.get_event_loop()
        total_docs = 0
        _stats = {}
        # try to identify root document sources amongst the list to first
        # process them (if any)
        root_sources = list(set(source_names).intersection(set(self.get_root_document_sources())))
        other_sources = list(set(source_names).difference(set(root_sources)))
        # now re-order
        source_names = root_sources + other_sources

        self.logger.info("Merging following sources: %s" % repr(source_names))

        jobs = []
        for i,src_name in enumerate(source_names):
            #if src_name in self.build_config.get(self.doc_root_key,[]):
            #    continue
            progress = "%s/%s" % (i+1,len(source_names))
            job = self.merge_source(src_name, batch_size=batch_size)
            job = asyncio.ensure_future(job)
            def merged(f,stats):
                self.logger.info("f: %s" % f.result())
                stats.update(f.result())
            job.add_done_callback(partial(merged,stats=_stats))
            jobs.append(job)
        yield from asyncio.wait(jobs)
        self.target_backend.finalize()

        return _stats

    def clean_document_to_merge(self,doc):
        return doc

    @asyncio.coroutine
    def merge_source(self, src_name, batch_size=100000):
        _query = self.generate_document_query(src_name)
        # Note: no need to check if there's an existing document with _id (we want to merge only with an existing document)
        # if the document doesn't exist then the update() call will silently fail.
        # That being said... if no root documents, then there won't be any previously inserted
        # documents, and this update() would just do nothing. So if no root docs, then upsert
        # (update or insert, but do something)
        upsert = (self.doc_root_key is None) or src_name in self.get_root_document_sources()
        if not upsert:
            self.logger.debug("Documents from source '%s' will be stored only if a previous document exist with same _id" % src_name)
        jobs = []
        total = self.source_backend[src_name].count()
        cnt = 0
        for doc_ids in doc_feeder(self.source_backend[src_name], step=batch_size, inbatch=True, fields={'_id':1}):
            cnt += len(doc_ids)
            self.logger.info("Creating merger job to process '%s' %d/%d" % (src_name,cnt,total))
            ids = [doc["_id"] for doc in doc_ids]
            job = self.loop.run_in_executor(None,
                    partial(merger_worker,
                        self.source_backend[src_name].name,
                        self.target_backend.target_name,
                        ids,
                        self.get_mapper_for_source(src_name,init=False),
                        upsert))
            def processed(f,cnt):
                # collect result ie. number of inserted
                cnt += f.result()
            job.add_done_callback(partial(processed,cnt=cnt))
            jobs.append(job)
        self.logger.info("%d jobs created for merging step" % len(jobs))
        yield from asyncio.wait(jobs)
        return {"total_%s" % src_name : cnt}


from biothings.utils.backend import DocMongoBackend
import biothings.utils.mongo as mongo

def merger_worker(col_name,dest_name,ids,mapper,upsert):
    src = mongo.get_src_db()
    tgt = mongo.get_target_db()
    col = src[col_name]
    dest = DocMongoBackend(tgt,tgt[dest_name])

    cnt = 0
    cur = col.find({'_id': {'$in': ids}})
    mapper.load()
    docs = mapper.process(cur)
    cnt += dest.update(docs, upsert=upsert)
    return cnt


from biothings.utils.manager import BaseManager
import biothings.utils.mongo as mongo
import biothings.databuild.backend as backend
from biothings.databuild.backend import TargetDocMongoBackend
from biothings.databuild.builder import DataBuilder

class BuilderManager(BaseManager):

    def __init__(self,*args,**kwargs):
        super(BuilderManager,self).__init__(*args,**kwargs)
        self.src_build = mongo.get_src_build()

    def register_builder(self,build_name):
        # will use partial to postponse object creations and their db connection
        # as we don't want to keep connection alive for undetermined amount of time
        # declare source backend
        def create(build_name):
            # postpone config import so app had time to set it up
            # before actual call time
            from biothings import config
            source_backend =  partial(backend.SourceDocMongoBackend,
                                    build=partial(mongo.get_src_build),
                                    master=partial(mongo.get_src_master),
                                    dump=partial(mongo.get_src_dump),
                                    sources=partial(mongo.get_src_db))

            # declare target backend
            target_backend = partial(TargetDocMongoBackend,target_db=partial(mongo.get_target_db))

            # assemble the whole
            bdr = DataBuilder(
                    build_name,
                    doc_root_key=None, # there's no root document concept in myvariant
                                       # _id are already normalized in individual collections
                    source_backend=source_backend,
                    target_backend=target_backend,
                    log_folder=config.LOG_FOLDER,
                    loop=self.loop)
            return bdr
        self.register[build_name] = partial(create,build_name)

    def __getitem__(self,build_name):
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self,build_name)
        return pclass()

    def sync(self):
        """Sync with src_build and register all build config"""
        for conf in self.src_build.find():
            self.register_builder(conf["_id"])

    def merge(self,build_name,sources=None,target_name=None):
        try:
            bdr = self[build_name]
            job = bdr.merge(sources,target_name,loop=self.loop)
            return job
        except KeyError as e:
            raise BuilderException("No such builder for '%s'" % build_name)

    def list_sources(self,build_name):
        info = self.src_build.find_one({"_id":build_name})
        return info and info["sources"] or []

    def clean_temp_collections(self,build_name,date=None,prefix=''):
        """
        Delete all target collections created from builder named
        "build_name" at given date (or any date is none given -- carefull...).
        Date is a string (YYYYMMDD or regex)
        Common collection name prefix can also be specified if needed.
        """
        target_db = mongo.get_target_db()
        for col_name in target_db.collection_names():
            search = prefix and prefix + "_" or ""
            search += build_name + '_'
            search += date and date + '_' or ''
            pat = re.compile(search)
            if pat.match(col_name) and not 'current' in col_name:
                logging.info("Dropping target collection '%s" % col_name)
                target_db[col_name].drop()

