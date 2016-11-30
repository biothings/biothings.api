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
                                    loadobj, get_class_from_classpath)
from biothings.utils.mongo import doc_feeder
from utils.es import ESIndexer
import biothings.databuild.backend as btbackend
from biothings.databuild.mapper import TransparentMapper


class BuilderException(Exception):
    pass
class ResumeException(Exception):
    pass


class DataBuilder(object):

    keep_archive = 10 # number of archived collection to keep. Oldest get dropped first.

    def __init__(self, build_name, source_backend, target_backend, log_folder,
                 doc_root_key="root", max_build_status=10,
                 mappers=[], default_mapper_class=TransparentMapper,
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
        self.mappers = {}
        self.timestamp = datetime.now()
        self.stats = {} # keep track of cnt per source, etc...

        for mapper in mappers + [default_mapper_class()]:
            self.mappers[mapper.name] = mapper

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

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        return {"category" : "builder",
                "source" : "%s:%s" % (self.build_name,self.target_backend.target_name),
                "step" : "",
                "description" : ""}


    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, '%s_%s_build.log' % (self.build_name,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fh.setFormatter(logging_mod.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        fh.name = "logfile"
        self.logger = logging_mod.getLogger("%s_build" % self.build_name)
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)

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

    def clean_old_collections(self):
        # use target_name is given, otherwise build name will be used 
        # as collection name prefix, so they should start like that
        prefix = "%s_" % (self.target_name or self.build_name)
        db = mongo.get_target_db()
        cols = [c for c in db.collection_names() if c.startswith(prefix)]
        # timestamp is what's after _archive_, YYYYMMDD, so we can sort it safely
        cols = sorted(cols,reverse=True)
        to_drop = cols[self.keep_archive:]
        for colname in to_drop:
            self.logger.info("Cleaning old archive collection '%s'" % colname)
            db[colname].drop()

    def init_mapper(self,mapper_name):
        if self.mappers[mapper_name].need_load():
            if mapper_name is None:
                self.logger.info("Initializing default mapper")
            else:
                self.logger.info("Initializing mapper name '%s'" % mapper_name)
            self.mappers[mapper_name].load()

    def generate_document_query(self, src_name):
        return None

    def get_root_document_sources(self):
        return self.build_config.get(self.doc_root_key,[]) or []

    def setup(self,sources=None, target_name=None):
        sources = sources or self.sources
        target_name = target_name or self.target_name
        self.target_backend.set_target_name(self.target_name, self.build_name)
        # root key is optional but if set, it must exist in build config
        if self.doc_root_key and not self.doc_root_key in self.build_config:
            raise BuilderException("Root document key '%s' can't be found in build configuration" % self.doc_root_key)

    def store_stats(self,future=None):
        self.target_backend.post_merge()
        _src_versions = self.source_backend.get_src_versions()
        self.register_status('success',build={"stats" : self.stats, "src_versions" : _src_versions})

    def resolve_sources(self,sources):
        """
        Source can be a string that may contain regex chars. It's usefull
        when you have plenty of sub-collections prefixed with a source name.
        For instance, given a source named 'blah' stored in as many collections
        as chromosomes, insteand of passing each name as 'blah_1', 'blah_2', etc...
        'blah_.*' can be specified in build_config. This method resolves potential
        regexed source name into real, existing collection names
        """
        if type(sources) == str:
            sources = [sources]
        src_db = mongo.get_src_db()
        cols = src_db.collection_names()
        found = []
        for src in sources:
            # restrict pattern to minimal match
            pat = re.compile("^%s$" % src)
            for col in cols:
                if pat.match(col):
                    found.append(col)
        return found


    def merge(self, sources=None, target_name=None, batch_size=100000, job_manager=None):
        assert job_manager
        self.t0 = time.time()
        # normalize
        if sources is None:
            self.target_backend.drop()
            self.target_backend.prepare()
            sources = self.build_config['sources']
        elif isinstance(sources,str):
            sources = [sources]

        sources = self.resolve_sources(sources)

        if target_name:
            self.target_name = target_name
            self.target_backend.set_target_name(self.target_name)

        self.clean_old_collections()
        self.logger.info("Merging into target collection '%s'" % self.target_backend.target_collection.name)
        try:

            self.register_status("building",transient=True,init=True,
                                 build={"step":"init","sources":sources})
            job = self.merge_sources(source_names=sources, batch_size=batch_size, job_manager=job_manager)
            job = asyncio.ensure_future(job)
            job.add_done_callback(self.store_stats)

            return job

        except (KeyboardInterrupt,Exception) as e:
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed",build={"err": repr(e)})
            raise

    def get_mapper_for_source(self,src_name,init=True):
        mapper_name = self.source_backend.get_src_master_docs()[src_name].get('mapper')
        # TODO: this could be a list
        try:
            init and self.init_mapper(mapper_name)
            mapper = self.mappers[mapper_name]
            self.logger.info("Found mapper '%s' for source '%s'" % (mapper,src_name))
            return mapper
        except KeyError:
            raise BuilderException("Found mapper named '%s' but no mapper associated" % mapper_name)

    @asyncio.coroutine
    def merge_sources(self, source_names, batch_size=100000, job_manager=None):
        """
        Merge resources from given source_names or from build config.
        Identify root document sources from the list to first process them.
        """
        assert job_manager
        total_docs = 0
        self.stats = {}
        # try to identify root document sources amongst the list to first
        # process them (if any)
        root_sources = list(set(source_names).intersection(set(self.get_root_document_sources())))
        other_sources = list(set(source_names).difference(set(root_sources)))
        # got root doc sources but not part of the merge ? that's weird...
        if self.get_root_document_sources() and not root_sources:
            self.logger.warning("Root document sources found (%s) for not part of the merge..." % self.get_root_document_sources())

        @asyncio.coroutine
        def merge(src_names):
            jobs = []
            for i,src_name in enumerate(src_names):
                job = self.merge_source(src_name, batch_size=batch_size, job_manager=job_manager)
                job = asyncio.ensure_future(job)
                def merged(f,stats):
                    stats.update(f.result())
                job.add_done_callback(partial(merged,stats=self.stats))
                jobs.append(job)
            yield from asyncio.wait(jobs)

        self.logger.info("Merging root document sources: %s" % root_sources)
        if root_sources:
            yield from merge(root_sources)

        self.logger.info("Merging other resources: %s" % other_sources)
        yield from merge(other_sources)

        self.logger.info("Finalizing target backend")
        self.target_backend.finalize()

        self.logger.info("Running post-merge process")
        pinfo = self.get_pinfo()
        pinfo["step"] = "post-merge"
        job_manager.defer_to_thread(pinfo,partial(self.post_merge, source_names, batch_size, job_manager))

        return self.stats

    def clean_document_to_merge(self,doc):
        return doc

    @asyncio.coroutine
    def merge_source(self, src_name, batch_size=100000, job_manager=None):
        # it's actually not optional
        assert job_manager
        _query = self.generate_document_query(src_name)
        # Note: no need to check if there's an existing document with _id (we want to merge only with an existing document)
        # if the document doesn't exist then the update() call will silently fail.
        # That being said... if no root documents, then there won't be any previously inserted
        # documents, and this update() would just do nothing. So if no root docs, then upsert
        # (update or insert, but do something)
        upsert = not self.get_root_document_sources() or src_name in self.get_root_document_sources()
        if not upsert:
            self.logger.debug("Documents from source '%s' will be stored only if a previous document exist with same _id" % src_name)
        jobs = []
        total = self.source_backend[src_name].count()
        cnt = 0
        for doc_ids in doc_feeder(self.source_backend[src_name], step=batch_size, inbatch=True, fields={'_id':1}):
            cnt += len(doc_ids)
            pinfo = self.get_pinfo()
            pinfo["step"] = src_name
            pinfo["description"] = "#docs %d/%d" % (cnt,total)
            self.logger.info("Creating merger job to process '%s' %d/%d" % (src_name,cnt,total))
            ids = [doc["_id"] for doc in doc_ids]
            job = job_manager.defer_to_process(
                    pinfo,
                    partial(merger_worker,
                        self.source_backend[src_name].name,
                        self.target_backend.target_name,
                        ids,
                        self.get_mapper_for_source(src_name,init=False),
                        upsert))
            def processed(f,cnt):
                # collect result ie. number of inserted
                try:
                    cnt += f.result()
                except Exception as e:
                    self.logger.error("Error in processed: %s" % e)
            job.add_done_callback(partial(processed,cnt=cnt))
            jobs.append(job)
        self.logger.info("%d jobs created for merging step" % len(jobs))
        yield from asyncio.wait(jobs)
        return {"total_%s" % src_name : cnt}

    def post_merge(self, source_names, batch_size, job_manager):
        pass


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

    def __init__(self,source_backend_factory=None,
                      target_backend_factory=None,
                      builder_class=None,
                      *args,**kwargs):
        """
        BuilderManager deals with the different builders used to merge datasources.
        It is connected to src_build() via sync(), where it grabs build information
        and register builder classes, ready to be instantiate when triggering builds.
        source_backend_factory can be a optional factory function (like a partial) that
        builder can call without any argument to generate a SourceBackend.
        Same for target_backend_factory for the TargetBackend. builder_class if given
        will be used as the actual Builder class used for the merge and will be passed
        same arguments as the base DataBuilder
        """
        super(BuilderManager,self).__init__(*args,**kwargs)
        self.src_build = mongo.get_src_build()
        self.source_backend_factory = source_backend_factory
        self.target_backend_factory = target_backend_factory
        self.builder_class = builder_class

    def register_builder(self,build_name):
        # will use partial to postponse object creations and their db connection
        # as we don't want to keep connection alive for undetermined amount of time
        # declare source backend
        def create(build_name):
            # postpone config import so app had time to set it up
            # before actual call time
            from biothings import config
            source_backend =  self.source_backend_factory and self.source_backend_factory() or \
                                    partial(backend.SourceDocMongoBackend,
                                            build=partial(mongo.get_src_build),
                                            master=partial(mongo.get_src_master),
                                            dump=partial(mongo.get_src_dump),
                                            sources=partial(mongo.get_src_db))

            # declare target backend
            target_backend = self.target_backend_factory and self.target_backend_factory() or \
                                    partial(TargetDocMongoBackend,
                                            target_db=partial(mongo.get_target_db))

            # assemble the whole
            klass = self.builder_class and self.builder_class or DataBuilder
            bdr = klass(
                    build_name,
                    source_backend=source_backend,
                    target_backend=target_backend,
                    log_folder=config.LOG_FOLDER)

            return bdr

        self.register[build_name] = partial(create,build_name)

    def __getitem__(self,build_name):
        """
        Return an instance of a builder for the build named 'build_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        # we'll get a partial class but will return an instance
        pclass = BaseManager.__getitem__(self,build_name)
        return pclass()

    def sync(self):
        """Sync with src_build and register all build config"""
        for conf in self.src_build.find():
            self.register_builder(conf["_id"])

    def merge(self,build_name,sources=None,target_name=None):
        """
        Trigger a merge for build named 'build_name'. Optional list of sources can be
        passed (one single or a list). target_name is the target collection name used
        to store to merge data. If none, each call will generate a unique target_name.
        """
        try:
            bdr = self[build_name]
            job = bdr.merge(sources,target_name,job_manager=self.job_manager)
            return job
        except KeyError as e:
            raise BuilderException("No such builder for '%s'" % build_name)

    def list_sources(self,build_name):
        """
        List all registered sources used to trigger a build named 'build_name'
        """
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

