import sys, re, math
import os
import time
import copy
import importlib
from datetime import datetime
from pprint import pformat
import logging
import asyncio
from functools import partial
import glob, random

from biothings.utils.common import timesofar, iter_n, get_timestamp, \
                                   dump, rmdashfr, loadobj
from biothings.utils.mongo import doc_feeder
from biothings.utils.loggers import get_logger, HipchatHandler
from biothings.utils.diff import diff_docs_jsonpatch
import biothings.databuild.backend as btbackend
from biothings.databuild.mapper import TransparentMapper
from biothings import config as btconfig


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
        # doc_root_key is a key name within src_build doc.
        # it's a list of datasources that are able to create a document
        # even it doesn't exist. It root documents list is not empty, then
        # any other datasources not listed there won't be able to create
        # a document, only will they able to update it.
        # If no root documets, any datasources can create/update a doc
        # and thus there's no priority nor limitations
        # note: negations can be used, like "!source1". meaning source1 is not
        # root document datasource.
        # Usefull to express; "all resources except source1"
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
        self.prepared = True

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
        # TODO: use bt.utils.loggers.get_logger
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, '%s_%s_build.log' % (self.build_name,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("%s_build" % self.build_name)
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not nh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(nh)
        return self.logger

    def register_status(self,status,transient=False,init=False,**extra):
        assert self.build_config, "build_config needs to be specified first"
        # get it from source_backend, kind of weird...
        src_build = self.source_backend.build
        build_info = {
            'status': status,
            'step_started_at': datetime.now(),
            'started_at' : datetime.fromtimestamp(self.t0),
            'logfile': self.logfile,
            'target_backend': self.target_backend.name,
            'target_name': self.target_backend.target_name}
        if transient:
            # record some "in-progress" information
            build_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            build_info["time"] = timesofar(self.t0)
            t1 = round(time.time() - self.t0, 0)
            build_info["time_in_s"] = t1
        if "build" in extra:
            build_info.update(extra["build"])
        # create a new build entry at the end and clean extra one (not needed/wanted)
        _cfg = src_build.find_one({'_id': self.build_config['_id']})
        if init:
            if not "build" in _cfg:
                # no build entrey yet, need to init the list
                src_build.update({'_id': self.build_config['_id']}, {"$set": {'build': [build_info]}})
            else:
                src_build.update({'_id': self.build_config['_id']}, {"$push": {'build': build_info}})
            if len(_cfg['build']) > self.max_build_status:
                howmany = len(_cfg['build']) - self.max_build_status
                #remove any status not needed anymore
                for _ in range(howmany):
                    # pop previous build starting from oldest ones
                    src_build.update({'_id': self.build_config['_id']}, {"$pop": {'build': -1}})
        else:
            # merge extra at root or "build" level
            # (to keep building data...) and update the last one
            # (it's been properly created before when init=True)
            _cfg["build"][-1].update(build_info)
            src_build.replace_one({'_id': self.build_config['_id']},_cfg)

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
        root_srcs = self.build_config.get(self.doc_root_key,[]) or []
        # check for "not this resource" and adjust the list
        none_root_srcs = [src.replace("!","") for src in root_srcs if src.startswith("!")]
        if none_root_srcs:
            if len(none_root_srcs) != len(root_srcs):
                raise BuilderException("If using '!' operator, all datasources must use it (cannot mix), got: %s" % \
                        (repr(root_srcs)))
            # ok, grab sources for this build, 
            srcs = self.build_config.get("sources",[])
            root_srcs = list(set(srcs).difference(set(none_root_srcs)))
            #self.logger.info("'except root' sources %s resolves to root source = %s" % (repr(none_root_srcs),root_srcs))

        # resolve possible regex based source name (split-collections sources)
        root_srcs = self.resolve_sources(root_srcs)
        return root_srcs

    def setup(self,sources=None, target_name=None):
        sources = sources or self.sources
        target_name = target_name or self.target_name
        self.target_backend.set_target_name(self.target_name, self.build_name)
        # root key is optional but if set, it must exist in build config
        if self.doc_root_key and not self.doc_root_key in self.build_config:
            raise BuilderException("Root document key '%s' can't be found in build configuration" % self.doc_root_key)

    def store_stats(self,f):
        try:
            self.target_backend.post_merge()
            _src_versions = self.source_backend.get_src_versions()
            self.register_status('success',build={"stats" : self.stats, "src_versions" : _src_versions})
            self.logger.info("success\nstats: %s\nversions: %s" % (self.stats,_src_versions),extra={"notify":True})
        except Exception as e:
            self.register_status("failed",build={"err": repr(e)})
            self.logger.exception("failed: %s" % e,extra={"notify":True})
            raise

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
        masters = self.source_backend.get_src_master_docs()
        found = []
        for src in sources:
            # check if master _id and name are different (meaning name is a regex)
            master = masters.get(src)
            if not master:
                raise BuilderException("'%s'could not be found in master documents (%s)" % \
                        (src,repr(list(masters.keys()))))
            search = src
            if master["_id"] != master["name"]:
                search = master["name"]
            # restrict pattern to minimal match
            pat = re.compile("^%s$" % search)
            for col in cols:
                if pat.match(col):
                    found.append(col)
        return found

    def merge(self, sources=None, target_name=None, job_manager=None, *args,**kwargs):
        assert job_manager
        self.t0 = time.time()
        # normalize
        avail_sources = self.build_config['sources']
        if sources is None:
            self.target_backend.drop()
            self.target_backend.prepare()
            sources = avail_sources # merge all
        elif isinstance(sources,str):
            sources = [sources]

        orig_sources = sources
        sources = self.resolve_sources(sources)
        if not sources:
            raise BuilderException("No source found, got %s while available sources are: %s" % \
                    (repr(orig_sources),repr(avail_sources)))
        if target_name:
            self.target_name = target_name
            self.target_backend.set_target_name(self.target_name)
        self.clean_old_collections()

        self.logger.info("Merging into target collection '%s'" % self.target_backend.target_collection.name)
        try:
            self.register_status("building",transient=True,init=True,
                                 build={"step":"init","sources":sources})
            job = self.merge_sources(source_names=sources, job_manager=job_manager, *args, **kwargs)
            task = asyncio.ensure_future(job)
            task.add_done_callback(self.store_stats)
            return task

        except (KeyboardInterrupt,Exception) as e:
            self.logger.exception(e)
            self.register_status("failed",build={"err": repr(e)})
            self.logger.error("failed: %s" % e,extra={"notify":True})
            raise

    def get_mapper_for_source(self,src_name,init=True):
        # src_name can be a regex (when source has split collections, they are merge but
        # comes from the same "template" sourcek
        docs = self.source_backend.get_src_master_docs()
        mapper_name = None
        for master_name in docs:
            pat = re.compile("^%s$" % master_name)
            if pat.match(src_name):
                mapper_name = docs[master_name].get("mapper")
        # TODO: this could be a list
        try:
            init and self.init_mapper(mapper_name)
            mapper = self.mappers[mapper_name]
            self.logger.info("Found mapper '%s' for source '%s'" % (mapper,src_name))
            return mapper
        except KeyError:
            raise BuilderException("Found mapper named '%s' but no mapper associated" % mapper_name)

    @asyncio.coroutine
    def merge_sources(self, source_names, steps=["merge","post"], batch_size=100000, job_manager=None):
        """
        Merge resources from given source_names or from build config.
        Identify root document sources from the list to first process them.
        """
        assert job_manager
        # check what to do
        if type(steps) == str:
            steps = [steps]
        do_merge = "merge" in steps
        do_post_merge = "post" in steps
        total_docs = 0
        self.stats = {}
        # try to identify root document sources amongst the list to first
        # process them (if any)
        defined_root_sources = self.get_root_document_sources()
        root_sources = list(set(source_names).intersection(set(defined_root_sources)))
        other_sources = list(set(source_names).difference(set(root_sources)))
        # got root doc sources but not part of the merge ? that's weird...
        if defined_root_sources and not root_sources:
            self.logger.warning("Root document sources found (%s) but not part of the merge..." % defined_root_sources)

        source_names = sorted(source_names)
        root_sources = sorted(root_sources)
        other_sources = sorted(other_sources)

        self.logger.info("Sources to be merged: %s" % source_names)
        self.logger.info("Root sources: %s" % root_sources)
        self.logger.info("Other sources: %s" % other_sources)

        got_error = False

        @asyncio.coroutine
        def merge(src_names):
            jobs = []
            for i,src_name in enumerate(src_names):
                yield from asyncio.sleep(0.0)
                job = self.merge_source(src_name, batch_size=batch_size, job_manager=job_manager)
                job = asyncio.ensure_future(job)
                def merged(f,stats):
                    try:
                        res = f.result()
                        stats.update(res)
                    except Exception as e:
                        self.logger.exception("Failed merging source '%s': %s" % (src_name, e))
                        nonlocal got_error
                        got_error = e
                job.add_done_callback(partial(merged,stats=self.stats))
                jobs.append(job)
                # raise error as soon as we know something went wrong
                if got_error:
                    raise got_error
            tasks = asyncio.gather(*jobs)
            yield from tasks

        if do_merge:
            if root_sources:
                self.register_status("building",transient=True,
                        build={"step":"merge-root","sources":root_sources})
                self.logger.info("Merging root document sources: %s" % root_sources)
                yield from merge(root_sources)

            if other_sources:
                self.register_status("building",transient=True,
                        build={"step":"merge-others","sources":other_sources})
                self.logger.info("Merging other resources: %s" % other_sources)
                yield from merge(other_sources)

            self.register_status("building",transient=True,
                    build={"step":"finalizing"})
            self.logger.info("Finalizing target backend")
            self.target_backend.finalize()
        else:
            self.logger.info("Skip data merging")

        if do_post_merge:
            self.logger.info("Running post-merge process")
            self.register_status("building",transient=True,
                    build={"step":"post-merge"})
            pinfo = self.get_pinfo()
            pinfo["step"] = "post-merge"
            job = yield from job_manager.defer_to_thread(pinfo,partial(self.post_merge, source_names, batch_size, job_manager))
            job = asyncio.ensure_future(job)
            def postmerged(f):
                self.logger.info("Post-merge completed [%s]" % f.result())
            job.add_done_callback(postmerged)
            res = yield from job
        else:
            self.logger.info("Skip post-merge process")

        yield from asyncio.sleep(0.0)
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
        defined_root_sources = self.get_root_document_sources()
        upsert = not defined_root_sources or src_name in defined_root_sources
        if not upsert:
            self.logger.debug("Documents from source '%s' will be stored only if a previous document exists with same _id" % src_name)
        jobs = []
        total = self.source_backend[src_name].count()
        btotal = math.ceil(total/batch_size) 
        bnum = 1
        cnt = 0
        got_error = False
        # grab ids only, so we can get more, let's say 10 times more
        id_batch_size = batch_size * 10
        self.logger.info("Fetch _ids from '%s' with batch_size=%d, and create merger job with batch_size=%d" % (src_name, id_batch_size, batch_size))
        for big_doc_ids in doc_feeder(self.source_backend[src_name], step=id_batch_size, inbatch=True, fields={'_id':1}):
            for doc_ids in iter_n(big_doc_ids,batch_size):
                # try to put some async here to give control back
                # (but everybody knows it's a blocking call: doc_feeder)
                yield from asyncio.sleep(0.1)
                cnt += len(doc_ids)
                pinfo = self.get_pinfo()
                pinfo["step"] = src_name
                pinfo["description"] = "#%d/%d (%.1f%%)" % (bnum,btotal,(cnt/total*100))
                self.logger.info("Creating merger job #%d/%d, to process '%s' %d/%d (%.1f%%)" % \
                        (bnum,btotal,src_name,cnt,total,(cnt/total*100.)))
                ids = [doc["_id"] for doc in doc_ids]
                job = yield from job_manager.defer_to_process(
                        pinfo,
                        partial(merger_worker,
                            self.source_backend[src_name].name,
                            self.target_backend.target_name,
                            ids,
                            self.get_mapper_for_source(src_name,init=False),
                            upsert,
                            bnum))
                def batch_merged(f,batch_num):
                    nonlocal got_error
                    if type(f.result()) != int:
                        got_error = Exception("Batch #%s failed while merging source '%s' [%s]" % (batch_num,src_name,f.result()))
                job.add_done_callback(partial(batch_merged,batch_num=bnum))
                jobs.append(job)
                bnum += 1
                # raise error as soon as we know
                if got_error:
                    raise got_error
        self.logger.info("%d jobs created for merging step" % len(jobs))
        tasks = asyncio.gather(*jobs)
        def done(f):
            nonlocal got_error
            if None in f.result():
                got_error = Exception("Some batches failed")
                return
            # compute overall inserted/updated records
            cnt = sum(f.result())

        tasks.add_done_callback(done)
        yield from tasks
        if got_error:
            raise got_error
        else:
            return {"total_%s" % src_name : cnt}

    def post_merge(self, source_names, batch_size, job_manager):
        pass


from biothings.utils.backend import DocMongoBackend
import biothings.utils.mongo as mongo

def merger_worker(col_name,dest_name,ids,mapper,upsert,batch_num):
    try:
        src = mongo.get_src_db()
        tgt = mongo.get_target_db()
        col = src[col_name]
        #if batch_num == 2:
        #    raise ValueError("oula pa bon")
        dest = DocMongoBackend(tgt,tgt[dest_name])
        cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}})
        mapper.load()
        docs = mapper.process(cur)
        cnt = dest.update(docs, upsert=upsert)
        return cnt
    except Exception as e:
        logger_name = "%s_%s_batch_%s" % (dest_name,col_name,batch_num)
        logger = get_logger(logger_name, btconfig.LOG_FOLDER)
        logger.exception(e)
        raise


from biothings.utils.manager import BaseManager
import biothings.utils.mongo as mongo
import biothings.databuild.backend as backend
from biothings.databuild.backend import TargetDocMongoBackend


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
        self.setup_log()
        # check if src_build exist and create it as necessary
        if not self.src_build.name in self.src_build.database.collection_names():
            logging.debug("Creating '%s' collection (one-time)" % self.src_build.name)
            self.src_build.database.create_collection(self.src_build.name)
            # this is dummy configuration, used as a template
            logging.debug("Creating dummy configuration (one-time)")
            conf = {"_id" : "placeholder",
                    "name" : "placeholder",
                    "root" : [],
                    "sources" : []}
            self.src_build.insert_one(conf)

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

    def setup_log(self):
        self.logger = btconfig.logger

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

    def merge(self, build_name, sources=None, target_name=None, **kwargs):
        """
        Trigger a merge for build named 'build_name'. Optional list of sources can be
        passed (one single or a list). target_name is the target collection name used
        to store to merge data. If none, each call will generate a unique target_name.
        """
        try:
            bdr = self[build_name]
            job = bdr.merge(sources,target_name,job_manager=self.job_manager,**kwargs)
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

    @asyncio.coroutine
    def diff_cols(self,old_db_col_names, new_db_col_names, batch_size=100000, purge=False, exclude=[]):
        """
        Compare new with old collections and produce diff files. Root keys can be excluded from 
        comparison with "exclude" parameter.
        *_db_col_names can be: 
         1. a colleciton name (as a string) asusming they are
            in the target database.
         2. tuple with 2 elements, the first one is then either "source" or "target"
            to respectively specify src or target database, and the second element is
            the collection name.
         3. tuple with 3 elements (URI,db,collection), looking like:
            ("mongodb://user:pass@host","dbname","collection"), allowing to specify
            any connection on any server
        """
        new = create_backend(new_db_col_names)
        old = create_backend(old_db_col_names)

        diff_folder = os.path.join(btconfig.DIFF_PATH,
                                   "%s_vs_%s" % (old.target_collection.name, new.target_collection.name))
        if os.path.exists(diff_folder):
            if purge:
                rmdashfr(diff_folder)
            else:
                raise FileExistsError("Found existing files in '%s', delete them or use purge=True" % diff_folder)
        if not os.path.exists(diff_folder):
            os.makedirs(diff_folder)

        data_new = doc_feeder(new.target_collection, step=batch_size, inbatch=True, fields={"_id":1})
        data_old = doc_feeder(old.target_collection, step=batch_size, inbatch=True, fields={"_id":1})
        cnt = 0
        stats = {"update":0, "add":0, "delete":0}

        jobs = []
        pinfo = {"category" : "diff",
                 "source" : "%s vs %s" % (new.target_collection.name,old.target_collection.name),
                 "step" : "new vs old",
                 "description" : ""}
        for _batch in data_new:
            cnt += 1
            id_list_new = [_doc['_id'] for _doc in _batch]
            pinfo["description"] = "batch #%s" % cnt
            def diffed(f):
                try:
                    res = f.result()
                    stats["update"] += res["update"]
                    stats["add"] += res["add"]
                    self.logger.info("(Updated: {}, Added: {})".format(res["update"], res["add"]))
                except Exception as e:
                    self.logger.exception("Error while diffing batch #%s, %s" % (cnt,e))
                    raise
            self.logger.info("Creating diff worker for batch #%s" % cnt)
            job = yield from self.job_manager.defer_to_process(pinfo,
                    partial(diff_worker_new_vs_old, id_list_new, old_db_col_names,
                            new_db_col_names, cnt , diff_folder, exclude))
            job.add_done_callback(diffed)
            jobs.append(job)
        yield from asyncio.gather(*jobs)
        self.logger.info("Finished calculating diff for the new collection. Total number of docs updated: {}, added: {}".format(stats["update"], stats["add"]))

        jobs = []
        pinfo["step"] = "old vs new"
        for _batch in data_old:
            cnt += 1
            id_list_old = [_doc['_id'] for _doc in _batch]
            pinfo["description"] = "batch #%s" % cnt
            def diffed(f):
                try:
                    res = f.result()
                    stats["delete"] += res["delete"]
                    self.logger.info("(Deleted: {})".format(res["delete"]))
                except Exception as e:
                    self.logger.exception("Error while diffing batch #%s, %s" % (cnt,e))
                    raise
            self.logger.info("Creating diff worker for batch #%s" % cnt)
            job = yield from self.job_manager.defer_to_process(pinfo,
                    partial(diff_worker_old_vs_new, id_list_old, new_db_col_names, cnt , diff_folder))
            job.add_done_callback(diffed)
            jobs.append(job)
        yield from asyncio.gather(*jobs)
        self.logger.info("Finished calculating diff for the old collection. Total number of docs deleted: {}".format(stats["delete"]))
        self.logger.info("Summary: (Updated: {}, Added: {}, Deleted: {})".format(stats["update"], stats["add"], stats["delete"]))

        return stats

    def diff(self,old_db_col_names, new_db_col_names, batch_size=100000, purge=False, exclude=[]):
        """wrapper over diff_cols() coroutine, return a task"""
        def diffed(f):
            try:
                pass
            except Exception as e:
                self.logger.exception("Error while running diff job, %s" % e)
                raise
        job = asyncio.ensure_future(self.diff_cols(old_db_col_names, new_db_col_names, batch_size, purge, exclude))
        job.add_done_callback(diffed)
        return job

    def build_diff_report(self, diff_folder, detailed=False,
                          max_reported_ids=btconfig.MAX_REPORTED_IDS):
        """
        Analyze diff files in diff_folder and give a summy of changes.
        max_reported_ids is the number of IDs contained in the report for each part.
        detailed will trigger a deeper analysis, takes more time.
        """

        update_details = {
                "add": {},# "count": 0, "data": {} },
                "remove": {}, # "count": 0, "data": {} },
                "replace": {}, # "count": 0, "data": {} },
                "move": {}, # "count": 0, "data": {} },
                "count": 0,
                }
        adds = {"count": 0, "ids": []}
        dels = {"count": 0, "ids": []}
        sources = {}

        def analyze(diff_file):
            data = loadobj(diff_file)
            sources[data["source"]] = 1
            if len(adds) < max_reported_ids:
                adds["ids"].extend(data["add"])
            adds["count"] += len(data["add"])
            if len(dels) < max_reported_ids:
                dels["ids"].extend(data["delete"])
            dels["count"] += len(data["delete"])
            for up in data["update"]:
                for patch in up["patch"]:
                    update_details[patch["op"]].setdefault(patch["path"],{"count": 0, "ids": []})
                    if len(update_details[patch["op"]][patch["path"]]["ids"]) < max_reported_ids:
                        update_details[patch["op"]][patch["path"]]["ids"].append(up["_id"])
                    update_details[patch["op"]][patch["path"]]["count"] += 1
            update_details["count"] += len(data["update"])

            assert len(sources) == 1, "Should have one datasource from diff files, got: %s" % [s for s in sources]


        data_folder = os.path.join(btconfig.DIFF_PATH,diff_folder)
        jobs = []
        # we randomize files order b/c we randomly pick some examples from those
        # files. If files contains data in order (like chrom 1, then chrom 2)
        # we won't have a representative sample
        files = glob.glob(os.path.join(data_folder,"*.pyobj"))
        random.shuffle(files)
        for f in files:
            logging.info("Running report worker for '%s'" % f)
            analyze(f)
        return {"added" : adds, "deleted": dels, "updated" : update_details,
                "diff_folder" : diff_folder, "detailed": detailed}



def diff_worker_new_vs_old(id_list_new, old_db_col_names, new_db_col_names, batch_num, diff_folder, exclude=[]):
    new = create_backend(new_db_col_names)
    old = create_backend(old_db_col_names)
    docs_common = old.target_collection.find({'_id': {'$in': id_list_new}}, projection=[])
    ids_common = [_doc['_id'] for _doc in docs_common]
    id_in_new = list(set(id_list_new) - set(ids_common))
    _updates = []
    if len(ids_common) > 0:
        _updates = diff_docs_jsonpatch(old, new, list(ids_common), exclude_attrs=exclude)
    file_name = os.path.join(diff_folder,"%s.pyobj" % str(batch_num))
    _result = {'add': id_in_new,
               'update': _updates,
               'delete': [],
               'source': new.target_collection.name,
               'timestamp': get_timestamp()}
    if len(_updates) != 0 or len(id_in_new) != 0:
        dump(_result, file_name)

    return {"add" : len(id_in_new), "update" : len(_updates), "delete" : 0}

def diff_worker_old_vs_new(id_list_old, new_db_col_names, batch_num, diff_folder):
    new = create_backend(new_db_col_names)
    docs_common = new.target_collection.find({'_id': {'$in': id_list_old}}, projection=[])
    ids_common = [_doc['_id'] for _doc in docs_common]
    id_in_old = list(set(id_list_old)-set(ids_common))
    file_name = os.path.join(diff_folder,"%s.pyobj" % str(batch_num))
    _result = {'delete': id_in_old,
               'add': [],
               'update': [],
               'source': new.target_collection.name,
               'timestamp': get_timestamp()}
    if len(id_in_old) != 0:
        dump(_result, file_name)

    return {"add" : 0, "update": 0, "delete" : len(id_in_old)}


def create_backend(db_col_names):
    col = None
    db = None
    if type(db_col_names) == str:
        db = mongo.get_target_db()
        col = db[db_col_names]
    elif db_col_names[0].startswith("mongodb://"):
        assert len(db_col_names) == 3, "Missing connection information for %s" % repr(db_col_names)
        conn = mongo.MongoClient(db_col_names[0])
        db = conn[db_col_names[1]]
        col = db[db_col_names[2]]
    else:
        assert len(db_col_names) == 2, "Missing connection information for %s" % repr(db_col_names)
        db = db_col_names[0] == "target" and mongo.get_target_db() or mongo.get_src_db()
        col = db[db_col_names[1]]
    assert not col is None, "Could not create collection object from %s" % repr(db_col_names)
    return btbackend.DocMongoBackend(db,col)


class DiffReportRendererBase(object):

    def __init__(self,
                 max_reported_ids=btconfig.MAX_REPORTED_IDS,
                 max_randomly_picked=btconfig.MAX_RANDOMLY_PICKED):
        self.max_reported_ids = max_reported_ids
        self.max_randomly_picked = max_randomly_picked

    def save(self,report,filename):
        """
        Save report output (rendered) into filename
        """
        raise NotImplementedError("implement me")


class DiffReportPrettyTable(DiffReportRendererBase):

    def save(self, report, filename="report.txt"):
        try:
            import prettytable
        except ImportError:
            raise ImportError("Please install prettytable to use this rendered")

        txt = ""
        txt += "Diff report\n"
        txt += "===========\n"
        txt += "\n"
        txt += "Summary\n"
        txt += "-------\n"
        txt += "#added documents: %s\n" % report["added"]["count"]
        txt += "#deleted documents: %s\n" % report["deleted"]["count"]
        txt += "#updated documents: %s\n" % report["updated"]["count"]
        txt += "\n"
        txt += "Added documents (%s randomly picked from report)\n" % self.max_reported_ids
        txt += "------------------------------------------------\n"
        table = prettytable.PrettyTable(["IDs"])
        if report["added"]["count"] <= self.max_reported_ids:
            ids = report["added"]["ids"]
        else:
            ids = [random.choice(report["added"]["ids"]) for i in range(self.max_reported_ids)]
        for dat in ids:
            table.add_row([dat])
        txt += table.get_string()
        txt += "\n"
        txt += "\n"
        txt += "Deleted documents (%s randomly picked from report)\n" % self.max_reported_ids
        txt += "--------------------------------------------------\n"
        table = prettytable.PrettyTable(["IDs"])
        if report["deleted"]["count"] <= self.max_reported_ids:
            ids = report["deleted"]["ids"]
        else:
            ids = [random.choice(report["deleted"]["ids"]) for i in range(self.max_reported_ids)]
        for dat in ids:
            table.add_row([dat])
        txt += table.get_string()
        txt += "\n"
        txt += "\n"
        txt += "Updated documents (%s examples randomly picked from report)\n" % self.max_randomly_picked
        txt += "-----------------------------------------------------------\n"
        txt += "\n"
        for op in report["updated"]:
            if op == "count":
                continue # already displayed
            table = prettytable.PrettyTable([op,"Count","Examples"])
            table.sortby = "Count"
            table.reversesort = True
            table.align[op] = "l"
            table.align["Count"] = "r"
            table.align["Examples"] = "l"
            for path in report["updated"][op]:
                info = report["updated"][op][path]
                row = [path,info["count"]]
                if info["count"] <= self.max_randomly_picked:
                    row.append(", ".join(info["ids"]))
                else:
                    row.append(", ".join([random.choice(info["ids"]) for i in range(self.max_randomly_picked)]))
                table.add_row(row)
            txt += table.get_string()
            txt += "\n"
            txt += "\n"
        txt += "\n"

        with open(os.path.join(report["diff_folder"],filename),"w") as fout:
            fout.write(txt)







