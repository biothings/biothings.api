import sys, re, os, time, math, glob, copy
from datetime import datetime
from dateutil.parser import parse as dtparse
import pickle, json
from pprint import pformat
import asyncio
from functools import partial
from elasticsearch import Elasticsearch

import biothings.utils.mongo as mongo
from biothings.utils.hub_db import get_src_build
import biothings.utils.aws as aws
from biothings.utils.common import timesofar, get_random_string, iter_n, \
                                   get_class_from_classpath, get_dotfield_value
from biothings.utils.loggers import HipchatHandler, get_logger
from biothings.utils.manager import BaseManager
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
from biothings import config as btconfig
from biothings.utils.mongo import doc_feeder, id_feeder
from config import LOG_FOLDER, logger as logging
from biothings.utils.hub import publish_data_version
from biothings.hub.databuild.backend import generate_folder, create_backend, \
                                            merge_src_build_metadata
from biothings.hub import INDEXER_CATEGORY, INDEXMANAGER_CATEGORY


def new_index_worker(col_name,ids,pindexer,batch_num):
        tgt = mongo.get_target_db()
        col = tgt[col_name]
        idxer = pindexer()
        cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}})
        cnt = idxer.index_bulk(cur)
        return cnt


def merge_index_worker(col_name,ids,pindexer,batch_num):
        tgt = mongo.get_target_db()
        col = tgt[col_name]
        idxer = pindexer()
        upd_cnt = 0
        new_cnt = 0
        cur = doc_feeder(col, step=len(ids), inbatch=False, query={'_id': {'$in': ids}})
        docs = [d for d in cur]
        [d.pop("_timestamp",None) for d in docs]
        dids = dict([(d["_id"],d) for d in docs])
        dexistings = dict([(d["_id"],d) for d in idxer.get_docs([k for k in dids.keys()])])
        for _id in dexistings:
            d = dexistings[_id]
            # update in-place
            d.update(dids[_id])
            # mark as processed/updated
            dids.pop(_id)
        # updated docs (those existing in col *and* index)
        upd_cnt = idxer.index_bulk(dexistings.values(),len(dexistings))
        logging.debug("%s documents updated in index" % repr(upd_cnt))
        # new docs (only in col, *not* in index)
        new_cnt = idxer.index_bulk(dids.values(),len(dids))
        logging.debug("%s new documents in index" % repr(new_cnt))
        # need to return one: tuple(cnt,list)
        ret = (upd_cnt[0] + new_cnt[0], upd_cnt[1] + new_cnt[1])
        return ret


def indexer_worker(col_name,ids,pindexer,batch_num,mode="index",
                   worker=new_index_worker):
    try:
        if mode in ["index","merge"]:
            return worker(col_name,ids,pindexer,batch_num)
        elif mode == "resume":
            idxr = pindexer()
            es_ids = idxr.mexists(ids)
            missing_ids = [e[0] for e in es_ids if e[1] == False]
            if missing_ids:
                return worker(col_name,missing_ids,pindexer,batch_num)
            else:
                # fake indexer results, it has to be a tuple, first elem is num of indexed docs
                return (0,None)
    except Exception as e:
        logger_name = "index_%s_%s_batch_%s" % (pindexer.keywords.get("index","index"),col_name,batch_num)
        logger = get_logger(logger_name, btconfig.LOG_FOLDER)
        logger.exception(e)
        exc_fn = os.path.join(btconfig.LOG_FOLDER,"%s.pick" % logger_name)
        pickle.dump({"exc":e,"ids":ids},open(exc_fn,"wb"))
        logger.info("Exception and IDs were dumped in pickle file '%s'" % exc_fn)
        raise


class IndexerException(Exception):
    pass


class IndexerManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super(IndexerManager,self).__init__(*args, **kwargs)
        self.src_build = get_src_build()
        self.indexers = {}
        self.t0 = time.time()
        self.prepared = False
        self.log_folder = LOG_FOLDER
        self.timestamp = datetime.now()
        self.setup()

    def setup(self):
        self.setup_log()

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, 'indexmanager_%s.log' % time.strftime("%Y%m%d",self.timestamp.timetuple()))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("indexmanager")
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not nh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(nh)
        return self.logger

    def get_predicates(self):
        def no_other_indexmanager_step_running(job_manager):
            """IndexManager deals with snapshot, publishing,
            none of them should run more than one at a time"""
            return len([j for j in job_manager.jobs.values() if j["category"] == INDEXMANAGER_CATEGORY]) == 0
        return [no_other_indexmanager_step_running]

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {"category" : INDEXMANAGER_CATEGORY,
                "source" : "",
                "step" : "",
                "description" : ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    def __getitem__(self,conf_name):
        """
        Return an instance of an indexer for the build configuration named 'conf_name'
        Note: each call returns a different instance (factory call behind the scene...)
        """
        kwargs = BaseManager.__getitem__(self,conf_name)
        return kwargs

    def configure_from_list(self,indexers_kwargs):
        for dindex in indexers_kwargs:
            assert len(dindex) == 1, "Invalid indexer registration data: %s" % dindex
            env,idxkwargs = list(dindex.items())[0]
            self.register[env] = idxkwargs

    def configure_from_dict(self,confdict):
        self.es_config = copy.deepcopy(confdict)
        self.indexers.update(confdict.get("indexer_select",{}))
        indexers_kwargs = []
        for env,conf in confdict["env"].items():
            idxkwargs = dict(**conf["indexer"]["args"])
            indexers_kwargs.append({env:idxkwargs})
        self.configure_from_list(indexers_kwargs)

    def configure(self,indexer_defs):
        """
        Register indexers with:
        - a list of dict as:
            [{"indexer_type_name": partial},{....}]
        - a dict containing all indexer definitions:
            {"env" : {
                "env1" : {
                    "host": "localhost:9200",
                    "timeout": ..., "retry":...,
                    "indexer" : "path.to.ClassIndexer",
                },
                ...
            }
        Partial is used to instantiate an indexer, without args
        """
        if type(indexer_defs) == list:
            self.configure_from_list(indexer_defs)
        elif type(indexer_defs) == dict:
            self.configure_from_dict(indexer_defs)
        else:
            raise ValueError("Unknown indexer definitions type (expecting a list or a dict")
        self.logger.info(self.indexers)
        self.logger.info(self.register)

    def find_indexer(self, target_name):
        """
        Return indexer class required to index target_name.
        Rules depend on what's inside the corresponding src_build doc
        and the indexers definitions
        """
        if not self.indexers:
            return Indexer # default one
        doc = self.src_build.find_one({"_id":target_name})
        klass = None
        for path_in_doc in self.indexers:
            if klass is None and path_in_doc is None:
                # couldn't find a klass yet and we found a default declated, keep it
                strklass = self.indexers[path_in_doc]
                klass = get_class_from_classpath(strklass)
            else:
                try:
                    val = get_dotfield_value(path_in_doc,doc)
                    strklass = self.indexers[path_in_doc]
                    klass = get_class_from_classpath(strklass)
                    self.logger.info("Found special indexer '%s' required to index '%s'" % (klass,target_name))
                    # the first to match wins
                    break
                except KeyError:
                    pass
        if klass is None:
            return Indexer
        else:
            # either we return a default declared in config or
            # a specific one found according to the doc
            return klass

    def index(self, indexer_env, target_name=None, index_name=None, ids=None, **kwargs):
        """
        Trigger an index creation to index the collection target_name and create an 
        index named index_name (or target_name if None). Optional list of IDs can be
        passed to index specific documents.
        """
        t0 = time.time()
        def indexed(f):
            res = f.result()
            try:
                self.logger.info("Done indexing target '%s' to index '%s': %s" % (target_name,index_name,res))
            except Exception as e:
                self.logger.exception("Error while running merge job, %s" % e)
                raise
        idxklass = self.find_indexer(target_name)
        idxkwargs = self[indexer_env]
        idx = idxklass(**idxkwargs)
        idx.env = indexer_env
        idx.target_name = target_name
        index_name = index_name or target_name
        job = idx.index(target_name, index_name, ids=ids, job_manager=self.job_manager, **kwargs)
        job = asyncio.ensure_future(job)
        job.add_done_callback(indexed)

        return job

    def snapshot(self, indexer_env, index, snapshot=None, mode=None, steps=["snapshot"], repository=btconfig.SNAPSHOT_REPOSITORY):
        # check what to do
        if type(steps) == str:
            steps = [steps]
        snapshot = snapshot or index
        idxr = self[indexer_env]
        es_idxr = ESIndexer(index=index,doc_type=idxr.doc_type,es_host=idxr.host)
        # will hold the overall result
        fut = asyncio.Future()

        def get_status():
            try:
                res = es_idxr.get_snapshot_status(repository, snapshot)
                assert "snapshots" in res, "Can't find snapshot '%s' in repository '%s'" % (snapshot,repository)
                # assuming only one index in the snapshot, so only check first elem
                state = res["snapshots"][0].get("state")
                assert state, "Can't find state in snapshot '%s'" % snapshot
                return state
            except Exception as e:
                # somethng went wrong, report as failure
                return "FAILED"

        @asyncio.coroutine
        def do(index):
            def snapshot_launched(f):
                try:
                    self.logger.info("Snapshot launched: %s" % f.result())
                except Exception as e:
                    self.logger.error("Error while lauching snapshot: %s" % e)
                    fut.set_exception(e)
            if "snapshot" in steps:
                pinfo = self.get_pinfo()
                pinfo["source"] = index
                pinfo["step"] = "snapshot"
                pinfo["description"] = idxr.host
                self.logger.info("Creating snapshot for index '%s' on host '%s', repository '%s'" % (index,idxr.host,repository))
                job = yield from self.job_manager.defer_to_thread(pinfo,
                        partial(es_idxr.snapshot,repository,snapshot, mode=mode))
                job.add_done_callback(snapshot_launched)
                yield from job
                while True:
                    state = get_status()
                    if state in ["INIT","IN_PROGRESS","STARTED"]:
                        yield from asyncio.sleep(getattr(btconfig,"MONITOR_SNAPSHOT_DELAY",60))
                    else:
                        if state == "SUCCESS":
                            fut.set_result(state)
                            self.logger.info("Snapshot '%s' successfully created (host: '%s', repository: '%s')" % \
                                    (snapshot,idxr.host,repository),extra={"notify":True})
                        else:
                            e = IndexerException("Snapshot '%s' failed: %s" % (snapshot,state))
                            fut.set_exception(e)
                            self.logger.error("Failed creating snapshot '%s' (host: %s, repository: %s), state: %s" % \
                                    (snapshot,idxr.host,repository,state),extra={"notify":True})
                            raise e
                        break

        task = asyncio.ensure_future(do(index))
        return fut

    def publish_snapshot(self, indexer_env, s3_folder, prev=None, snapshot=None, release_folder=None, index=None,
                         repository=btconfig.SNAPSHOT_REPOSITORY):
        """
        Publish snapshot metadata (not the actal snapshot, but the metadata, release notes, etc... associated to it) to S3,
        and then register that version to it's available to auto-updating hub.

        Though snapshots don't need any previous version to be applied on, a release note with significant changes
        between current snapshot and a previous version could have been generated. In that case, 

        'prev' and 'snaphost' must be defined (as strings, should match merged collections names) to generate
        a release folder, or directly release_folder (if it's required to find release notes).
        If all 3 are None, no release note will be referenced in snapshot metadata.

        'snapshot' and actual underlying index can have different names, if so, 'index' can be specified.
        'index' is mainly used to get the build_version from metadata as this information isn't part of snapshot
        information. It means in order to publish a snaphost, both the snapshot *and* the index must exist.
        """
        assert getattr(btconfig,"BIOTHINGS_ROLE",None) == "master","Hub needs to be master to publish metadata about snapshots"
        # keep passed values if any, otherwise derive them
        index = index or snapshot
        snapshot = snapshot or index
        idxr = self[indexer_env]
        es_idxr = ESIndexer(index=index,doc_type=idxr.doc_type,es_host=idxr.host)
        esb = DocESBackend(es_idxr)
        assert esb.version, "Can't retrieve a version from index '%s'" % index
        self.logger.info("Generating JSON metadata for full release '%s'" % esb.version)
        repo = es_idxr._es.snapshot.get_repository(repository)
        release_note = "release_%s" % esb.version
        # generate json metadata about this diff release
        assert snapshot, "Missing snapshot name information"
        full_meta = {
                "type": "full",
                "build_version": esb.version,
                "target_version": esb.version,
                "release_date" : datetime.now().isoformat(),
                "app_version": None,
                "metadata" : {"repository" : repo,
                              "snapshot_name" : snapshot}
                }
        # TODO: merged collection name can be != index name which can be != snapshot name...
        if prev and index and not release_folder:
            release_folder = generate_folder(btconfig.RELEASE_PATH,prev,index)
        if release_folder and os.path.exists(release_folder):
            # ok, we have something in that folder, just pick the release note files
            # (we can generate diff + snaphost at the same time, so there could be diff files in that folder
            # from a diff process done before. release notes will be the same though)
            s3basedir = os.path.join(s3_folder,esb.version)
            notes = glob.glob(os.path.join(release_folder,"%s.*" % release_note))
            self.logger.info("Uploading release notes from '%s' to s3 folder '%s'" % (notes,s3basedir))
            for note in notes:
                if os.path.exists(note):
                    s3key = os.path.join(s3basedir,os.path.basename(note))
                    aws.send_s3_file(note,s3key,
                            aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                            s3_bucket=btconfig.S3_RELEASE_BUCKET,overwrite=True)
            # specify release note URLs in metadata
            rel_txt_url = aws.get_s3_url(os.path.join(s3basedir,"%s.txt" % release_note),
                            aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=btconfig.S3_RELEASE_BUCKET)
            rel_json_url = aws.get_s3_url(os.path.join(s3basedir,"%s.json" % release_note),
                            aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,s3_bucket=btconfig.S3_RELEASE_BUCKET)
            if rel_txt_url:
                full_meta.setdefault("changes",{})
                full_meta["changes"]["txt"] = {"url" : rel_txt_url}
            if rel_json_url:
                full_meta.setdefault("changes",{})
                full_meta["changes"]["json"] = {"url" : rel_json_url}
        else:
            self.logger.info("No release_folder found, no release notes will be part of the publishing")

        # now dump that metadata
        build_info = "%s.json" % esb.version
        build_info_path = os.path.join(btconfig.DIFF_PATH,build_info)
        json.dump(full_meta,open(build_info_path,"w"))
        # override lastmodified header with our own timestamp
        local_ts = dtparse(es_idxr.get_mapping_meta()["_meta"]["build_date"])
        utc_epoch = int(time.mktime(local_ts.timetuple()))
        utc_ts = datetime.fromtimestamp(time.mktime(time.gmtime(utc_epoch)))
        str_utc_epoch = str(utc_epoch)
        # it's a full release, but all build info metadata (full, incremental) all go
        # to the diff bucket (this is the main entry)
        s3key = os.path.join(s3_folder,build_info)
        aws.send_s3_file(build_info_path,s3key,
                aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                s3_bucket=btconfig.S3_RELEASE_BUCKET,metadata={"lastmodified":str_utc_epoch},
                 overwrite=True)
        url = aws.get_s3_url(s3key,aws_key=btconfig.AWS_KEY,aws_secret=btconfig.AWS_SECRET,
                s3_bucket=btconfig.S3_RELEASE_BUCKET)
        self.logger.info("Full release metadata published for version: '%s'" % url)
        full_info = {"build_version":full_meta["build_version"],
                "require_version":None,
                "target_version":full_meta["target_version"],
                "type":full_meta["type"],
                "release_date":full_meta["release_date"],
                "url":url}
        publish_data_version(s3_folder,full_info)
        self.logger.info("Registered version '%s'" % (esb.version))

    def index_info(self, env=None, remote=False):
        res = copy.deepcopy(self.es_config)
        for kenv in self.es_config["env"]:
            if env and env != kenv:
                continue
            if remote:
                # lost all indices, remotely
                try:
                    cl = Elasticsearch(res["env"][kenv]["host"])
                    indices = [{"index":k,
                        "doc_type":list(v["mappings"].keys())[0],
                        "aliases":list(v["aliases"].keys())}
                        for k,v in cl.indices.get("*").items()]
                    # for now, we just consider
                    if type(res["env"][kenv]["index"]) == dict:
                        # we don't where to put those indices because we don't
                        # have that information, so we just put those in a default category
                        # TODO: put that info in metadata ?
                        res["env"][kenv]["index"].setdefault(None,[]).extend(indices)
                    else:
                        assert type(res["env"][kenv]["index"]) == list
                        res["env"][kenv]["index"].extend(indices)
                except Exception as e:
                    self.logger.warning("Can't load remote indices: %s" % e)
                    continue

        return res

    def validate_mapping(self, mapping, env):
        idxr_obj = self[env]
        host = idxr_obj.host
        settings = idxr_obj.get_index_creation_settings()
        # generate a random index, it'll be deleted at the end
        index_name = ("hub_tmp_%s" % get_random_string()).lower()
        idxr = ESIndexer(index=index_name,es_host=host,doc_type=None)
        self.logger.info("Testing mapping by creating index '%s' on host '%s' (settings: %s)" % \
                (index_name,host,settings))
        try:
            res = idxr.create_index(mapping,settings)
            return res
        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            try:
                idxr.delete_index()
            except Exception as e:
                pass

class Indexer(object):
    """
    Basic indexer, reading documents from a mongo collection (target_name)
    and sending documents to ES.
    """

    def __init__(self, es_host, target_name=None, **kwargs):
        self.host = es_host
        self.env = None
        self.log_folder = LOG_FOLDER
        self.timestamp = datetime.now()
        self.conf_name = None
        self.build_doc = None
        self.target_name = None
        self.index_name = None
        self.doc_type = None
        self.num_shards = None
        self.num_replicas = None
        self.kwargs = kwargs
        self.ti = time.time()

    def get_predicates(self):
        return []

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        return {"category" : INDEXER_CATEGORY,
                "source" : "%s:%s" % (self.conf_name,self.index_name),
                "step" : "",
                "description" : ""}
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo

    @asyncio.coroutine
    def index(self, target_name, index_name, job_manager, steps=["index","post"],
              batch_size=10000, ids=None, mode="index", worker=None):
        """
        Build an index named "index_name" with data from collection
        "target_collection". "ids" can be passed to selectively index documents. "mode" can have the following
        values:
        - 'purge': will delete index if it exists
        - 'resume': will use existing index and add documents. "ids" can be passed as a list of missing IDs,
                 or, if not pass, ES will be queried to identify which IDs are missing for each batch in
                 order to complete the index.
        - 'merge': will merge data with existing index' documents, used when populated several distinct times (cold/hot merge for instance)
        - None (default): will create a new index, assuming it doesn't already exist
        """
        assert job_manager
        # check what to do
        if type(steps) == str:
            steps = [steps]
        self.target_name = target_name
        self.index_name = index_name
        self.setup_log()
        self.load_build()

        # select proper index worker according to mode:
        if worker is None: # none specified, choose correct one
            if mode == "merge":
                worker = merge_index_worker
            else:
                worker = new_index_worker

        got_error = False
        cnt = 0

        if "index" in steps:
            self.register_status("indexing",transient=True,init=True,job={"step":"index"})
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
                                 number_of_shards=self.num_shards,
                                 number_of_replicas=self.num_replicas,
                                 **self.kwargs)
            # instantiate one here for index creation
            es_idxer = partial_idxer()
            if es_idxer.exists_index():
                if mode == "purge":
                    es_idxer.delete_index()
                elif not mode in ["resume","merge"]:
                    raise IndexerException("Index already '%s' exists, (use mode='purge' to auto-delete it or mode='resume' to add more documents)" % index_name)

            if not mode in ["resume","merge"]:
                es_idxer.create_index({self.doc_type:_mapping},_extra)

            jobs = []
            total = target_collection.count()
            btotal = math.ceil(total/batch_size) 
            bnum = 1
            if ids:
                self.logger.info("Indexing from '%s' with specific list of _ids, create indexer job with batch_size=%d" % (target_name, batch_size))
                id_provider = [ids]
            else:
                self.logger.info("Fetch _ids from '%s', and create indexer job with batch_size=%d" % (target_name, batch_size))
                id_provider = id_feeder(target_collection, batch_size=batch_size,logger=self.logger)
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
                            bnum,
                            mode,
                            worker))
                def batch_indexed(f,batch_num):
                    nonlocal got_error
                    res = f.result()
                    if type(res) != tuple or type(res[0]) != int:
                        got_error = Exception("Batch #%s failed while indexing collection '%s' [result:%s]" % \
                                (batch_num,self.target_name,repr(res)))
                job.add_done_callback(partial(batch_indexed,batch_num=bnum))
                jobs.append(job)
                bnum += 1
                # raise error as soon as we know
                if got_error:
                    self.register_status("failed",job={"err": repr(got_error)})
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
                self.register_status("success",job={"step":"index"},index={"count":cnt})
                self.logger.info("Index '%s' successfully created" % index_name,extra={"notify":True})
            tasks.add_done_callback(done)
            yield from tasks

        if "post" in steps:
            self.logger.info("Running post-index process for index '%s'" % index_name)
            self.register_status("indexing",transient=True,init=True,job={"step":"post-index"})
            pinfo = self.get_pinfo()
            pinfo["step"] = "post_index"
            # for some reason (like maintaining object's state between pickling).
            # we can't use process there. Need to use thread to maintain that state without
            # building an unmaintainable monster
            job = yield from job_manager.defer_to_thread(pinfo, partial(self.post_index, target_name, index_name,
                    job_manager, steps=steps, batch_size=batch_size, ids=ids, mode=mode))
            def posted(f):
                nonlocal got_error
                try:
                    res = f.result()
                    self.logger.info("Post-index process done for index '%s': %s" % (index_name,res))
                    self.register_status("indexing",job={"step":"post-index"})
                except Exception as e:
                    got_error = e
                    self.logger.error("Post-index process failed for index '%s': %s" % (index_name,e),extra={"notify":True})
                    return
            job.add_done_callback(posted)
            yield from asyncio.gather(job) # consume future

        if got_error:
            self.register_status("failed",job={"err": repr(got_error)})
            raise got_error
        else:
            self.register_status("success")
            return {"%s" % self.index_name : cnt}

    def register_status(self,status,transient=False,init=False,**extra):
        assert self.build_doc
        src_build = get_src_build()
        job_info = {
                'status': status,
                'step_started_at': datetime.now(),
                'logfile': self.logfile,
                }
        index_info = {
                "index": {
                    self.index_name : {
                        'host' : self.host,
                        'environment' : self.env,
                        'conf_name' : self.conf_name,
                        'target_name' : self.target_name,
                        'index_name' : self.index_name,
                        'doc_type' : self.doc_type,
                        'num_shards' : self.num_shards,
                        'num_replicas' : self.num_replicas
                        }
                    }
                }
        if transient:
            # record some "in-progress" information
            job_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            job_info["time"] = timesofar(self.ti)
            t1 = round(time.time() - self.ti, 0)
            job_info["time_in_s"] = t1
            index_info["index"][self.index_name]["created_at"] = datetime.now()
        if "index" in extra:
            index_info["index"][self.index_name].update(extra["index"])
        if "job" in extra:
            job_info.update(extra["job"])
        # since the base is the merged collection, we register info there
        build = src_build.find_one({'_id': self.target_name})
        assert build, "Can't find build document '%s'" % self.target_name
        if init:
            # init timer for this step
            self.ti = time.time()
            src_build.update({'_id': self.target_name}, {"$push": {'jobs': job_info}})
            # now refresh/sync
            build = src_build.find_one({'_id': self.target_name})
        else:
            # merge extra at root level
            # (to keep building data...) and update the last one
            # (it's been properly created before when init=True)
            build["jobs"] and build["jobs"][-1].update(job_info)
            def merge_index_info(target,d):
                if "__REPLACE__" in d.keys():
                    d.pop("__REPLACE__")
                    target = d
                else:
                    for k,v in d.items():
                        if type(v) == dict:
                            if k in target:
                                target[k] = merge_index_info(target[k],v) 
                            else:
                                v.pop("__REPLACE__",None)
                                # merge v with "nothing" just to make sure to remove any "__REPLACE__"
                                v = merge_index_info({},v)
                                target[k] = v
                        else:
                            target[k] = v
                return target
            build = merge_index_info(build,index_info)
            src_build.replace_one({"_id" : build["_id"]}, build)

    def post_index(self, target_name, index_name, job_manager, steps=["index","post"], batch_size=10000, ids=None, mode=None):
        """
        Override in sub-class to add a post-index process. Method's signature is the same as index() to get
        the full context. This method will run in a thread (using job_manager.defer_to_thread())
        """
        pass

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.log_folder):
            os.makedirs(self.log_folder)
        self.logfile = os.path.join(self.log_folder, 'index_%s_%s.log' % (self.index_name,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fmt = logging_mod.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S")
        fh.setFormatter(fmt)
        fh.name = "logfile"
        nh = HipchatHandler(btconfig.HIPCHAT_CONFIG)
        nh.setFormatter(fmt)
        nh.name = "hipchat"
        self.logger = logging_mod.getLogger("%s_index" % self.conf_name)
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
        return {
                # as of ES6, include_in_all was removed, we need to create our own "all" field
                "query": {"default_field": "all"},
                # as of ES6, analysers/tokenizers must be defined in index settings, during creation
                "analysis": {
                    "analyzer": {
                        "string_lowercase": {
                            "tokenizer": "keyword",
                            "filter": "lowercase"
                            },
                        "whitespace_lowercase": {
                            "tokenizer": "whitespace",
                            "filter": "lowercase"
                            },
                        }
                    },
                }

    def get_mapping(self):
        '''collect mapping data from data sources.
           This is for GeneDocESBackend only.
        '''
        mapping = self.build_doc.get("mapping",{})
        # default "all" field to replace include_in_all field in older versions of ES
        mapping["all"] = {'type': 'text'}
        final_mapping = {"properties": mapping, "dynamic": "false"}
        final_mapping["_meta"] = self.get_metadata()

        return final_mapping

    def get_metadata(self):
        return self.build_doc.get("_meta",{})

    def get_build(self,target_name=None):
        target_name = target_name or self.target_name
        assert target_name, "target_name must be defined first before searching for builds"
        builds = [b for b in self.build_config["build"] if b == target_name]
        assert len(builds) == 1, "Can't find build for config '%s' and target_name '%s'" % (self.conf_name,self.target_name)
        return self.build_config["build"][builds[0]]

    def get_src_versions(self):
        build = self.get_build()
        return build["src_version"]

    def get_stats(self):
        build = self.get_build()
        return build["stats"]

    def get_timestamp(self):
        build = self.get_build()
        return build["build_date"]

    def get_build_version(self):
        build = self.get_build()
        return build["build_version"]

    def load_build(self, target_name=None):
        '''Load build info from src_build collection.'''
        target_name = target_name or self.target_name
        src_build = get_src_build()
        self.build_doc = src_build.find_one({'_id': target_name})
        assert self.build_doc, "Can't find build document associated to '%s'" % target_name
        _cfg = self.build_doc.get("build_config")
        if _cfg:
            self.build_config = _cfg
            #if not "doc_type" in _cfg:
            #    raise ValueError("Missing 'doc_type' in build config")
            self.doc_type = _cfg.get("doc_type")
            self.num_shards = _cfg.get("num_shards",10) # optional
            self.num_shards = self.num_shards and int(self.num_shards) or self.num_shards
            self.num_replicas = _cfg.get("num_replicas",0) # optional
            self.num_replicas = self.num_replicas and int(self.num_replicas) or self.num_replicas
            self.conf_name = _cfg["name"]
        else:
            raise ValueError("Cannot find build config associated to '%s'" % target_name)
        return _cfg


class ColdHotIndexer(Indexer):
    """
    This indexer works with 2 mongo collections to create a single index.
    - one premerge collection contains "cold" data, which never changes (not updated)
    - another collection contains "hot" data, regularly updated
    Index is created fetching the premerge documents. Then, documents from the hot collection
    are merged by fetching docs from the index, updating them, and putting them back in the index.
    """

    def __init__(self, *args, **kwargs):
        super(ColdHotIndexer,self).__init__(*args, **kwargs)
        self.hot_target_name = None
        self.cold_target_name = None
        self.cold_build_doc = None
        self.hot_build_doc = None
        self.cold_cfg = None
        self.hot_cfg = None

    @asyncio.coroutine
    def index(self, hot_name, index_name, job_manager, steps=["index","post"], batch_size=10000, ids=None, mode="index"):
        assert job_manager
        # check what to do
        if type(steps) == str:
            steps = [steps]
        self.hot_target_name = hot_name
        self.setup_log()
        self.load_build()
        if type(index_name) == list:
            # values are coming from target names, use the cold
            self.index_name = self.hot_target_name
        else:
            self.index_name = index_name
        got_error = False
        cnt = 0
        # selectively index cold then hot collections, using default index method
        # but specifically 'index' step to prevent any post-process before end of
        # index creation
        cold_task = super(ColdHotIndexer,self).index(self.cold_target_name,
                                                     self.index_name,steps="index",
                                                     job_manager=job_manager,
                                                     batch_size=batch_size,ids=ids,mode=mode)
        # wait until cold is fully indexed
        yield from cold_task
        # use updating indexer worker for hot to merge in index
        hot_task = super(ColdHotIndexer,self).index(self.hot_target_name,
                                                     self.index_name,steps="index",
                                                     job_manager=job_manager,
                                                     batch_size=batch_size,ids=ids,mode="merge")
        task = asyncio.ensure_future(hot_task)
        def done(f):
            nonlocal got_error
            nonlocal cnt
            try:
                res = f.result()
                # compute overall inserted/updated records
                cnt = sum(res.values())
                self.register_status("success",job={"step":"index"},index={"count":cnt})
                self.logger.info("Index '%s' successfully created" % index_name,extra={"notify":True})
            except Exception as e:
                logging.exception("Failed indexing cold/hot collections: %s" % e)
                got_error = e
                raise
        task.add_done_callback(done)
        yield from task
        if got_error:
            raise got_error
        return {self.index_name:cnt}

    def get_mapping(self):
        cold_mapping = self.cold_build_doc.get("mapping",{})
        hot_mapping = self.hot_build_doc.get("mapping",{})
        hot_mapping.update(cold_mapping) # mix cold&hot
        mapping = {"properties": hot_mapping,
                   "dynamic": "false"}
        mapping["_meta"] = self.get_metadata()
        return mapping

    def get_metadata(self):
        meta = merge_src_build_metadata([self.cold_build_doc,self.hot_build_doc])
        return meta

    def get_src_versions(self):
        _meta = self.get_metadata()
        return _meta["src_version"]

    def get_stats(self):
        _meta = self.get_metadata()
        return _meta["stats"]

    def get_timestamp(self):
        _meta = self.get_metadata()
        return _meta["build_date"]

    def get_build_version(self):
        _meta = self.get_metadata()
        return _meta["build_version"]

    def load_build(self):
        """
        Load cold and hot build documents.
        Index settings are the one declared in the hot build doc.
        """
        src_build = get_src_build()
        self.hot_build_doc = src_build.find_one({'_id': self.hot_target_name})
        # search the cold collection definition
        assert "build_config" in self.hot_build_doc and "cold_collection" in self.hot_build_doc["build_config"], \
                "Can't find cold_collection field in build_config"
        self.cold_target_name = self.hot_build_doc["build_config"]["cold_collection"]
        self.cold_build_doc = src_build.find_one({'_id': self.cold_target_name})
        # we'll register everything (status) on the hot one
        self.build_doc = self.hot_build_doc
        assert self.cold_build_doc, "Can't find build document associated to '%s'" % self.cold_target_name
        assert self.hot_build_doc, "Can't find build document associated to '%s'" % self.hot_target_name
        self.cold_cfg = self.cold_build_doc.get("build_config")
        self.hot_cfg = self.hot_build_doc.get("build_config")
        if self.hot_cfg or not self.cold_cfg:
            self.build_config = self.hot_cfg
            if not "doc_type" in self.hot_cfg:
                raise ValueError("Missing 'doc_type' in build config")
            self.doc_type = self.hot_cfg["doc_type"]
            self.num_shards = self.hot_cfg.get("num_shards",10) # optional
            self.num_shards = self.num_shards and int(self.num_shards) or self.num_shards
            self.num_replicas = self.hot_cfg.get("num_replicas",0) # optional
            self.num_replicas = self.num_replicas and int(self.num_replicas) or self.num_replicas
            self.conf_name = self.hot_cfg["name"]
        else:
            raise ValueError("Cannot find build config associated to '%s' or '%s'" % (self.hot_target_name,self.cold_target_name))
        return (self.cold_cfg,self.hot_cfg)

