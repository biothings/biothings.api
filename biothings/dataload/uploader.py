import time, sys, os, copy
import datetime, pprint
import asyncio
import logging as loggingmod
from functools import wraps, partial

from biothings.utils.common import get_timestamp, get_random_string, timesofar, iter_n
from biothings.utils.mongo import get_src_conn, get_src_dump
from biothings.utils.dataload import merge_struct
from biothings.utils.manager import BaseSourceManager, track_process, \
                                    ManagerError, ResourceNotFound
from .storage import IgnoreDuplicatedStorage, MergerStorage, \
                     BasicStorage, NoBatchIgnoreDuplicatedStorage, \
                     NoStorage

from biothings import config

logging = config.logger

class ResourceNotReady(Exception):
    pass
class ResourceError(Exception):
    pass


def upload_worker(name,storage_class,loaddata_func,col_name,batch_size,*args):
    """
    Pickable job launcher, typically running from multiprocessing.
    storage_class will instanciate with col_name, the destination 
    collection name. loaddata_func is the parsing/loading function,
    called with *args
    """
    data = loaddata_func(*args)
    storage = storage_class(None,col_name,loggingmod)
    storage.process(data,batch_size)


class DocSourceMaster(dict):
    '''A class to manage various doc data sources.'''
    # TODO: fix this delayed import
    from biothings import config
    __collection__ = config.DATA_SRC_MASTER_COLLECTION
    __database__ = config.DATA_SRC_DATABASE
    use_dot_notation = True
    use_schemaless = True
    structure = {
        'name': str,
        'timestamp': datetime.datetime,
    }


class BaseSourceUploader(object):
    '''
    Default datasource uploader. Database storage can be done
    in batch or line by line. Duplicated records aren't not allowed
    '''
    # TODO: fix this delayed import
    from biothings import config
    __database__ = config.DATA_SRC_DATABASE

    # define storage strategy, override in subclass as necessary
    storage_class = BasicStorage

    # Will be override in subclasses
    name = None # name of the resource and collection name used to store data
    main_source =None # if several resources, this one if the main name,
                      # it's also the _id of the resource in src_dump collection
                      # if set to None, it will be set to the value of variable "name"

    keep_archive = 10 # number of archived collection to keep. Oldest get dropped first.

    def __init__(self, db_conn_info, data_root, collection_name=None, *args, **kwargs):
        """db_conn_info is a database connection info tuple (host,port) to fetch/store 
        information about the datasource's state data_root is the root folder containing
        all resources. It will generate its own data folder from this point"""
        # non-pickable attributes (see __getattr__, prepare() and unprepare())
        self.init_state()
        self.db_conn_info = db_conn_info
        self.timestamp = datetime.datetime.now()
        self.t0 = time.time()
        # main_source at object level so it's part of pickling data
        # otherwise it won't be set properly when using multiprocessing
        # note: "name" is always defined at class level so pickle knows
        # how to restore it
        self.main_source = self.__class__.main_source or self.__class__.name
        self.src_root_folder=os.path.join(data_root, self.main_source)
        self.logfile = None
        self.temp_collection_name = None
        self.collection_name = collection_name or self.name
        self.data_folder = None
        self.prepared = False

    @property
    def fullname(self):
        if self.main_source != self.name:
            name = "%s.%s" % (self.main_source,self.name)
        else:
            name = self.name
        return name

    @classmethod
    def create(klass, db_conn_info, data_root, *args, **kwargs):
        """
        Factory-like method, just return an instance of this uploader
        (used by SourceManager, may be overridden in sub-class to generate
        more than one instance per class, like a true factory.
        This is usefull when a resource is splitted in different collection but the
        data structure doesn't change (it's really just data splitted accros
        multiple collections, usually for parallelization purposes).
        Instead of having actual class for each split collection, factory
        will generate them on-the-fly.
        """
        return klass(db_conn_info, data_root, *args, **kwargs)

    def init_state(self):
        self._state = {
                "db" : None,
                "conn" : None,
                "collection" : None,
                "src_dump" : None,
                "logger" : None
        }

    def prepare(self,state={}):
        """Sync uploader information with database (or given state dict)"""
        if self.prepared:
            return
        if state:
            # let's be explicit, _state takes what it wants
            for k in self._state:
                self._state[k] = state[k]
            return
        self._state["conn"] = get_src_conn()
        self._state["db"] = self.conn[self.__class__.__database__]
        self._state["collection"] = self.db[self.collection_name]
        self._state["src_dump"] = self.prepare_src_dump()
        self._state["logger"] = self.setup_log()
        self.data_folder = self.src_doc.get("data_folder")
        # flag ready
        self.prepared = True

    def unprepare(self):
        """
        reset anything that's not pickable (so self can be pickled)
        return what's been reset as a dict, so self can be restored
        once pickled
        """
        state = {
            "db" : self._state["db"],
            "conn" : self._state["conn"],
            "collection" : self._state["collection"],
            "src_dump" : self._state["src_dump"],
            "logger" : self._state["logger"]
        }
        for k in state:
            self._state[k] = None
        self.prepared = False
        return state

    def check_ready(self,force=False):
        if not self.src_doc:
            raise ResourceNotReady("Missing information for source '%s' to start upload" % self.main_source)
        if not self.src_doc.get("data_folder"):
            raise ResourceNotReady("No data folder found for resource '%s'" % self.name)
        if not force and not self.src_doc.get("download",{}).get("status") == "success":
            raise ResourceNotReady("No successful download found for resource '%s'" % self.name)
        if not os.path.exists(self.src_root_folder):
            raise ResourceNotReady("Data folder '%s' doesn't exist for resource '%s'" % self.name)
        job = self.src_doc.get("upload",{}).get("job",{}).get(self.name)
        if not force and job:
            raise ResourceNotReady("Resource '%s' is already being uploaded (job: %s)" % (self.name,job))

    def load_data(self,data_folder):
        """Parse data inside data_folder and return structure ready to be
        inserted in database"""
        raise NotImplementedError("Implement in subclass")

    @classmethod
    def get_mapping(self):
        """Return ES mapping"""
        raise NotImplementedError("Implement in subclass")

    def make_temp_collection(self):
        '''Create a temp collection for dataloading, e.g., entrez_geneinfo_INEMO.'''
        if self.temp_collection_name:
            # already set
            return
        new_collection = None
        self.temp_collection_name = self.collection_name + '_temp_' + get_random_string()
        return self.temp_collection_name

    def clean_archived_collections(self):
        # archived collections look like...
        prefix = "%s_archive_" % self.name
        cols = [c for c in self.db.collection_names() if c.startswith(prefix)]
        # timestamp is what's after _archive_, YYYYMMDD, so we can sort it safely
        cols = sorted(cols,reverse=True)
        to_drop = cols[self.keep_archive:]
        for colname in to_drop:
            self.logger.info("Cleaning old archive collection '%s'" % colname)
            self.db[colname].drop()

    def switch_collection(self):
        '''after a successful loading, rename temp_collection to regular collection name,
           and renaming existing collection to a temp name for archiving purpose.
        '''
        if self.temp_collection_name and self.db[self.temp_collection_name].count() > 0:
            if self.collection.count() > 0:
                # renaming existing collections
                new_name = '_'.join([self.collection_name, 'archive', get_timestamp(), get_random_string()])
                self.collection.rename(new_name, dropTarget=True)
            self.db[self.temp_collection_name].rename(self.collection_name)
        else:
            raise ResourceError("No temp collection (or it's empty)")

    def post_update_data(self):
        """Override as needed to perform operations after
           data has been uploaded"""
        pass

    @asyncio.coroutine
    def update_data(self, batch_size, job_manager):
        """
        Iterate over load_data() to pull data and store it
        """
        self.unprepare()
        f = job_manager.defer_to_process(
                upload_worker,
                self.fullname,
                self.__class__.storage_class,
                self.load_data,
                self.temp_collection_name,
                batch_size,
                self.data_folder)
        yield from f
        self.switch_collection()
        self.clean_archived_collections()

    def generate_doc_src_master(self):
        _doc = {"_id": str(self.name),
                "name": str(self.name), # TODO: remove ?
                "timestamp": datetime.datetime.now()}
        # store mapping
        _doc['mapping'] = self.__class__.get_mapping()
        # type of id being stored in these docs
        if hasattr(self.__class__, '__metadata__'):
            _doc.update(self.__class__.__metadata__)
        return _doc

    def update_master(self):
        _doc = self.generate_doc_src_master()
        self.save_doc_src_master(_doc)

    def save_doc_src_master(self,_doc):
        coll = self.conn[DocSourceMaster.__database__][DocSourceMaster.__collection__]
        dkey = {"_id": _doc["_id"]}
        prev = coll.find_one(dkey)
        if prev:
            coll.replace_one(dkey, _doc)
        else:
            coll.insert_one(_doc)

    def register_status(self,status,**extra):
        """
        Register step status, ie. status for a sub-resource
        """
        upload_info = {"status" : status}
        upload_info.update(extra)
        job_key = "upload.jobs.%s" % self.name

        if status == "uploading":
            # record some "in-progress" information
            upload_info['step'] = self.name
            upload_info['temp_collection'] = self.temp_collection_name
            upload_info['pid'] = os.getpid()
            upload_info['logfile'] = self.logfile
            upload_info['started_at'] = datetime.datetime.now()
            self.src_dump.update({"_id":self.main_source},{"$set" : {job_key : upload_info}})
        else:
            # only register time when it's a final state
            # also, keep previous uploading information
            upd = {}
            for k,v in upload_info.items():
                upd["%s.%s" % (job_key,k)] = v
            t1 = round(time.time() - self.t0, 0)
            upd["%s.status" % job_key] = status
            upd["%s.time" % job_key] = timesofar(self.t0)
            upd["%s.time_in_s" % job_key] = t1
            self.src_dump.update({"_id" : self.main_source},{"$set" : upd})

    @asyncio.coroutine
    def load(self, steps=["data","post","master"], force=False,
             batch_size=10000, job_manager=None):
        """
        Main resource load process, reads data from doc_c using chunk sized as batch_size.
        steps defines the different processes used to laod the resource:
        - "data"   : will store actual data into single collections
        - "post"   : will perform post data load operations
        - "master" : will register the master document in src_master
        """
        self.logger.info("Uploading '%s' (collection: %s)" % (self.name, self.collection_name))
        # sanity check before running
        self.check_ready(force)
        # check what to do
        if type(steps) == str:
            steps = [steps]
        update_data = "data" in steps
        update_master = "master" in steps
        post_update_data = "post" in steps
        try:
            if not self.temp_collection_name:
                self.make_temp_collection()
            self.db[self.temp_collection_name].drop()       # drop all existing records just in case.
            self.register_status("uploading")
            if update_data:
                # unsync to make it pickable
                state = self.unprepare()
                yield from self.update_data(batch_size, job_manager)
                # then restore state
                self.prepare(state)
            if update_master:
                self.update_master()
            if post_update_data:
                self.unprepare()
                f2 = job_manager.defer_to_process(self.post_update_data)
                yield from f2
            cnt = self.db[self.collection_name].count()
            self.register_status("success",count=cnt)
        except Exception as e:
            self.register_status("failed",err=str(e))
            raise


    def prepare_src_dump(self):
        """Sync with src_dump collection, collection information (src_doc)
        Return src_dump collection"""
        src_dump = get_src_dump()
        self.src_doc = src_dump.find_one({'_id': self.main_source})
        return src_dump

    def setup_log(self):
        """Setup and return a logger instance"""
        import logging as logging_mod
        if not os.path.exists(self.src_root_folder):
            os.makedirs(self.src_root_folder)
        self.logfile = os.path.join(self.src_root_folder, '%s-%s_%s_upload.log' % (self.main_source,self.name,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fh.setFormatter(logging_mod.Formatter('%(asctime)s [%(process)d] %(name)s - %(levelname)s - %(message)s'))
        fh.name = "logfile"
        sh = logging_mod.StreamHandler()
        sh.name = "logstream"
        logger = logging_mod.getLogger("%s_upload" % self.main_source)
        logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in logger.handlers]:
            logger.addHandler(fh)
        if not sh.name in [h.name for h in logger.handlers]:
            logger.addHandler(sh)
        return logger

    def __getattr__(self,attr):
        """This catches access to unpicabkle attributes. If unset,
        will call sync to restore them."""
        # tricky: self._state will always exist when the instance is create
        # through __init__(). But... when pickling the instance, __setstate__
        # is used to restore attribute on an instance that's hasn't been though
        # __init__() constructor. So we raise an error here to tell pickle not 
        # to restore this attribute (it'll be set after)
        if attr == "_state":
            raise AttributeError(attr)
        if attr in self._state:
            if not self._state[attr]:
                self.prepare()
            return self._state[attr]
        else:
            raise AttributeError(attr)


class NoBatchIgnoreDuplicatedSourceUploader(BaseSourceUploader):
    '''Same as default uploader, but will store records and ignore if
    any duplicated error occuring (use with caution...). Storage
    is done line by line (slow, not using a batch) but preserve order
    of data in input file.
    '''
    storage_class = NoBatchIgnoreDuplicatedStorage


class IgnoreDuplicatedSourceUploader(BaseSourceUploader):
    '''Same as default uploader, but will store records and ignore if
    any duplicated error occuring (use with caution...). Storage
    is done using batch and unordered bulk operations.
    '''
    storage_class = IgnoreDuplicatedStorage


class MergerSourceUploader(BaseSourceUploader):

    storage_class = MergerStorage


class DummySourceUploader(BaseSourceUploader):
    """
    Dummy uploader, won't upload any data, assuming data is already there
    but make sure every other bit of information is there for the overall process
    (usefull when online data isn't available anymore)
    """

    def prepare_src_dump(self):
        src_dump = get_src_dump()
        # just populate/initiate an src_dump record (b/c no dump before)
        src_dump.save({"_id":self.main_source})
        self.src_doc = src_dump.find_one({'_id': self.main_source})
        return src_dump

    def check_ready(self,force=False):
        # bypass checks about src_dump
        pass

    @asyncio.coroutine
    def update_data(self, batch_size, job_manager=None):
        self.logger.info("Dummy uploader, nothing to upload")
        # sanity check, dummy uploader, yes, but make sure data is there
        assert self.collection.count() > 0, "No data found in collection '%s' !!!" % self.collection_name


class ParallelizedSourceUploader(BaseSourceUploader):

    def jobs(self):
        """Return list of (*arguments) passed to self.load_data, in order. for
        each parallelized jobs. Ex: [(x,1),(y,2),(z,3)]"""
        raise NotImplementedError("implement me in subclass")

    @asyncio.coroutine
    def update_data(self, batch_size, job_manager=None):
        fs = []
        jobs = self.jobs()
        state = self.unprepare()
        for args in jobs:
            f = job_manager.defer_to_process(
                    # pickable worker
                    upload_worker,
                    # worker name
                    self.fullname,
                    # storage class
                    self.__class__.storage_class,
                    # loading func
                    self.load_data,
                    # dest collection name
                    self.temp_collection_name,
                    # batch size
                    batch_size,
                    # and finally *args passed to loading func
                    *args)
            fs.append(f)
        yield from asyncio.wait(fs)
        self.switch_collection()
        self.clean_archived_collections()


class NoDataSourceUploader(BaseSourceUploader):
    """
    This uploader won't upload any data and won't even assume
    there's actual data (different from DummySourceUploader on this point).
    It's usefull for instance when mapping need to be stored (get_mapping())
    but data doesn't comes from an actual upload (ie. generated)
    """
    storage_class = NoStorage

    @asyncio.coroutine
    def update_data(self, batch_size, job_manager=None):
        self.logger.debug("No data to upload, skip")


##############################

import aiocron

class UploaderManager(BaseSourceManager):
    '''
    After registering datasources, manager will orchestrate source uploading.
    '''

    SOURCE_CLASS = BaseSourceUploader

    def __init__(self,poll_schedule=None,*args,**kwargs):
        super(UploaderManager,self).__init__(*args,**kwargs)
        self.poll_schedule = poll_schedule

    def filter_class(self,klass):
        if klass.name is None:
            # usually a base defined in an uploader, which then is subclassed in same
            # module. Kind of intermediate, not fully functional class
            logging.debug("%s has no 'name' defined, skip it" % klass)
            return None
        else:
            return klass

    def create_instance(self,klass):
        res = klass.create(db_conn_info=self.conn.address,data_root=config.DATA_ARCHIVE_ROOT)
        return res

    def register_classes(self,klasses):
        for klass in klasses:
            if klass.main_source:
                self.register.setdefault(klass.main_source,[]).append(klass)
            else:
                self.register.setdefault(klass.name,[]).append(klass)
            self.conn.register(klass)

    def register_status(self,src_name,status,**extra):
        """
        Register overall status for resource
        """
        src_dump = get_src_dump()
        upload_info = {'status': status}
        upload_info.update(extra)
        if status == "uploading":
            upload_info["jobs"] = {}
            # unflag "need upload"
            src_dump.update({"_id" : src_name},{"$unset" : {"pending_to_upload":None}})
            src_dump.update({"_id" : src_name},{"$set" : {"upload" : upload_info}})
        else:
            # we want to keep information
            upd = {}
            for k,v in upload_info.items():
                upd["upload.%s" % k] = v
            src_dump.update({"_id" : src_name},{"$set" : upd})


    def upload_all(self,raise_on_error=False,**kwargs):
        """
        Trigger upload processes for all registered resources.
        **kwargs are passed to upload_src() method
        """
        errors = {}
        for src in self.register:
            try:
                self.upload_src(src, **kwargs)
            except Exception as e:
                errors[src] = e
                if raise_on_error:
                    raise
        if errors:
            logging.warning("Found errors while uploading:\n%s" % pprint.pformat(errors))
            return errors

    def upload_src(self, src, *args, **kwargs):
        """
        Trigger upload for registered resource named 'src'.
        Other args are passed to uploader's load() method
        """
        try:
            klasses = self[src]
        except KeyError:
            raise ResourceNotFound("Can't find '%s' in registered sources (whether as main or sub-source)" % src)

        jobs = []
        try:
            self.register_status(src,"uploading")
            for i,klass in enumerate(klasses):
                job = self.job_manager.submit(partial(
                        self.create_and_load,klass,job_manager=self.job_manager,*args,**kwargs))
                jobs.append(job)
            tasks = asyncio.gather(*jobs)
            def done(f):
                try:
                    self.register_status(src,"success")
                except Exception as e:
                    self.register_status(src,"failed",err=repr(e))
            tasks.add_done_callback(done)
            return jobs
        except Exception as e:
            import traceback
            logging.error("Error while uploading '%s': %s\n%s" % (src,e,traceback.format_exc()))
            self.register_status(src,"failed",err=repr(e))
            raise

    @asyncio.coroutine
    def create_and_load(self,klass,*args,**kwargs):
        insts = self.create_instance(klass)
        if type(insts) != list:
            insts = [insts]
        for inst in insts:
            yield from inst.load(*args,**kwargs)

    def poll(self):
        if not self.poll_schedule:
            raise ManagerError("poll_schedule is not defined")
        src_dump = get_src_dump()
        @asyncio.coroutine
        def check_pending_to_upload():
            sources = [src['_id'] for src in src_dump.find({'pending_to_upload': True}) if type(src['_id']) == str]
            logging.info("Found %d resources to upload (%s)" % (len(sources),repr(sources)))
            for src_name in sources:
                logging.info("Launch upload for '%s'" % src_name)
                try:
                    self.upload_src(src_name)
                except ResourceNotFound:
                    logging.error("Resource '%s' needs upload but is not registerd in manager" % src_name)
        cron = aiocron.crontab(self.poll_schedule,func=partial(check_pending_to_upload),
                start=True, loop=self.job_manager.loop)
