import time, sys, os, importlib, copy
import datetime, types, pprint
import asyncio
import logging as loggingmod
from functools import wraps

from biothings.utils.common import get_timestamp, get_random_string, timesofar, iter_n
from biothings.utils.mongo import get_src_conn, get_src_dump
from biothings.utils.dataload import merge_struct
from .storage import IgnoreDuplicatedStorage, MergerStorage, \
                     BasicStorage, NoBatchIgnoreDuplicatedStorage

from biothings import config

logging = config.logger

class ResourceNotReady(Exception):
    pass
class UnknownResource(Exception):
    pass
class ResourceError(Exception):
    pass


def ensure_prepared(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.prepared:
            self.prepare()
        return func(self,*args, **kwargs)
    return wrapper


def upload_worker(storage_class,loaddata_func,col_name,step,*args):
    """
    Pickable job launcher, typically running from multiprocessing.
    storage_class will instanciate with col_name, the destination 
    collection name. loaddata_func is the parsing/loading function,
    called with *args
    """
    data = loaddata_func(*args)
    storage = storage_class(None,col_name,loggingmod)
    storage.process(data,step)


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

# define storage strategy, set in subclass
    storage_class = None

    # Will be override in subclasses
    name = None # name of the resource and collection name used to store data
    main_source =None # if several resources, this one if the main name,
                      # it's also the _id of the resource in src_dump collection
                      # if set to None, it will be set to the value of variable "name"

    def __init__(self, db_conn_info, data_root, collection_name=None, *args, **kwargs):
        """db_conn_info is a database connection info tuple (host,port) to fetch/store 
        information about the datasource's state data_root is the root folder containing
        all resources. It will generate its own data folder from this point"""
        self.db_conn_info = db_conn_info
        self.timestamp = datetime.datetime.now()
        self.t0 = time.time()
        self.conn = None
        self.db = None
        self.__class__.main_source = self.__class__.main_source or self.__class__.name
        self.src_root_folder=os.path.join(data_root, self.__class__.main_source)
        self.logfile = None
        self.temp_collection_name = None
        self.collection = None # final collection
        self.collection_name = collection_name or self.name
        self.data_folder = None
        self.prepared = False

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

    def prepare(self,state={}):
        if self.prepared:
            return
        if state:
            for k in state:
                setattr(self,k,state[k])
            return

        self.conn = get_src_conn()
        self.db = self.conn[self.__class__.__database__]
        self.collection = self.db[self.collection_name]
        self.prepare_src_dump()
        self.data_folder = self.src_doc.get("data_folder")
        self.setup_log()
        # flag ready
        self.prepared = True

    def unprepare(self):
        """
        reset anything that's not pickable (so self can be pickled)
        return what's been reset as a dict, so self can be restored
        once pickled
        """
        state = {"db" : self.db,
                 "conn" : self.conn,
                 "collection" : self.collection,
                 "src_dump" : self.src_dump,
                 "logger" : self.logger,
                 "prepared" : self.prepared}
        for k in state:
            setattr(self,k,None)
        self.prepared = False
        return state

    def check_ready(self,force=False):
        if not self.src_doc:
            raise ResourceNotReady("Missing information for source '%s' to start upload" % self.main_source)
        if not self.src_doc.get("data_folder"):
            raise ResourceNotReady("No data folder found for resource '%s'" % self.name)
        if not self.src_doc.get("download",{}).get("status") == "success":
            raise ResourceNotReady("No successful download found for resource '%s'" % self.name)
        if not os.path.exists(self.src_root_folder):
            raise ResourceNotReady("Data folder '%s' doesn't exist for resource '%s'" % self.name)
        ##if not force and self.src_doc.get("upload",{}).get("status") == "uploading":
        ##    pid = self.src_doc.get("upload",{}).get("pid","unknown")
        ##    raise ResourceNotReady("Resource '%s' is already being uploaded (pid: %s)" % (self.name,pid))

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
    def update_data(self, doc_d, step, loop=None):
        f = loop.run_in_executor(None,upload_worker,BasicStorage,self.load_data,self.temp_collection_name,step,self.data_folder)
        yield from f
        #doc_d = doc_d or self.load_data(data_folder=self.data_folder)
        #self.logger.debug("doc_d mem: %s" % sys.getsizeof(doc_d))
        #self.storage = BasicStorage(self.db.client.address,self.temp_collection_name,self.logger)
        #self.storage.process(doc_d,step)
        self.switch_collection()
        self.post_update_data()

    def generate_doc_src_master(self):
        _doc = {"_id": str(self.name),
                "name": str(self.name), # TODO: remove ?
                "timestamp": datetime.datetime.now()}
        # store mapping
        _doc['mapping'] = self.__class__.get_mapping()
        # type of id being stored in these docs
        if hasattr(self, 'id_type'):
            _doc['id_type'] = getattr(self, 'id_type')
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

    def register_status(self,status,transient=False,**extra):
        upload_info = {
                'started_at': datetime.datetime.now(),
                'logfile': self.logfile,
                'status': status}

        if transient:
            # record some "in-progress" information
            upload_info['step'] = self.name
            upload_info['temp_collection'] = self.temp_collection_name
            upload_info['pid'] = os.getpid()
        else:
            # only register time when it's a final state
            t1 = round(time.time() - self.t0, 0)
            upload_info["time"] = timesofar(self.t0)
            upload_info["time_in_s"] = t1

        # merge extra at root or upload level
        # (to keep upload data...)
        if "upload" in extra:
            upload_info.update(extra["upload"])
        else:
            self.src_doc.update(extra)
        self.src_doc.update({"upload" : upload_info})
        self.src_dump.save(self.src_doc)

    def unset_pending_upload(self):
        self.src_doc.pop("pending_to_upload",None)
        self.src_dump.save(self.src_doc)

    @asyncio.coroutine
    def load(self, doc_d=None, update_data=True, update_master=True, force=False, step=10000, progress=None, total=None, loop=None):
        # postponed until now !
        self.prepare()
        # sanity check before running
        self.logger.info("Uploading '%s' (collection: %s)" % (self.name, self.collection_name))
        self.check_ready(force)
        try:
            self.unset_pending_upload()
            if not self.temp_collection_name:
                self.make_temp_collection()
            self.db[self.temp_collection_name].drop()       # drop all existing records just in case.
            upload = {}
            if total:
                upload = {"progress" : "%s/%s" % (progress,total)}
            self.register_status("uploading",transient=True,upload=upload)
            if update_data:
                state = self.unprepare()
                yield from self.update_data(doc_d,step,loop)
                self.prepare(state)
                #self.update_data(doc_d,step)
            if update_master:
                self.update_master()
            if progress == total:
                self.register_status("success")
        except (KeyboardInterrupt,Exception) as e:
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed",upload={"err": repr(e)})
            raise

    def prepare_src_dump(self):
        self.src_dump = get_src_dump()
        self.src_doc = self.src_dump.find_one({'_id': self.main_source})

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.src_root_folder):
            os.makedirs(self.src_root_folder)
        self.logfile = os.path.join(self.src_root_folder, '%s_%s_upload.log' % (self.main_source,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fh.setFormatter(logging_mod.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        fh.name = "logfile"
        sh = logging_mod.StreamHandler()
        sh.name = "logstream"
        self.logger = logging_mod.getLogger("%s_upload" % self.main_source)
        self.logger.setLevel(logging_mod.DEBUG)
        if not fh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(fh)
        if not sh.name in [h.name for h in self.logger.handlers]:
            self.logger.addHandler(sh)


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
        self.src_dump = get_src_dump()
        # just populate/initiate an src_dump record (b/c no dump before)
        self.src_dump.save({"_id":self.main_source})
        self.src_doc = self.src_dump.find_one({'_id': self.main_source})

    def check_ready(self,force=False):
        # bypass checks about src_dump
        pass

    def update_data(self, doc_d, step):
        self.logger.info("Dummy uploader, nothing to upload")
        # sanity check, dummy uploader, yes, but make sure data is there
        assert self.collection.count() > 0, "No data found in collection '%s' !!!" % self.collection_name


class ParallelizedSourceUploader(BaseSourceUploader):

    @ensure_prepared
    def jobs(self):
        """Return list of (func,*arguments) passed to self.load_data, in order. for
        each parallelized jobs. Ex: [(x,1),(y,2),(z,3)]"""
        raise NotImplementedError("implement me in subclass")

    @asyncio.coroutine
    def update_data(self, doc_d, step, loop=None):
        fs = []
        jobs = self.jobs()
        state = self.unprepare()
        for args in jobs:
            f = loop.run_in_executor(None,
                    # pickable worker
                    upload_worker,
                    # storage class
                    self.__class__.storage_class,
                    # loading func
                    self.load_data,
                    # dest collection name
                    self.temp_collection_name,
                    # batch size
                    step,
                    # and finally *args passed to loading func
                    *args)
            fs.append(f)
        yield from asyncio.wait(fs)
        self.prepare(state)
        self.switch_collection()
        self.post_update_data()



##############################

class SourceManager(object):
    '''After registering datasources, manager will orchestrate
    source uploading. Default source uploader is used when none specified
    for a datasource. Otherwise, when registering a datasource, a specific
    datasource can be specfied.
    '''

    def __init__(self, event_loop=None, datasource_path="dataload.sources"):
        self.doc_register = {}
        self.conn = get_src_conn()
        self.default_src_path = datasource_path
        self.loop = event_loop

    def generate_uploader_instances(self,src_module):
        # try to find a uploader class in the module
        uploader_klasses = []
        found_one = False
        for attr in dir(src_module):
            something = getattr(src_module,attr)
            if type(something) == type and issubclass(something,BaseSourceUploader):
                uploader_klass = something
                if uploader_klass.name is None:
                    logging.debug("%s has no 'name' defined, skip it" % uploader_klass)
                    continue
                found_one = True
                logging.debug("Found uploader class '%s'" % uploader_klass)
                res = uploader_klass.create(db_conn_info=self.conn.address,data_root=config.DATA_ARCHIVE_ROOT)
                if isinstance(res,list):
                    # a true factory may return several instances
                    for inst in res:
                        yield inst
                else:
                    yield res
        if not found_one:
            raise UnknownResource("Can't find an uploader class in module '%s'" % src_module)

    def register_source(self,src_data):
        """Register a new data source. src_data can be a module where some
        __metadata__ about the source can be found. It can also be a module path
        as a string, or just a source name in which case it will try to find
        information from default path.
        """
        if isinstance(src_data,str):
            try:
                src_m = importlib.import_module(src_data)
            except ImportError:
                try:
                    src_m = importlib.import_module("%s.%s" % (self.default_src_path,src_data))
                except ImportError:
                    msg = "Can't find module '%s', even in '%s'" % (src_data,self.default_src_path)
                    logging.error(msg)
                    raise UnknownResource(msg)

        elif isinstance(src_data,dict):
            # source is comprised of several other sub sources
            assert len(src_data) == 1, "Should have only one element in source dict '%s'" % src_data
            _, sub_srcs = list(src_data.items())[0]
            for src in sub_srcs:
                self.register_source(src)
            return
        else:
            src_m = src_data
        uploader_insts = self.generate_uploader_instances(src_m)
        for uploader_inst in uploader_insts:
            if uploader_inst.main_source:
                self.doc_register.setdefault(uploader_inst.main_source,[]).append(uploader_inst)
            else:
                self.doc_register[updloader_inst.name] = uploader_inst
            self.conn.register(uploader_inst.__class__)

    def register_sources(self, sources):
        assert not isinstance(sources,str), "sources argument is a string, should pass a list"
        self.doc_register.clear()
        for src_data in sources:
            try:
                self.register_source(src_data)
            except UnknownResource as e:
                logging.info("Can't register source '%s', skip it; %s" % (src_data,e))
                import traceback
                logging.error(traceback.format_exc())

    def upload_all(self,raise_on_error=False,**kwargs):
        errors = {}
        for src in self.doc_register:
            try:
                self.upload_src(src, **kwargs)
            except Exception as e:
                errors[src] = e
                if raise_on_error:
                    raise
        if errors:
            logging.warning("Found errors while uploading:\n%s" % pprint.pformat(errors))
            return errors

    #@asyncio.coroutine
    def upload_src(self, src, **kwargs):
        uploaders = None
        if src in self.doc_register:
            uploaders = self.doc_register[src]
        else:
            # maybe src is a sub-source ?
            for main_src in self.doc_register:
                for sub_src in self.doc_register[main_src]:
                    # search for "sub_src" or "main_src.sub_src"
                    main_sub = "%s.%s" % (main_src,sub_src.name)
                    if (src == sub_src.name) or (src == main_sub):
                        uploaders = sub_src
                        logging.info("Found uploader '%s' for '%s'" % (uploaders,src))
                        break
        if not uploaders:
            raise ResourceError("Can't find '%s' in registered sources (whether as main or sub-source)" % src)

        jobs = []
        try:
            if isinstance(uploaders,list):
                # this is a resource composed by several sub-resources
                # let's mention intermediate step (so "success" means all subsources
                # have been uploaded correctly
                for i,one in enumerate(uploaders):
                    # FIXME: load() will call update_master_data(), which calls get_mapping()
                    # which is a class-method. should iterate over instances' class and 
                    # call update_master_data() for each class, not each instance
                    job = self.submit(one.load,None,True,True,False,10000,i+1,len(uploaders),self.loop)
                    jobs.append(job)
            else:
                uploader = uploaders # for the sake of readability...
                job = self.submit(uploader.load,None,True,True,False,10000,None,None,self.loop)
                jobs.append(job)
            return jobs
        except Exception as e:
            logging.error("Error while uploading '%s': %s" % (src,e))
            raise

    def submit(self,f,*args,**kwargs):
        if self.loop:
            logging.info("Building task: %s(*args=%s,**kwargs=%s)" % (f,args,kwargs))
            ff = asyncio.ensure_future(f(*args,**kwargs))
            return ff
        else:
            return f(*args,**kwargs)

    def __repr__(self):
        return "<%s [%d registered]: %s>" % (self.__class__.__name__,len(self.doc_register), list(self.doc_register.keys()))

    def __getitem__(self,src_name):
        try:
            # as a main-source
            return self.doc_register[src_name]
        except KeyError:
            try:
                # as a sub-source
                main,sub = src_name.split(".")
                srcs = self.doc_register[main]
                # there can be many uploader for one resource (when each is dealing
                # with one specific file but upload to the same collection for instance)
                # so we want to make sure user is aware of this and not just return one
                # uploader when many are needed
                # on the other hand, if only one avail, just return it
                res = []
                for src in srcs:
                    if src.name == sub:
                        res.append(src)
                if len(res) == 1:
                    return res.pop()
                elif len(res) == 0:
                    raise KeyError(src_name)
                else:
                    return res
            except (ValueError,KeyError):
                # nope, can't find it...
                raise KeyError(src_name)


