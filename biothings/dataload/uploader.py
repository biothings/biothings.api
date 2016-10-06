import time, sys, os, importlib, copy
import datetime, types, pprint

from pymongo.errors import DuplicateKeyError, BulkWriteError

from biothings.utils.common import get_timestamp, get_random_string, timesofar, iter_n
from biothings.utils.mongo import get_src_conn, get_src_dump
from biothings.utils.dataload import merge_struct

from biothings import config

logging = config.logger

class ResourceNotReady(Exception):
    pass
class UnknownResource(Exception):
    pass
class ResourceError(Exception):
    pass

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
    temp_collection = None     # temp collection is for dataloading

    # Will be override in subclasses
    name = None # name of the resource and collection name used to store data
    main_source =None # if several resources, this one if the main name,
                      # it's also the _id of the resource in src_dump collection
                      # if set to None, it will be set to the value of variable "name"

    def __init__(self,db_conn,data_root):
        """db_conn is a database connection to fetch/store information about the datasource's state
        data_root is the root folder containing all resources. It will generate its own
        data folder from this point"""
        self.conn = db_conn
        self.timestamp = datetime.datetime.now()
        self.t0 = time.time()
        self.db = self.conn[self.__class__.__database__]
        self.__class__.main_source = self.__class__.main_source or self.__class__.name
        self.src_root_folder=os.path.join(data_root, self.__class__.main_source)
        self.prepare_src_dump()
        self.setup_log()

    def check_ready(self):
        if not self.src_doc:
            raise ResourceNotReady("Missing information for source '%s' to start upload" % self.main_source)
        if not self.src_doc.get("data_folder"):
            raise ResourceNotReady("No data folder found for resource '%s'" % self.name)
        if not self.src_doc.get("download",{}).get("status") == "success":
            raise ResourceNotReady("No successful download found for resource '%s'" % self.name)
        if not os.path.exists(self.src_root_folder):
            raise ResourceNotReady("Data folder '%s' doesn't exist for resource '%s'" % self.name)
        if self.src_doc.get("upload",{}).get("status") == "uploading":
            pid = self.src_doc.get("upload",{}).get("pid","unknown")
            raise ResourceNotReady("Resource '%s' is already being uploaded (pid: %s)" % (self.name,pid))

    def load_data(self,data_folder):
        """Parse data inside data_folder and return structure ready to be
        inserted in database"""
        raise NotImplementedError("Implement in subclass")

    def get_mapping(self):
        """Return ES mapping"""
        raise NotImplementedError("Implement in subclass")

    def make_temp_collection(self):
        '''Create a temp collection for dataloading, e.g., entrez_geneinfo_INEMO.'''
        new_collection = None
        while 1:
            new_collection = self.name + '_temp_' + get_random_string()
            if new_collection not in self.db.collection_names():
                break
        self.temp_collection = self.db[new_collection]
        return new_collection

    def switch_collection(self):
        '''after a successful loading, rename temp_collection to regular collection name,
           and renaming existing collection to a temp name for archiving purpose.
        '''
        if self.temp_collection and self.temp_collection.count() > 0:
            if self.collection.count() > 0:
                # renaming existing collections
                new_name = '_'.join([self.name, 'archive', get_timestamp(), get_random_string()])
                self.collection.rename(new_name, dropTarget=True)
            self.temp_collection.rename(self.name)
        else:
            raise ResourceError("No temp collection (or it's empty)")

    def doc_iterator(self, doc_d, batch=True, step=10000):
        if isinstance(doc_d, types.GeneratorType) and batch:
            for doc_li in iter_n(doc_d, n=step):
                yield doc_li
        else:
            if batch:
                doc_li = []
                i = 0
            for _id, doc in doc_d.items():
                doc['_id'] = _id
                _doc = {}
                #_doc.clear()
                _doc.update(doc)
                #if validate:
                #    _doc.validate()
                if batch:
                    doc_li.append(_doc)
                    i += 1
                    if i % step == 0:
                        yield doc_li
                        doc_li = []
                else:
                    yield _doc

            if batch:
                yield doc_li

    def post_update_data(self):
        """Override as needed to perform operations after
           date has been uploaded"""
        pass

    def update_data(self, doc_d, step):
        doc_d = doc_d or self.load_data(data_folder=self.data_folder)
        self.logger.debug("doc_d mem: %s" % sys.getsizeof(doc_d))
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        for doc_li in self.doc_iterator(doc_d, batch=True, step=step):
            self.temp_collection.insert(doc_li, manipulate=False, check_keys=False)
        self.logger.info('Done[%s]' % timesofar(t0))
        self.switch_collection()
        self.post_update_data()

    def generate_doc_src_master(self):
        _doc = {"_id": str(self.name),
                "name": str(self.name), # TODO: remove ?
                "timestamp": datetime.datetime.now()}
        # store mapping
        if hasattr(self, 'get_mapping'):
            _doc['mapping'] = getattr(self, 'get_mapping')()
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
            upload_info['temp_collection'] = self.temp_collection and self.temp_collection.name
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

    def load(self, doc_d=None, update_data=True, update_master=True, test=False, step=10000, progress=None, total=None):
        # sanity check before running
        self.logger.info("Upoading '%s'" % self.name)
        self.check_ready()
        try:
            self.unset_pending_upload()
            if not self.temp_collection:
                self.make_temp_collection()
            self.temp_collection.drop()       # drop all existing records just in case.
            upload = {}
            if total:
                upload = {"progress" : "%s/%s" % (progress,total)}
            self.register_status("uploading",transient=True,upload=upload)
            if update_data:
                self.update_data(doc_d,step)
            if update_master:
                # update src_master collection
                if not test:
                    self.update_master()
            if progress == total:
                self.register_status("success")
        except (KeyboardInterrupt,Exception) as e:
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed",upload={"err": repr(e)})
            raise

    @property
    def collection(self):
        return self.db[self.name]

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

    @property
    def data_folder(self):
        return self.src_doc.get("data_folder")


class NoBatchIgnoreDuplicatedSourceUploader(BaseSourceUploader):
    '''Same as default uploader, but will store records and ignore if
    any duplicated error occuring (use with caution...). Storage
    is done line by line (slow, not using a batch) but preserve order
    of data in input file.
    '''

    def update_data(self, doc_d, step):
        doc_d = doc_d or self.load_data(data_folder=self.data_folder)
        self.logger.debug("doc_d mem: %s" % sys.getsizeof(doc_d))

        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        for doc_li in self.doc_iterator(doc_d, batch=True, step=step):
            try:
                self.temp_collection.insert(doc_li, manipulate=False, check_keys=False)
            except DuplicateKeyError:
                pass
        self.logger.info('Done[%s]' % timesofar(t0))
        self.switch_collection()
        self.post_update_data()


class SourceManager(object):
    '''After registering datasources, manager will orchestrate
    source uploading. Default source uploader is used when none specified
    for a datasource. Otherwise, when registering a datasource, a specific
    datasource can be specfied.
    '''
    __DEFAULT_SOURCE_UPLOADER__ = "biothings.dataload.uploader.BaseSourceUploader"

    def __init__(self, datasource_path="dataload.sources"):
        self.doc_register = {}
        self.conn = get_src_conn()
        self.default_src_path = datasource_path
        self.name = None # name of the datasource (ie. entrez_gene, dbsnp)
        self.main_source = None # main datasource name, might be self.name is no sub-datasource (ie. entrez, dbsnp)

    def get_source_uploader(self,src_module):
        return src_module.__metadata__.get("uploader",self.__class__.__DEFAULT_SOURCE_UPLOADER__)

    def generate_uploader_instance(self,src_module):
        # try to find a uploader class in the module
        uploader_klass = None
        for attr in dir(src_module):
            something = getattr(src_module,attr)
            if type(something) == type and issubclass(something,BaseSourceUploader):
                uploader_klass = something
                logging.debug("Found uploader class '%s'" % uploader_klass)
                break
        if not uploader_klass:
            raise UnknownResource("Can't find an uploader class in module '%s'" % src_module)
        uploader_inst = uploader_klass(
                db_conn=self.conn,
                data_root=config.DATA_ARCHIVE_ROOT
                )
        return uploader_inst

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
                    print("onela %s.%s" % (self.default_src_path,src_data))
                    src_m = importlib.import_module("%s.%s" % (self.default_src_path,src_data))
                except ImportError:
                    logging.error("Can't find module '%s', even in '%s'" % (src_data,self.default_src_path))
                    raise
        elif isinstance(src_data,dict):
            # source is comprised of several other sub sources
            assert len(src_data) == 1, "Should have only one element in source dict '%s'" % src_data
            _, sub_srcs = list(src_data.items())[0]
            for src in sub_srcs:
                self.register_source(src)
            return
        else:
            src_m = src_data
        uploader_inst = self.generate_uploader_instance(src_m)
        if uploader_inst.main_source:
            self.doc_register.setdefault(uploader_inst.main_source,[]).append(uploader_inst)
        else:
            self.doc_register[updloader_inst.name] = uploader_inst
        self.conn.register(uploader_inst.__class__)

    def register_sources(self, sources):
        assert not isinstance(sources,str), "sources argument is a string, should pass a list"
        self.doc_register.clear()
        for src_data in sources:
            self.register_source(src_data)

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

        try:
            if isinstance(uploaders,list):
                # this is a resource composed by several sub-resources
                # let's mention intermediate step (so "success" means all subsources
                # have been uploaded correctly
                for i,one in enumerate(uploaders):
                    one.load(progress=i+1,total=len(uploaders),**kwargs)
            else:
                uploader = uploaders # for the sake of readability...
                uploader.load(**kwargs)
        except Exception as e:
            logging.error("Error while uploading '%s': %s" % (src,e))
            raise

    def __repr__(self):
        return "<%s [%d registered]: %s>" % (self.__class__.__name__,len(self.doc_register), list(self.doc_register.keys()))


class MergerSourceUploader(BaseSourceUploader):
    """
    This uploader will try to merge documents when finding duplicated errors.
    It's useful when data is parsed using iterator. A record can be stored in database,
    then later, another record with the same ID is sent to the db, raising a duplicated error.
    These two documents would have been merged before using a 'put all in memory' parser. 
    Since data is here read line by line, the merge is done while storing
    """

    def update_data(self, doc_d, step):
        doc_d = doc_d or self.load_data(data_folder=self.data_folder)
        self.logger.info("Uploading to the DB...")
        t0 = time.time()
        tinner = time.time()
        aslistofdict = None
        for doc_li in self.doc_iterator(doc_d, batch=True, step=step):
            toinsert = len(doc_li)
            nbinsert = 0
            self.logger.info("Inserting %s records ... " % toinsert)
            try:
                bob = self.temp_collection.initialize_unordered_bulk_op()
                for d in doc_li:
                    aslistofdict = d.pop("__aslistofdict__",None)
                    bob.insert(d)
                res = bob.execute()
                nbinsert += res["nInserted"]
                self.logger.info("OK [%s]" % timesofar(tinner))
            except BulkWriteError as e:
                inserted = e.details["nInserted"]
                nbinsert += inserted
                self.logger.info("Fixing %d records " % len(e.details["writeErrors"]))
                ids = [d["op"]["_id"] for d in e.details["writeErrors"]]
                # build hash of existing docs
                docs = self.temp_collection.find({"_id" : {"$in" : ids}})
                hdocs = {}
                for doc in docs:
                    hdocs[doc["_id"]] = doc
                bob2 = self.temp_collection.initialize_unordered_bulk_op()
                for err in e.details["writeErrors"]:
                    errdoc = err["op"]
                    existing = hdocs[errdoc["_id"]]
                    assert "_id" in existing
                    _id = errdoc.pop("_id")
                    merged = merge_struct(errdoc, existing,aslistofdict=aslistofdict)
                    bob2.find({"_id" : _id}).update_one({"$set" : merged})
                    # update previously fetched doc. if several errors are about the same doc id,
                    # we would't merged things properly without an updated document
                    assert "_id" in merged
                    hdocs[_id] = merged
                    nbinsert += 1

                res = bob2.execute()
                self.logger.info("OK [%s]" % timesofar(tinner))
            assert nbinsert == toinsert, "nb %s to %s" % (nbinsert,toinsert)
            # end of loop so it counts the time spent in doc_iterator
            tinner = time.time()

        self.logger.info('Done[%s]' % timesofar(t0))
        self.switch_collection()
        self.post_update_data()


class DummySourceUploader(BaseSourceUploader):
    """
    Dummy uploader, won't upload any data, assuming data is already there
    but make sure every other bit of information is there for the overall process
    (usefull when online data isn't available anymore)
    """

    def update_data(self, doc_d, step):
        self.logger.info("Dummy uploader, nothing to upload")
        # sanity check, dummy uploader, yes, but make sure data is there
        assert self.collection.count() > 0, "No data found in collection '%s' !!!" % self.name

