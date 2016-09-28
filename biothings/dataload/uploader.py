import time, sys, os, importlib, copy
import datetime, types

from pymongo.errors import DuplicateKeyError

from biothings.utils.common import get_timestamp, get_random_string, timesofar, iter_n
from biothings.utils.mongo import get_src_conn, get_src_dump
from biothings import config

logging = config.logger


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

class DefaultSourceUploader(dict):
    '''
    Default datasource uploader. Database storage can be done
    in batch or line by line. Duplicated records aren't not allowed
    '''
    # TODO: fix this delayed import
    from biothings import config
    __database__ = config.DATA_SRC_DATABASE
    temp_collection = None     # temp collection is for dataloading


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
            self.logger.error("Error: load data first.")

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
                _doc = copy.copy(self)
                _doc.clear()
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
        doc_d = doc_d or self.load_data()
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
        if hasattr(self, 'get_mapping'):
            _doc['mapping'] = getattr(self, 'get_mapping')()
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
                'timestamp': self.timestamp,
                'logfile': self.logfile,
                'status': status}

        if transient:
            # record some "in-progress" information
            upload_info['step'] = self.name
            upload_info['collection'] = self.temp_collection and self.temp_collection.name
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
        assert self.src_doc, "Missing information for source '%s' to start upload" % self.main_source

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


class NoBatchIgnoreDuplicatedSourceUploader(DefaultSourceUploader):
    '''Same as default uploader, but will store records and ignore if
    any duplicated error occuring (use with caution...). Storage
    is done line by line (slow, not using a batch) but preserve order
    of data in input file.
    '''

    def update_data(self, doc_d, step):
        doc_d = doc_d or self.load_data()
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


class SourceStorage(object):
    '''After registering datasources, storage will orchestrate
    source uploading. Default source uploader is used when none specified
    for a datasource. Otherwise, when registering a datasource, a specific
    datasource can be specfied.
    '''
    __DEFAULT_SOURCE_UPLOADER__ = "biothings.dataload.uploader.DefaultSourceUploader"

    def __init__(self, datasource_path="dataload.sources"):
        self.doc_register = {}
        self.conn = get_src_conn()
        self.default_src_path = datasource_path
        self.name = None # name of the datasource (ie. entrez_gene, dbsnp)
        self.main_source = None # main datasource name, might be self.name is no sub-datasource (ie. entrez, dbsnp)

    def get_source_uploader(self,src_module):
        return src_module.__metadata__.get("uploader",self.__class__.__DEFAULT_SOURCE_UPLOADER__)

    def add_custom_source_metadata(self,metadata):
        """Subclass to customize source uploader metadata"""
        pass

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
        assert hasattr(src_m,"__metadata__"), "'%s' module has no __metadata__" % src_m
        src_name = src_m.__metadata__["name"]
        main_source = src_m.__metadata__["main_source"]
        src_uploader = self.get_source_uploader(src_m)
        metadata = src_m.__metadata__
        # TODO: use standard class() to create new instances
        metadata['load_data'] = src_m.load_data
        metadata['get_mapping'] = src_m.get_mapping
        metadata['conn'] = self.conn
        metadata['name'] = src_name
        metadata['timestamp'] = datetime.datetime.now()
        metadata['t0'] = time.time()
        # src_name can be main_source.sub_source (like entrez.entrez_gene), only keep main source here
        metadata['src_root_folder'] = os.path.join(config.DATA_ARCHIVE_ROOT, main_source)
        # let subclasses enrich metadata as needed
        self.add_custom_source_metadata(metadata)
        # dynamically load uploader class
        str_mod,str_klass = ".".join(src_uploader.split(".")[:-1]),src_uploader.split(".")[-1]
        mod = importlib.import_module(str_mod)
        klass = getattr(mod,str_klass)
        logging.debug("Source: %s will use uploader %s" % (src_name,klass))
        src_class_name = src_name
        src_cls = type(src_class_name, (klass,), metadata)
        # manually propagate db attr
        src_cls.db = self.conn[src_cls.__database__]
        src_cls.setup_log(src_cls)
        src_cls.prepare_src_dump(src_cls)
        if main_source:
            self.doc_register.setdefault(main_source,[]).append(src_cls)
        else:
            self.doc_register[src_class_name] = src_cls
        self.conn.register(src_cls)

    def register_sources(self, sources):
        for src_data in sources:
            self.register_source(src_data)

    def upload_all(self,**kwargs):
        for src in self.doc_register:
            self.upload_src(src, **kwargs)

    def upload_src(self, src, **kwargs):
        assert "%s" % src in self.doc_register, "'%s' has not been registered first, can't launch upload" % src
        klass = self.doc_register[src]
        if isinstance(klass,list):
            # this is a resource composed by several sub-resources
            # let's mention intermediate step (so "success" means all subsources
            # have been uploaded correctly
            for i,one in enumerate(klass):
                one().load(progress=i,total=len(klass),**kwargs)
        else:
            klass().load(**kwargs)

    def __repr__(self):
        return "<%s [%d registered]: %s>" % (self.__class__.__name__,len(self.doc_register), list(self.doc_register.keys()))

