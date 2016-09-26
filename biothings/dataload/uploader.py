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
    __collection__ = None      # should be specified individually
    __database__ = config.DATA_SRC_DATABASE
    temp_collection = None     # temp collection is for dataloading


    def make_temp_collection(self):
        '''Create a temp collection for dataloading, e.g., entrez_geneinfo_INEMO.'''
        new_collection = None
        while 1:
            new_collection = self.__collection__ + '_temp_' + get_random_string()
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
                new_name = '_'.join([self.__collection__, 'archive', get_timestamp(), get_random_string()])
                self.collection.rename(new_name, dropTarget=True)
            self.temp_collection.rename(self.__collection__)
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
        _doc = {"_id": str(self.__collection__),
                "name": str(self.__collection__),
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
        # only register time when it's a final state
        if not transient:
            t1 = round(time.time() - self.t0, 0)
            upload_info["time"] = timesofar(self.t0)
            upload_info["time_in_s"] = t1
        self.src_doc.update({"upload" : upload_info})
        self.src_doc.update(extra)
        self.src_dump.save(self.src_doc)

    def load(self, doc_d=None, update_data=True, update_master=True, test=False, step=10000):
        try:
            self.register_status("uploading",transient=True)
            if not self.temp_collection:
                self.make_temp_collection()
            self.temp_collection.drop()       # drop all existing records just in case.

            if update_data:
                self.update_data(doc_d,step)
            if update_master:
                # update src_master collection
                if not test:
                    self.update_master()
            self.register_status("success")
        except (KeyboardInterrupt,Exception) as e:
            import traceback
            self.logger.error(traceback.format_exc())
            self.register_status("failed")
            raise

    @property
    def collection(self):
        return self.db[self.__collection__]

    def prepare_src_dump(self):
        self.src_dump = get_src_dump()
        self.src_doc = self.src_dump.find_one({'_id': self.name.split(".")[0]})
        assert self.src_doc, "Missing information for source '%s' to start upload" % self.name

    def setup_log(self):
        import logging as logging_mod
        if not os.path.exists(self.src_root_folder):
            os.makedirs(self.src_root_folder)
        main_source = self.name.split(".")[0]
        self.logfile = os.path.join(self.src_root_folder, '%s_%s_upload.log' % (main_source,time.strftime("%Y%m%d",self.timestamp.timetuple())))
        fh = logging_mod.FileHandler(self.logfile)
        fh.setFormatter(logging_mod.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        fh.name = "logfile"
        sh = logging_mod.StreamHandler()
        sh.name = "logstream"
        self.logger = logging_mod.getLogger("%s_upload" % main_source)
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
    __sources__ = {}

    __DEFAULT_SOURCE_UPLOADER__ = "biothings.dataload.uploader.DefaultSourceUploader"

    def __init__(self,sources):
        self.__class__.__sources__ = sources
        self.doc_register = {}
        self.conn = get_src_conn()

    def get_source_name(self,src_data):
        if isinstance(src_data,dict):
            return src_data["name"]
        else:
            return src_data

    def get_source_uploader(self,src_data):
        if isinstance(src_data,dict):
            return src_data.get("uploader",self.__class__.__DEFAULT_SOURCE_UPLOADER__)
        else:
            return self.__class__.__DEFAULT_SOURCE_UPLOADER__

    def register_sources(self):
        for src_data in self.__sources__:
            src_name = self.get_source_name(src_data)
            src_uploader = self.get_source_uploader(src_data)
            src_m = importlib.import_module('dataload.sources.' + src_name)
            metadata = src_m.__metadata__
            name = src_name + '_doc'
            # TODO: use standard class() to create new instances
            metadata['load_data'] = src_m.load_data
            metadata['get_mapping'] = src_m.get_mapping
            metadata['conn'] = self.conn
            metadata['name'] = src_name
            metadata['timestamp'] = datetime.datetime.now()
            metadata['t0'] = time.time()
            # src_name can be main_source.sub_source (like entrez.entrez_gene), only keep main source here
            metadata['src_root_folder'] = os.path.join(config.DATA_ARCHIVE_ROOT, src_name.split(".")[0])
            # dynamically load uploader class
            str_mod,str_klass = ".".join(src_uploader.split(".")[:-1]),src_uploader.split(".")[-1]
            mod = importlib.import_module(str_mod)
            klass = getattr(mod,str_klass)
            logging.debug("Source: %s will use uploader %s" % (src_name,klass))
            src_cls = type(name, (klass,), metadata)
            # manually propagate db attr
            src_cls.db = self.conn[src_cls.__database__]
            src_cls.setup_log(src_cls)
            src_cls.prepare_src_dump(src_cls)
            self.doc_register[name] = src_cls
            self.conn.register(src_cls)

    def upload_all(self,**kwargs):
        for src in self.__sources__:
            self.upload_src(src, **kwargs)

    def upload_src(self, src, **kwargs):
        _src = self.doc_register[self.get_source_name(src) + '_doc']()
        _src.load(**kwargs)

