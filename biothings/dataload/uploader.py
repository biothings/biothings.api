import time, sys, os, importlib, copy
import datetime, types
from biothings.utils.common import get_timestamp, get_random_string, timesofar, iter_n
from biothings.utils.mongo import get_src_conn



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

class DocSource(dict):
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
            print("Error: load data first.")

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
        print("doc_d mem: %s" % sys.getsizeof(doc_d))

        print("Uploading to the DB...", end='')
        t0 = time.time()
        for doc_li in self.doc_iterator(doc_d, batch=True, step=step):
            self.temp_collection.insert(doc_li, manipulate=False, check_keys=False)
        print('Done[%s]' % timesofar(t0))
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

    def load(self, doc_d=None, update_data=True, update_master=True, test=False, step=10000):
        if not self.temp_collection:
            self.make_temp_collection()
        self.temp_collection.drop()       # drop all existing records just in case.

        if update_data:
            self.update_data(doc_d,step)
        if update_master:
            # update src_master collection
            if not test:
                self.update_master()

    @property
    def collection(self):
        return self.db[self.__collection__]


class SourceUploader(object):
    __sources__ = {}

    __doc_source_class__ = DocSource

    def __init__(self,sources):
        self.__class__.__sources__ = sources
        self.doc_register = {}
        self.conn = get_src_conn()

    def register_sources(self):
        for src in self.__sources__:
            src_m = importlib.import_module('dataload.sources.' + src)
            metadata = src_m.__metadata__
            name = src + '_doc'
            metadata['load_data'] = src_m.load_data
            metadata['get_mapping'] = src_m.get_mapping
            metadata['conn'] = self.conn
            src_cls = type(name, (self.__class__.__doc_source_class__,), metadata)
            # manually propagate db attr
            src_cls.db = self.conn[src_cls.__database__]
            self.doc_register[name] = src_cls
            self.conn.register(src_cls)

    def upload_all(self,**kwargs):
        for src in self.__sources__:
            print("src: %s" % src)
            self.upload_src(src, **kwargs)

    def upload_src(self, src, **kwargs):
        _src = self.doc_register[src + '_doc']()
        _src.load(**kwargs)

