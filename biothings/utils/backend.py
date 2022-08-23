''' Backend access class. '''
from functools import partial

from elasticsearch.exceptions import NotFoundError, TransportError
from pymongo import UpdateOne

from biothings.utils.es import ESIndexer
from biothings import config as btconfig


# Generic base backend
class DocBackendBase(object):
    name = 'Undefined'

    def prepare(self):
        '''if needed, add extra preparation steps here.'''

    @property
    def target_name(self):
        raise NotImplementedError()

    @property
    def version(self):
        raise NotImplementedError()

    def insert(self, doc_li):
        raise NotImplementedError()

    def update(self, id, extra_doc):
        '''update only, no upsert.'''
        raise NotImplementedError()

    def drop(self):
        raise NotImplementedError()

    def get_id_list(self):
        raise NotImplementedError()

    def get_from_id(self, id):
        raise NotImplementedError()

    def finalize(self):
        '''if needed, for example for bulk updates, perform flush
           at the end of updating.
           Final optimization or compacting can be done here as well.
        '''


class DocMemoryBackend(DocBackendBase):
    name = 'memory'

    def __init__(self, target_name=None):
        """target_dict is None or a dict."""
        self.target_dict = {}
        self._target_name = target_name or "unnamed"

    @property
    def target_name(self):
        return self._target_name

    def insert(self, doc_li):
        for doc in doc_li:
            self.target_dict[doc['_id']] = doc

    def update(self, id, extra_doc):
        current_doc = self.target_dict.get(id, None)
        if current_doc:
            current_doc.update(extra_doc)
            self.target_dict[id] = current_doc

    def drop(self):
        self.target_dict = {}

    def get_id_list(self):
        return self.target_dict.keys()

    def get_from_id(self, id):
        return self.target_dict[id]

    def finalize(self):
        '''dump target_dict into a file.'''
        from biothings.utils.common import dump
        dump(self.target_dict, self.target_name + '.pyobj')


class DocMongoBackend(DocBackendBase):
    name = 'mongo'

    def __init__(self, target_db, target_collection=None):
        """target_collection is a pymongo collection object."""
        if callable(target_db):
            self._target_db_provider = target_db
            self._target_db = None
        else:
            self._target_db = target_db
        if target_collection:
            self.target_collection = target_collection

    def __eq__(self, other):
        if not isinstance(other, DocMongoBackend):
            return False
        return self.target_name == other.target_name and \
            self.target_collection.database.name == other.target_collection.database.name and \
            self.target_collection.database.client.address == other.target_collection.database.client.address

    @property
    def target_name(self):
        return self.target_collection.name

    @property
    def version(self):
        import biothings.utils.mongo as mongo
        if self.target_collection.database.name == btconfig.DATA_SRC_DATABASE:
            fulln = mongo.get_source_fullname(self.target_collection.name)
            if not fulln:
                return
            mainsrc = fulln.split(".")[0]
            col = mongo.get_src_dump()
            src = col.find_one({"_id": mainsrc})
            return src.get("release")
        elif self.target_collection.database.name == btconfig.DATA_TARGET_DATABASE:
            col = mongo.get_src_build()
            tgt = col.find_one({"_id": self.target_collection.name})
            if not tgt:
                return
            return tgt.get("_meta", {}).get("build_version")
        else:
            return None

    @property
    def target_db(self):
        if self._target_db is None:
            self._target_db = self._target_db_provider()
        return self._target_db

    def count(self):
        return self.target_collection.count()

    def insert(self, docs):
        try:
            res = self.target_collection.insert_many(documents=docs)
            return len(res.inserted_ids)
        except Exception as e:
            import pickle
            pickle.dump(e, open("err", "wb"))

    def update(self, docs, upsert=False):
        '''if id does not exist in the target_collection,
            the update will be ignored except if upsert is True
        '''
        bulk = []
        for doc in docs:
            bulk.append(UpdateOne({'_id': doc["_id"]}, {"$set": doc}, upsert=upsert))

        if bulk:
            result = self.target_collection.bulk_write(bulk)
            # if doc is the same, it'll be matched but not modified.
            # but for us, it's been processed. if upserted, then it can't be matched
            # before (so matched count doesn't include upserted). finally, it's only update
            # ops, so don't count inserted_count and deleted_count
            return result.matched_count + result.upserted_count
        return 0

    def update_diff(self, diff, extra={}):
        '''update a doc based on the diff returned from diff.diff_doc
            "extra" can be passed (as a dictionary) to add common fields to the
            updated doc, e.g. a timestamp.
        '''
        _updates = {}
        _add_d = dict(list(diff.get('add', {}).items()) + list(diff.get('update', {}).items()))
        if _add_d or extra:
            if extra:
                _add_d.update(extra)
            _updates['$set'] = _add_d
        if diff.get('delete', None):
            # _updates['$unset'] = dict([(x, 1) for x in diff['delete']])    # TODO: remove this line, rewritten using dict comprehension
            _updates['$unset'] = {x: 1 for x in diff['delete']}
        res = self.target_collection.update_one({'_id': diff['_id']}, _updates, upsert=False)
        return res.modified_count

    def drop(self):
        self.target_collection.drop()

    def get_id_list(self):
        return [x['_id'] for x in self.target_collection.find(projection=[], manipulate=False)]

    def get_from_id(self, id):
        return self.target_collection.find_one({"_id": id})

    def mget_from_ids(self, ids, asiter=False):
        '''ids is an id list.
           returned doc list should be in the same order of the
             input ids. non-existing ids are ignored.
        '''
        #this does not return doc in the same order of ids
        cur = self.target_collection.find({'_id': {'$in': ids}})
        if asiter:
            return cur
        else:
            return [doc for doc in cur]

    def count_from_ids(self, ids, step=100000):
        '''return the count of docs matching with input ids
           normally, it does not need to query in batches, but MongoDB
           has a BSON size limit of 16M bytes, so too many ids will raise a
           pymongo.errors.DocumentTooLarge error.
        '''
        total_cnt = 0
        for i in range(0, len(ids), step):
            _ids = ids[i:i + step]
            _cnt = self.target_collection.count_documents({'_id': {'$in': _ids}})
            total_cnt += _cnt
        return total_cnt

    def finalize(self):
        '''flush all pending writes.'''
        # no need to flush, fsync is used for backups. also, this locks the whole
        # database for reads...
        #self.target_collection.database.client.fsync(async=True)

    def remove_from_ids(self, ids, step=10000):
        deleted = 0
        for i in range(0, len(ids), step):
            res = self.target_collection.delete_many({'_id': {'$in': ids[i:i + step]}})
            deleted += res.deleted_count
        return deleted


# backward-compatible
DocMongoDBBackend = DocMongoBackend


class DocESBackend(DocBackendBase):
    name = 'es'

    def __init__(self, esidxer=None):
        """esidxer is an instance of utils.es.ESIndexer class."""
        if type(esidxer) == partial:
            self._target_esidxer_provider = esidxer
            self._target_esidxer = None
        else:
            self._target_esidxer_provider = None
            self._target_esidxer = esidxer

    @property
    def target_esidxer(self):
        if not self._target_esidxer:
            self._target_esidxer = self._target_esidxer_provider()
        return self._target_esidxer

    @property
    def target_name(self):
        return self.target_esidxer._index

    @property
    def target_alias(self):
        try:
            alias_info = self.target_esidxer.get_alias(self.target_name) or {}
            return list(alias_info[self.target_name]["aliases"].keys())[0]
        except Exception:
            return

    @property
    def version(self):
        try:
            mapping = self.target_esidxer.get_mapping_meta()
            if mapping.get("_meta"):
                return mapping["_meta"].get("build_version")
        except NotFoundError:
            # index doesn't even exist
            return None

    def prepare(self, update_mapping=True):
        self.target_esidxer.create_index()
        self.target_esidxer.verify_mapping(update_mapping=update_mapping)

    def count(self):
        try:
            return self.target_esidxer.count()
        except TransportError:
            return None

    def insert(self, doc_li):
        self.target_esidxer.add_docs(doc_li)

    def update(self, id, extra_doc):
        self.target_esidxer.update(id, extra_doc, bulk=True)

    def drop(self):
        from utils.es import IndexMissingException

        conn = self.target_esidxer.conn
        index_name = self.target_esidxer.ES_INDEX_NAME
        index_type = self.target_esidxer.ES_INDEX_TYPE

        #Check if index_type exists
        try:
            conn.get_mapping(index_type, index_name)
        except IndexMissingException:
            return
        return conn.delete_mapping(index_name, index_type)

    def finalize(self):
        conn = self.target_esidxer.conn
        conn.indices.flush()
        conn.indices.refresh()
        self.target_esidxer.optimize()

    def get_id_list(self, step=None):
        return self.target_esidxer.get_id_list(step=step)

    def get_from_id(self, id):
        return self.target_esidxer.get_biothing(id, only_source=True)

    def mget_from_ids(self, ids, step=100000, only_source=True, asiter=True, **kwargs):
        '''ids is an id list. always return a generator'''
        return self.target_esidxer.get_docs(ids, step=step, only_source=only_source, **kwargs)

    def remove_from_ids(self, ids, step=10000):
        self.target_esidxer.delete_docs(ids, step=step)

    def query(self, query=None, verbose=False, step=10000, scroll="10m",
              only_source=True, **kwargs):
        ''' Function that takes a query and returns an iterator to query results. '''
        try:
            return self.target_esidxer.doc_feeder(query=query, verbose=verbose, step=step, scroll=scroll, only_source=only_source, **kwargs)
        except Exception:
            pass

    @classmethod
    def create_from_options(cls, options):
        ''' Function that recreates itself from a DocBackendOptions class.  Probably a needless
        rewrite of __init__... '''
        if not options.es_index or not options.es_host or not options.es_doc_type:
            raise Exception("Cannot create backend class from options, ensure that es_index, es_host, and es_doc_type are set")
        return cls(ESIndexer(index=options.es_index, doc_type=options.es_doc_type, es_host=options.es_host))


class DocBackendOptions(object):
    def __init__(self, cls, es_index=None, es_host=None, es_doc_type=None,
                 mongo_target_db=None, mongo_target_collection=None):
        self.cls = cls
        self.es_index = es_index
        self.es_host = es_host
        self.es_doc_type = es_doc_type
        self.mongo_target_db = mongo_target_db
        self.mongo_target_collection = mongo_target_collection
