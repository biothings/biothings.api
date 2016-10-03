'''
Backend for storing merged genedoc after building.
Support MongoDB, ES, CouchDB
'''
from biothings.utils.common import get_timestamp, get_random_string


# Generic base backend
class DocBackendBase(object):
    name = 'Undefined'

    def prepare(self):
        '''if needed, add extra preparation steps here.'''
        pass

    def insert(self, doc_li):
        raise NotImplemented

    def update(self, id, extra_doc):
        '''update only, no upsert.'''
        raise NotImplemented

    def drop(self):
        raise NotImplemented

    def get_id_list(self):
        raise NotImplemented

    def get_from_id(self, id):
        raise NotImplemented

    def finalize(self):
        '''if needed, for example for bulk updates, perform flush
           at the end of updating.
           Final optimization or compacting can be done here as well.
        '''
        pass

# Source specific backend (deals with build config, master docs, etc...)
class SourceDocBackendBase(DocBackendBase):

    def __init__(self,build_collection, master_collection, src_db):
        self.build_collection = build_collection
        self.master_collection = master_collection
        self.src_db = src_db
        self._build_config = None
        self.src_masterdocs = None

    def get_build_configuration(self, build):
        raise NotImplementedError("sub-class and implement me")

    def get_src_master_docs(self):
        raise NotImplementedError("sub-class and implement me")

    def validate_sources(self,sources=None):
        raise NotImplementedError("sub-class and implement me")

# Target specific backend
class TargetDocBackend(DocBackendBase):

    def __init__(self,*args,**kwargs):
        super(TargetDocBackend,self).__init__(*args,**kwargs)
        self.target_name = None

    def set_target_name(self,target_name, build_name):
        """
        Create/prepare a target backend, either strictly named "target_name"
        or named derived from "build_name" (for temporary backends)
        """
        self.target_name = target_name or self.generate_target_name(build_name)

    def generate_target_name(self,build_config_name):
        return 'genedoc_{}_{}_{}'.format(build_config_name,
                                         get_timestamp(), get_random_string()).lower()

    def post_merge(self):
        pass

###################
# Implementations #
###################

class DocMemoryBackend(DocBackendBase):
    name = 'memory'

    def __init__(self, target_name=None):
        """target_dict is None or a dict."""
        self.target_dict = {}
        self.target_name = target_name or "unnamed"

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
        self.target_db = target_db
        if target_collection:
            self.target_collection = target_collection

    def count(self):
        return self.target_collection.count()

    def insert(self, doc_li):
        self.target_collection.insert(doc_li, manipulate=False,
                                      check_keys=False, w=0)

    def update(self, id, extra_doc):
        '''if id does not exist in the target_collection,
            the update will be ignored.
        '''
        self.target_collection.update({'_id': id}, {'$set': extra_doc},
                                      manipulate=False, check_keys=False,
                                      upsert=False, w=0)

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
            _updates['$unset'] = dict([(x, 1) for x in diff['delete']])
        self.target_collection.update({'_id': diff['_id']}, _updates,
                                      manipulate=False, check_keys=False,
                                      upsert=False, w=0)

    def drop(self):
        self.target_collection.drop()

    def get_id_list(self):
        return [x['_id'] for x in self.target_collection.find(projection=[], manipulate=False)]

    def get_from_id(self, id):
        return self.target_collection.get_from_id(id)

    def mget_from_ids(self, ids, asiter=False):
        '''ids is an id list.
           returned doc list should be in the same order of the
             input ids. non-existing ids are ignored.
        '''
        #this does not return doc in the same order of ids
        cur = self.target_collection.find({'_id': {'$in': ids}})
        _d = dict([(d['_id'], d) for d in cur])
        doc_li = [_d[_id] for _id in ids if _id in _d]
        del _d
        return iter(doc_li) if asiter else doc_li

    def count_from_ids(self, ids, step=100000):
        '''return the count of docs matching with input ids
           normally, it does not need to query in batches, but MongoDB
           has a BSON size limit of 16M bytes, so too many ids will raise a
           pymongo.errors.DocumentTooLarge error.
        '''
        total_cnt = 0
        for i in range(0, len(ids), step):
            _ids = ids[i:i + step]
            _cnt = self.target_collection.find({'_id': {'$in': _ids}}).count()
            total_cnt += _cnt
        return total_cnt

    def finalize(self):
        '''flush all pending writes.'''
        self.target_collection.database.client.fsync(async=True)

    def remove_from_ids(self, ids, step=10000):
        for i in range(0, len(ids), step):
            self.target_collection.remove({'_id': {'$in': ids[i:i + step]}})

# backward-compatible
DocMongoDBBackend = DocMongoBackend

class SourceDocMongoBackend(SourceDocBackendBase):

    def get_build_configuration(self, build):
        self._build_config = self.build_collection.find_one({'_id' : build})
        return self._build_config

    def validate_sources(self,sources=None):
        assert self._build_config, "'self._build_config' cannot be empty."
        if self.src_masterdocs is None:
            self.src_masterdocs = self.get_src_master_docs()
        if not sources:
            sources = set(self.src_db.collection_names())
            build_conf_src = self._build_config['sources']
        else:
            build_conf_src = collection_list
        # check interseciton between what's needed and what's existing
        for src in build_conf_src:
            assert src in self.src_masterdocs, '"%s" not found in "src_master"' % src
            assert src in sources, '"%s" not an existing collection in "%s"' % (src, self.src_db.name)

    def get_src_master_docs(self):
        return dict([(src['_id'], src) for src in list(self.master_collection.find())])


class DocESBackend(DocBackendBase):
    name = 'es'

    def __init__(self, esidxer=None):
        """esidxer is an instance of utils.es.ESIndexer class."""
        self.target_esidxer = esidxer

    def prepare(self, update_mapping=True):
        self.target_esidxer.create_index()
        self.target_esidxer.verify_mapping(update_mapping=update_mapping)

    def count(self):
        return self.target_esidxer.count()['count']

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

    def get_id_list(self):
        return self.target_esidxer.get_id_list()

    def get_from_id(self, id):
        return self.target_esidxer.get(id)

    def mget_from_ids(self, ids, step=100000):
        '''ids is an id list. always return a generator'''
        return self.target_esidxer.get_docs(ids, step=step)

    def remove_from_ids(self, ids, step=10000):
        self.target_esidxer.delete_docs(ids, step=step)


class DocCouchDBBackend(DocBackendBase):
    name = 'couchdb'

    def __init__(self, target_server=None, db_name=None):
        '''target_server is an instance of Couchdb Server class.'''
        self.target_server = target_server
        self.db_name = db_name
        self._prepare(db_name)

        self._doc_cache = {}

    def _prepare(self, db_name):
        from couchdb import ResourceNotFound
        if db_name:
            try:
                self.target_db = self.target_server[db_name]
            except ResourceNotFound:
                self.target_db = self.target_server.create(db_name)

    def _db_upload(self, doc_li, step=10000, verbose=True):
        import time
        from biothings.utils.common import timesofar
        from biothings.utils.dataload import list2dict, list_itemcnt, listsort

        output = []
        t0 = time.time()
        for i in range(0, len(doc_li), step):
            output.extend(self.target_db.update(doc_li[i:i + step]))
            if verbose:
                print('\t%d-%d Done [%s]...' % (i + 1, min(i + step, len(doc_li)), timesofar(t0)))

        res = list2dict(list_itemcnt([x[0] for x in output]), 0)
        print("Done![%s, %d OK, %d Error]" % (timesofar(t0), res.get(True, 0), res.get(False, 0)))
        res = listsort(list_itemcnt([x[2].args[0] for x in output if x[0] is False]), 1, reverse=True)
        print('\n'.join(['\t%s\t%d' % x for x in res[:10]]))
        if len(res) > 10:
            print("\t%d lines omitted..." % (len(res) - 10))

    def _homologene_trimming(self, species_li):
        '''A special step to remove species not included in <species_li>
           from "homologene" attributes.
           species_li is a list of taxids
        '''
        species_set = set(species_li)
        if self._doc_cache:
            for gid, gdoc in self._doc_cache.iteritems():
                hgene = gdoc.get('homologene', None)
                if hgene:
                    _genes = hgene.get('genes', None)
                    if _genes:
                        _genes_filtered = [g for g in _genes if g[0] in species_set]
                        hgene['genes'] = _genes_filtered
                        gdoc['homologene'] = hgene
                        self._doc_cache[gid] = gdoc

    def prepare(self):
        self._prepare(self.db_name)

    def insert(self, doc_li):
        self.target_db.update(doc_li)

    def update(self, id, extra_doc):
        if not self._doc_cache:
            self._doc_cache = dict([(item.id, item.doc) for item in self.target_db.view('_all_docs', include_docs=True)])
        current_doc = self._doc_cache.get(id, None)
        if current_doc:
            current_doc.update(extra_doc)
            self._doc_cache[id] = current_doc

    def drop(self):
        from couchdb import ResourceNotFound
        try:
            self.target_server.delete(self.db_name)
        except ResourceNotFound:
            pass

    def finalize(self):
        if len(self._doc_cache) > 0:
            #do homologene trimming for nine species mygene.info current supported.
            species_li = [9606, 10090, 10116, 7227, 6239, 7955, 3702, 8364, 9823]
            self._homologene_trimming(species_li)
            #perform final updates now
            #self.target_db.update(self._doc_cache.values())
            print("Now doing the actual updating...")
            self._db_upload(self._doc_cache.values())
            self._doc_cache = {}
        self.target_db.commit()
        self.target_db.compact()

    def get_id_list(self):
        return iter([item.id for item in self.target_db.view('_all_docs', include_docs=False)])

    def get_from_id(self, id):
        return self.target_db[id]



class TargetDocMongoBackend(TargetDocBackend,DocMongoBackend):

    def set_target_name(self,target_name=None, build_name=None):
        super(TargetDocMongoBackend,self).set_target_name(target_name,build_name)
        self.target_collection = self.target_db[self.target_name]


class TargetDocESBackend(TargetDocBackend, DocESBackend):

    def __init__(self,*args,**kwargs):
        raise NotImplementedError("ES backend for building/merging isn't implemented")

    def set_target_name(self,name):
        raise NotImplementedError("Unsupported")
        self.target_esidxer.ES_INDEX_NAME = name
        self.target_esidxer._mapping = self.get_mapping()

    def post_merge(self):
        self.update_mapping_meta()

    def update_mapping_meta(self):
        '''updating _meta field of ES mapping data, including index stats, versions.
           This is for DocESBackend only.
        '''
        _meta = {}
        src_version = self.get_src_version()
        if src_version:
            _meta['src_version'] = src_version
        if getattr(self, '_stats', None):
            _meta['stats'] = self._stats

        if _meta:
            self.target.target_esidxer.update_mapping_meta({'_meta': _meta})

    def get_src_version(self):
        src_dump = get_src_dump(self.src_db.client)
        src_version = {}
        for src in src_dump.find():
            version = src.get('release', src.get('timestamp', None))
            if version:
                src_version[src['_id']] = version
        return src_version

