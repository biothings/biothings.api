import copy
import datetime
import itertools
import json
import re
import time
import functools

from elasticsearch import (Elasticsearch, NotFoundError, RequestError,
                           TransportError, ElasticsearchException)
from elasticsearch import helpers
from elasticsearch.serializer import JSONSerializer
from importlib import import_module

from biothings.utils.common import iter_n, splitstr, nan, inf, merge, traverse
from biothings.utils.dataload import dict_walk
from biothings.utils.serializer import to_json

# setup ES logging
import logging
formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
es_logger.addHandler(ch)

es_tracer = logging.getLogger('elasticsearch.trace')
es_tracer.setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
es_tracer.addHandler(ch)

def verify_ids(doc_iter, es_host, index, doc_type=None, step=100000, ):
    '''verify how many docs from input interator/list overlapping with existing docs.'''

    index = index
    doc_type = doc_type
    es = get_es(es_host)
    q = {'query': {'ids': {"values": []}}}
    total_cnt = 0
    found_cnt = 0
    out = []
    for doc_batch in iter_n(doc_iter, n=step):
        id_li = [doc['_id'] for doc in doc_batch]
        # id_li = [doc['_id'].replace('chr', '') for doc in doc_batch]
        q['query']['ids']['values'] = id_li
        xres = es.search(index=index, doc_type=doc_type, body=q, _source=False)
        found_cnt += xres['hits']['total']
        total_cnt += len(id_li)
        out.extend([x['_id'] for x in xres['hits']['hits']])
    return out


def get_es(es_host, timeout=120, max_retries=3, retry_on_timeout=False):
    es = Elasticsearch(es_host, timeout=timeout, max_retries=max_retries,
                       retry_on_timeout=retry_on_timeout)
    return es


def wrapper(func):
    '''this wrapper allows passing index and doc_type from wrapped method.'''
    def outter_fn(*args, **kwargs):
        self = args[0]
        index = kwargs.pop('index', self._index)             # pylint: disable=protected-access
        doc_type = kwargs.pop('doc_type', self._doc_type)    # pylint: disable=protected-access
        self._index = index                                  # pylint: disable=protected-access
        self._doc_type = doc_type                            # pylint: disable=protected-access
        return func(*args, **kwargs)
    outter_fn.__doc__ = func.__doc__
    return outter_fn


class IndexerException(Exception):
    pass

class ESIndex():
    """ An Elasticsearch Index Wrapping A Client.
    Counterpart for pymongo.collection.Collection """

    # a new approach to biothings.utils.es.ESIndexer
    # but not intended to be a replacement in features.

    def __init__(self, client, index_name):
        self.client = client  # synchronous
        self.index_name = index_name  # MUST exist

    @property
    @functools.lru_cache()
    def doc_type(self):
        if int(self.client.info()['version']['number'].split('.')[0]) < 7:
            mappings = self.client.indices.get_mapping(self.index_name)
            mappings = mappings[self.index_name]["mappings"]
            return next(iter(mappings.keys()))
        return None

    # SUBCLASS NOTE &&
    # BEFORE YOU ADD A METHOD UNDER THIS CLASS:

    # An object of this class refers to an existing ES index. All operations
    # should target this index. Do not put uncommon methods here. They belong
    # to the subclasses. This class is to provide a common framework to support
    # Index-specific ES operations, an concept does not exist in the low-level
    # ES library, thus only providing low-level common operations, like doc_type
    # parsing across ES versions for biothings usecases. (single type per index)

class ESIndexer():
    # RETIRING THIS CLASS

    # --
    # Since we don't always directly work on
    # existing actual indices, the index referred here
    # can be an alias or does not exist.

    def __init__(self, index, doc_type='_doc', es_host='localhost:9200',
                 step=500, step_size=10,  # elasticsearch.helpers.bulk
                 number_of_shards=1, number_of_replicas=0,
                 check_index=True, **kwargs):
        self.es_host = es_host
        self._es = get_es(es_host, **kwargs)
        self._host_major_ver = int(self._es.info()['version']['number'].split('.')[0])
        if check_index:
            # if index is actually an alias, resolve the alias to
            # the real underlying index
            try:
                res = self._es.indices.get_alias(index=index)
                # this was an alias
                assert len(res) == 1, "Expecing '%s' to be an alias, but got nothing..." % index
                self._index = list(res.keys())[0]
            except NotFoundError:
                # this was a real index name
                self._index = index
        self._doc_type = None
        if doc_type:
            self._doc_type = doc_type
        else:
            # assuming index exists, get mapping to discover doc_type
            try:
                m = self.get_mapping()
                assert len(m) == 1, "Expected only one doc type, got: %s" % m.keys()
                self._doc_type = list(m).pop()
            except Exception as e:       # pylint: disable=broad-except
                if check_index:
                    logging.info("Failed to guess doc_type: %s", e)
        self.number_of_shards = number_of_shards            # set number_of_shards when create_index
        self.number_of_replicas = int(number_of_replicas)   # set number_of_replicas when create_index
        self.step = step or 500   # the bulk size when doing bulk operation.
        self.step_size = (step_size or 10) * 1048576  # MB -> bytes
        self.s = None      # number of records to skip, useful to continue indexing after an error.

    @wrapper
    def get_biothing(self, bid, only_source=False, **kwargs):
        rawdoc = self._es.get(index=self._index, id=bid, doc_type=self._doc_type, **kwargs)
        if not only_source:
            return rawdoc
        else:
            doc = rawdoc['_source']
            doc["_id"] = rawdoc["_id"]
            return doc

    @wrapper
    def exists(self, bid):
        """return True/False if a biothing id exists or not."""
        try:
            doc = self.get_biothing(bid, stored_fields=None)
            return doc['found']
        except NotFoundError:
            return False

    @wrapper
    def mexists(self, bid_list):
        q = {
            "query": {
                "ids": {
                    "values": bid_list
                }
            }
        }
        res = self._es.search(index=self._index, doc_type=self._doc_type, body=q, stored_fields=None, size=len(bid_list))
        # id_set = set([doc['_id'] for doc in res['hits']['hits']])     # TODO: Confirm this line
        id_set = {doc['_id'] for doc in res['hits']['hits']}
        return [(bid, bid in id_set) for bid in bid_list]

    @wrapper
    def count(self, q=None, raw=False):
        try:
            _res = self._es.count(index=self._index, doc_type=self._doc_type, body=q)
            return _res if raw else _res['count']
        except NotFoundError:
            return None

    @wrapper
    def count_src(self, src):
        if isinstance(src, str):
            src = [src]
        cnt_d = {}
        for _src in src:
            q = {
                "query": {
                    "constant_score": {
                        "filter": {
                            "exists": {"field": _src}
                        }
                    }
                }
            }
            cnt_d[_src] = self.count(q)
        return cnt_d

    @wrapper
    def create_index(self, mapping=None, extra_settings=None):
        if not self._es.indices.exists(index=self._index):
            body = {
                'settings': {
                    'number_of_shards': self.number_of_shards,
                    "number_of_replicas": self.number_of_replicas,
                }
            }
            extra_settings = extra_settings or {}
            body["settings"].update(extra_settings)
            if mapping:

                # the mapping is passed in for elasticsearch 6
                # if the remote server is of elasticsearch version 7 or later
                # drop the doc_type first level key as it is no longer supported
                self._populate_es_version()
                if self._es_version > 6:
                    if len(mapping) == 1 and next(iter(mapping)) not in ('properties', 'dynamic', '_meta'):
                        mapping = next(iter(mapping.values()))

                mapping = {"mappings": mapping}
                body.update(mapping)
            self._es.indices.create(index=self._index, body=body)

    def _populate_es_version(self):
        if not hasattr(self, "_es_version"):
            self._es_version = int(self._es.info()['version']['number'].split('.')[0])

    @wrapper
    def exists_index(self):
        return self._es.indices.exists(index=self._index)

    def index(self, doc, id=None, action="index"):       # pylint: disable=redefined-builtin
        '''add a doc to the index. If id is not None, the existing doc will be
           updated.
        '''
        self._es.index(index=self._index, doc_type=self._doc_type, body=doc, id=id, params={"op_type": action})

    def index_bulk(self, docs, step=None, action='index'):

        self._populate_es_version()
        index_name = self._index
        doc_type = self._doc_type
        step = step or self.step

        def _get_bulk(doc):
            # keep original doc
            ndoc = copy.copy(doc)
            ndoc.update({
                "_index": index_name,
                "_type": doc_type,
                "_op_type": action,
            })
            if self._es_version > 6:
                ndoc.pop("_type")
            return ndoc
        actions = (_get_bulk(doc) for doc in docs)
        num_ok, errors = helpers.bulk(self._es, actions, chunk_size=step, max_chunk_bytes=self.step_size)
        if errors:
            raise ElasticsearchException("%d errors while bulk-indexing: %s" % (len(errors), [str(e) for e in errors]))
        return num_ok, errors

    def delete_doc(self, id):                # pylint: disable=redefined-builtin
        '''delete a doc from the index based on passed id.'''
        return self._es.delete(index=self._index, doc_type=self._doc_type, id=id)

    def delete_docs(self, ids, step=None):
        '''delete a list of docs in bulk.'''
        index_name = self._index
        doc_type = self._doc_type
        step = step or self.step

        def _get_bulk(_id):
            if self._host_major_ver >= 7:
                doc = {
                    '_op_type': 'delete',
                    "_index": index_name,
                    "_id": _id
                }
            else:
                doc = {
                    '_op_type': 'delete',
                    "_index": index_name,
                    "_type": doc_type,
                    "_id": _id
                }
            return doc
        actions = (_get_bulk(_id) for _id in ids)
        return helpers.bulk(self._es, actions, chunk_size=step, stats_only=True, raise_on_error=False)

    def delete_index(self):
        self._es.indices.delete(index=self._index)

    def update(self, id, extra_doc, upsert=True):          # pylint: disable=redefined-builtin
        '''update an existing doc with extra_doc.
           allow to set upsert=True, to insert new docs.
        '''
        body = {'doc': extra_doc}
        if upsert:
            body['doc_as_upsert'] = True
        return self._es.update(index=self._index, doc_type=self._doc_type, id=id, body=body)

    def update_docs(self, partial_docs, upsert=True, step=None, **kwargs):
        '''update a list of partial_docs in bulk.
           allow to set upsert=True, to insert new docs.
        '''
        index_name = self._index
        doc_type = self._doc_type
        step = step or self.step

        def _get_bulk(doc):
            doc = {
                '_op_type': 'update',
                "_index": index_name,
                "_type": doc_type,
                "_id": doc['_id'],
                "doc": doc
            }
            if upsert:
                doc['doc_as_upsert'] = True
            return doc
        actions = (_get_bulk(doc) for doc in partial_docs)
        return helpers.bulk(self._es, actions, chunk_size=step, **kwargs)

    def get_mapping(self):
        """return the current index mapping"""
        m = self._es.indices.get_mapping(index=self._index, doc_type=self._doc_type, include_type_name=True)
        return m[self._index]["mappings"]

    def update_mapping(self, m):
        assert list(m) == [self._doc_type], "Bad mapping format, should have one doc_type, got: %s" % list(m)
        assert 'properties' in m[self._doc_type], "Bad mapping format, no 'properties' key"
        if self._host_major_ver <= 6:
            return self._es.indices.put_mapping(
                index=self._index, doc_type=self._doc_type, body=m
            )
        elif self._host_major_ver == 7:
            return self._es.indices.put_mapping(
                index=self._index, doc_type=self._doc_type, body=m,
                include_type_name=True
            )
        else:
            raise RuntimeError(f"Server Elasticsearch version is {self._host_major_ver} "
                               "which is unsupported when using old ESIndexer class")

    def get_mapping_meta(self):
        """return the current _meta field."""
        m = self.get_mapping()
        doc_type = self._doc_type
        if doc_type is None:
            # fetch doc_type from mapping
            assert len(m) == 1, "More than one doc_type found, not supported when self._doc_type " + \
                                "is not initialized"
            doc_type = list(m.keys())[0]
        return {"_meta": m[doc_type]["_meta"]}

    def update_mapping_meta(self, meta):
        allowed_keys = {'_meta', '_timestamp'}
        # if isinstance(meta, dict) and len(set(meta) - allowed_keys) == 0:
        if isinstance(meta, dict) and not set(meta) - allowed_keys:
            if self._host_major_ver >= 7:
                return self._es.indices.put_mapping(
                    index=self._index,
                    body=meta,
                )
            else:  # not sure if _type needs to be specified
                body = {self._doc_type: meta}
                return self._es.indices.put_mapping(
                    doc_type=self._doc_type,
                    body=body,
                    index=self._index
                )
        else:
            raise ValueError('Input "meta" should have and only have "_meta" field.')

    @wrapper
    def build_index(self, collection, verbose=True, query=None, bulk=True, update=False, allow_upsert=True):
        index_name = self._index
        # update some settings for bulk indexing
        body = {
            "index": {
                "refresh_interval": "-1",              # disable refresh temporarily
                "auto_expand_replicas": "0-all",
            }
        }
        self._es.indices.put_settings(body=body, index=index_name)
        try:
            self._build_index_sequential(collection, verbose, query=query, bulk=bulk, update=update, allow_upsert=True)
        finally:
            # restore some settings after bulk indexing is done.
            body = {
                "index": {
                    "refresh_interval": "1s"              # default settings
                }
            }
            self._es.indices.put_settings(body=body, index=index_name)

            try:
                self._es.indices.flush()
                self._es.indices.refresh()
            except:          # pylint: disable=bare-except  # noqa
                pass

            time.sleep(1)
            src_cnt = collection.count(query)
            es_cnt = self.count()
            if src_cnt != es_cnt:
                raise IndexerException("Total count of documents does not match [{}, should be {}]".format(es_cnt, src_cnt))

            return es_cnt

    def _build_index_sequential(self, collection, verbose=False, query=None, bulk=True, update=False, allow_upsert=True):

        def rate_control(cnt, t):
            delay = 0
            if t > 90:
                delay = 30
            elif t > 60:
                delay = 10
            if delay:
                time.sleep(delay)

        from biothings.utils.mongo import doc_feeder
        src_docs = doc_feeder(collection, step=self.step, s=self.s, batch_callback=rate_control, query=query)
        if bulk:
            if update:
                # input doc will update existing one
                # if allow_upsert, create new one if not exist
                res = self.update_docs(src_docs, upsert=allow_upsert)
            else:
                # input doc will overwrite existing one
                res = self.index_bulk(src_docs)
            # if len(res[1]) > 0:
            if res[1]:
                raise IndexerException("Error: {} docs failed indexing.".format(len(res[1])))
            return res[0]

        else:
            cnt = 0
            for doc in src_docs:
                self.index(doc)
                cnt += 1
            return cnt

    @wrapper
    def optimize(self, max_num_segments=1):
        '''optimize the default index.'''
        params = {
            "wait_for_merge": False,
            "max_num_segments": max_num_segments,
        }
        return self._es.indices.forcemerge(index=self._index, params=params)

    def clean_field(self, field, dryrun=True, step=5000):
        '''remove a top-level field from ES index, if the field is the only field of the doc,
           remove the doc as well.
           step is the size of bulk update on ES
           try first with dryrun turned on, and then perform the actual updates with dryrun off.
        '''
        q = {
            "query": {
                "constant_score": {
                    "filter": {
                        "exists": {
                            "field": field
                        }
                    }
                }
            }
        }
        cnt_orphan_doc = 0
        cnt = 0
        _li = []
        for doc in self.doc_feeder(query=q):
            if set(doc) == {'_id', field}:
                cnt_orphan_doc += 1
                # delete orphan doc
                _li.append({
                    "delete": {
                        "_index": self._index,
                        "_type": self._doc_type,
                        "_id": doc['_id']
                    }
                })
            else:
                # otherwise, just remove the field from the doc
                _li.append({
                    "update": {
                        "_index": self._index,
                        "_type": self._doc_type,
                        "_id": doc['_id']
                    }
                })
                # this script update requires "script.disable_dynamic: false" setting
                # in elasticsearch.yml
                _li.append({"script": 'ctx._source.remove("{}")'.format(field)})

            cnt += 1
            if len(_li) == step:
                if not dryrun:
                    self._es.bulk(body=_li)
                _li = []
        if _li:
            if not dryrun:
                self._es.bulk(body=_li)

        return {"total": cnt, "updated": cnt - cnt_orphan_doc, "deleted": cnt_orphan_doc}

    @wrapper
    def doc_feeder_using_helper(self, step=None, verbose=True, query=None, scroll='10m', **kwargs):
        # verbose unimplemented
        step = step or self.step
        q = query if query else {'query': {'match_all': {}}}
        for rawdoc in helpers.scan(client=self._es, query=q, scroll=scroll, index=self._index,
                                   doc_type=self._doc_type, **kwargs):
            if rawdoc.get('_source', False):
                doc = rawdoc['_source']
                doc["_id"] = rawdoc["_id"]
                yield doc
            else:
                yield rawdoc

    @wrapper
    def doc_feeder(self, step=None, verbose=True, query=None, scroll='10m', only_source=True, **kwargs):
        step = step or self.step
        q = query if query else {'query': {'match_all': {}}}
        _q_cnt = self.count(q=q, raw=True)
        n = _q_cnt['count']
        n_shards = _q_cnt['_shards']['total']
        assert n_shards == _q_cnt['_shards']['successful']
        # Not sure if scroll size is per shard anymore in the new ES...should check this
        _size = int(step / n_shards)
        assert _size * n_shards == step
        cnt = 0
        # t0 = time.time()
        # if verbose:
        #     t1 = time.time()

        res = self._es.search(index=self._index, doc_type=self._doc_type, body=q,
                              size=_size, search_type='scan', scroll=scroll, **kwargs)
        # double check initial scroll request returns no hits
        # assert len(res['hits']['hits']) == 0
        assert not res['hits']['hits']

        while True:
            # if verbose:
            #     t1 = time.time()
            res = self._es.scroll(scroll_id=res['_scroll_id'], scroll=scroll)
            # if len(res['hits']['hits']) == 0:
            if not res['hits']['hits']:
                break
            else:
                for rawdoc in res['hits']['hits']:
                    if rawdoc.get('_source', False) and only_source:
                        doc = rawdoc['_source']
                        doc["_id"] = rawdoc["_id"]
                        yield doc
                    else:
                        yield rawdoc
                    cnt += 1

        assert cnt == n, "Error: scroll query terminated early [{}, {}], please retry.\nLast response:\n{}".format(cnt, n, res)

    @wrapper
    def get_id_list(self, step=None, verbose=True):
        step = step or self.step
        cur = self.doc_feeder(step=step, _source=False, verbose=verbose)
        for doc in cur:
            yield doc['_id']

    @wrapper
    def get_docs(self, ids, step=None, only_source=True, **mget_args):
        ''' Return matching docs for given ids iterable, if not found return None.
            A generator is returned to the matched docs.  If only_source is False,
            the entire document is returned, otherwise only the source is returned. '''
        # chunkify
        step = step or self.step
        for chunk in iter_n(ids, step):
            chunk_res = self._es.mget(body={"ids": chunk}, index=self._index,
                                      doc_type=self._doc_type, **mget_args)
            for rawdoc in chunk_res['docs']:
                if (('found' not in rawdoc) or (('found' in rawdoc) and not rawdoc['found'])):
                    continue
                elif not only_source:
                    yield rawdoc
                else:
                    doc = rawdoc['_source']
                    doc["_id"] = rawdoc["_id"]
                    yield doc

    def find_biggest_doc(self, fields_li, min=5, return_doc=False):     # pylint: disable=redefined-builtin
        """return the doc with the max number of fields from fields_li."""
        for n in range(len(fields_li), min - 1, -1):
            for field_set in itertools.combinations(fields_li, n):
                q = ' AND '.join(["_exists_:" + field for field in field_set])
                q = {'query': {"query_string": {"query": q}}}
                cnt = self.count(q)
                if cnt > 0:
                    if return_doc:
                        res = self._es.search(index=self._index, doc_type=self._doc_type, body=q, size=cnt)
                        return res
                    else:
                        return (cnt, q)

    def snapshot(self, repo, snapshot, mode=None, **params):
        body = {
            "indices": self._index,
            "include_global_state": False
            # there is no reason to include global state in our application
            # we want to separate the staging env from the production env
            # (global state includes index templates and ingest pipeline)
            # but this doesn't mean this setting has to be here
            # maybe move this line to where it belongs later
        }
        if mode == "purge":
            # Note: this works, just for small one when deletion is done instantly
            try:
                self._es.snapshot.get(repo, snapshot)
                # if we can get it, we have to delete it
                self._es.snapshot.delete(repo, snapshot)
            except NotFoundError:
                # ok, nothing to delete/purge
                pass
        try:
            return self._es.snapshot.create(repo, snapshot, body=body, params=params)
        except RequestError as e:
            try:
                err_msg = e.info['error']['reason']
            except KeyError:
                err_msg = e.error
            raise IndexerException("Can't snapshot '%s': %s" % (self._index, err_msg))

    def restore(self, repo_name, snapshot_name, index_name=None, purge=False, body=None):
        index_name = index_name or snapshot_name
        if purge:
            try:
                self._es.indices.get(index=index_name)
                # if we get there, it exists, delete it
                self._es.indices.delete(index=index_name)
            except NotFoundError:
                # no need to delete it,
                pass
        try:
            # this is just about renaming index within snapshot to index_name
            body = {
                "indices": snapshot_name,   # snaphost name is the same as index in snapshot
                "rename_replacement": index_name,
                "ignore_unavailable": True,
                "rename_pattern": "(.+)",
                "include_global_state": True
            }
            return self._es.snapshot.restore(repo_name, snapshot_name, body=body)
        except TransportError as e:
            raise IndexerException("Can't restore snapshot '%s' (does index '%s' already exist ?): %s" %
                                   (snapshot_name, index_name, e))

    def get_repository(self, repo_name):
        try:
            return self._es.snapshot.get_repository(repo_name)
        except NotFoundError:
            raise IndexerException("Repository '%s' doesn't exist" % repo_name)

    def create_repository(self, repo_name, settings):
        try:
            self._es.snapshot.create_repository(repo_name, settings)
        except TransportError as e:
            raise IndexerException("Can't create snapshot repository '%s': %s" % (repo_name, e))

    def get_snapshot_status(self, repo, snapshot):
        return self._es.snapshot.status(repo, snapshot)

    def get_restore_status(self, index_name=None):
        index_name = index_name or self._index
        recov = self._es.indices.recovery(index=index_name)
        if index_name not in recov:
            return {"status": "INIT", "progress": "0%"}
        shards = recov[index_name]["shards"]
        # get all shards status
        shards_status = [s["stage"] for s in shards]
        done = len([s for s in shards_status if s == "DONE"])
        if set(shards_status) == {"DONE"}:
            return {"status": "DONE", "progress": "100%"}
        else:
            return {"status": "IN_PROGRESS", "progress": "%.2f%%" % (done/len(shards_status)*100)}


class MappingError(Exception):
    pass

def generate_es_mapping(inspect_doc, init=True, level=0):
    """Generate an ES mapping according to "inspect_doc", which is
    produced by biothings.utils.inspect module"""
    map_tpl = {
        int: {"type": "integer"},
        bool: {"type": "boolean"},
        float: {"type": "float"},
        str: {"type": "keyword", "normalizer": "keyword_lowercase_normalizer"},    # not splittable (like an ID for instance)
        splitstr: {"type": "text"},
    }
    # inspect_doc, if it's been jsonified, contains keys with type as string,
    # such as "<class 'str'>". This is not a real type and we need to convert them
    # back to actual types. This is transparent if inspect_doc isalready in proper format
    pat = re.compile(r"<class '(\w+)'>")

    def str2type(k):
        if isinstance(k, str):
            mat = pat.findall(k)
            if mat:
                return eval(mat[0])     # actual type
            else:
                return k
        else:
            return k
    inspect_doc = dict_walk(inspect_doc, str2type)

    mapping = {}
    errors = []
    none_type = type(None)
    if init and "_id" not in inspect_doc:
        errors.append("No _id key found, document won't be indexed. (doc: %s)" % inspect_doc)
    for rootk in inspect_doc:
        if rootk == "_id":
            keys = list(inspect_doc[rootk].keys())
            if str in keys and splitstr in keys:
                keys.remove(str)
            if not len(keys) == 1 or (keys[0] != str and keys[0] != splitstr):
                errors.append("_id fields should all be a string type (got: %s)" % keys)
            # it was just a check, it's not part of the mapping
            continue
        if rootk == "_stats":
            continue
        if isinstance(rootk, type(None)):     # if rootk == type(None):
            # value can be null, just skip it
            continue
        # some inspect report have True as value, others have dict (will all have dict eventually)
        if inspect_doc[rootk] is True:
            inspect_doc[rootk] = {}
        keys = list(inspect_doc[rootk].keys())
        # if dict, it can be a dict containing the type (no explore needed) or a dict
        # containing more keys (explore needed)
        if list in keys:
            # we explore directly the list w/ inspect_doc[rootk][list] as param.
            # (similar to skipping list type, as there's no such list type in ES mapping)
            # carefull: there could be list of list, if which case we move further into the structure
            # to skip them
            toexplore = inspect_doc[rootk][list]
            while list in toexplore:
                toexplore = toexplore[list]
            if len(toexplore) > 1:
                # we want to make sure that, whatever the structure, the types involved were the same
                # Exception: None is allowed with other types (translates to 'null' in ES)
                # other_types = set([k for k in toexplore.keys() if k != list and isinstance(k, type) and k is not type(None)])    # TODO: Confirm this line
                other_types = {k for k in toexplore.keys() if k != list and isinstance(k, type) and not isinstance(k, none_type)}
                # some mixes are allowed by ES
                if {int, float}.issubset(other_types):
                    other_types.discard(int)    # float > int
                    toexplore.pop(int)
                if len(other_types) > 1:
                    raise Exception("Mixing types for key '%s': %s" % (rootk, other_types))
            res = generate_es_mapping(toexplore, init=False, level=level+1)
            # is it the only key or do we have more ? (ie. some docs have data as "x", some
            # others have list("x")
            # list was either a list of values (end of tree) or a list of dict. Depending
            # on that, we add "properties" (when list of dict) or not (when list of values)
            if type in set(map(type, inspect_doc[rootk][list])):
                mapping[rootk] = res
            else:
                mapping[rootk] = {"properties": {}}
                mapping[rootk]["properties"] = res
        elif set(map(type, keys)) == {type}:
            # it's a type declaration, no explore
            # typs = list(map(type, [k for k in keys if k is not type(None)]))    # TODO: Confirm this line
            typs = list(map(type, [k for k in keys if not isinstance(k, none_type)]))
            if len(typs) > 1:
                errors.append("More than one type (key:%s,types:%s)" % (repr(rootk), repr(keys)))
            try:
                typ = list(inspect_doc[rootk].keys())
                # ther can still be more than one type, if we have a None combined with
                # the "correct" one. We allow None as a combined type, but we want to ignore
                # it when we want to find the mapping
                if len(typ) == 1:
                    typ = typ[0]
                else:
                    # typ = [t for t in typ if t is not type(None)][0]      # TODO: Confirm this line
                    typ = [t for t in typ if not isinstance(t, none_type)][0]
                if typ is nan or typ is inf:
                    raise TypeError(typ)
                mapping[rootk] = map_tpl[typ]
            except KeyError:
                errors.append("Can't find map type %s for key %s" % (inspect_doc[rootk], rootk))
            except TypeError:
                errors.append("Type %s for key %s isn't allowed in ES mapping" % (typ, rootk))
        elif inspect_doc[rootk] == {}:
            typ = rootk
            return map_tpl[typ]
        else:
            mapping[rootk] = {"properties": {}}
            mapping[rootk]["properties"] = generate_es_mapping(inspect_doc[rootk], init=False, level=level+1)
    if errors:
        raise MappingError("Error while generating mapping", errors)
    return mapping


######################@#
# ES as HUB DB backend #
#@######################
from biothings.utils.hub_db import IDatabase
from biothings.utils.dotfield import parse_dot_fields
from biothings.utils.dataload import update_dict_recur
from biothings.utils.common import json_serial


def get_hub_db_conn():
    return Database()

def get_src_conn():
    return get_hub_db_conn()

def get_src_dump():
    db = Database()
    return db[db.CONFIG.DATA_SRC_DUMP_COLLECTION]

def get_src_master():
    db = Database()
    return db[db.CONFIG.DATA_SRC_MASTER_COLLECTION]

def get_src_build():
    db = Database()
    return db[db.CONFIG.DATA_SRC_BUILD_COLLECTION]

def get_src_build_config():
    db = Database()
    return db[db.CONFIG.DATA_SRC_BUILD_CONFIG_COLLECTION]

def get_data_plugin():
    db = Database()
    return db[db.CONFIG.DATA_PLUGIN_COLLECTION]

def get_api():
    db = Database()
    return db[db.CONFIG.API_COLLECTION]

def get_cmd():
    db = Database()
    return db[db.CONFIG.CMD_COLLECTION]

def get_event():
    db = Database()
    return db[db.CONFIG.EVENT_COLLECTION]

def get_hub_config():
    db = Database()
    return db[getattr(db.CONFIG, "HUB_CONFIG_COLLECTION", "hub_config")]

def get_source_fullname(col_name):
    return col_name

def get_last_command():
    cmds = list(sorted(get_cmd()._read().values(), key=lambda cmd: cmd["_id"]))
    return cmds[-1] if cmds else None


# ES7+ FOR HUB_DB
# IS *NOT* DESIGNED FOR
# MANAGING HEAVY WORKLOADS

class Database(IDatabase):

    CONFIG = None   # will be set by bt.utils.hub_db.setup()

    def __init__(self):
        super(Database, self).__init__()
        self.name = self.CONFIG.DATA_HUB_DB_DATABASE
        self.host = self.CONFIG.HUB_DB_BACKEND["host"]
        self.client = Elasticsearch(self.host, serializer=_HubDBEncoder())

        if not self.client.indices.exists(index=self.name):
            self.client.indices.create(index=self.name, body={
                'settings': {
                    'number_of_shards': 1,
                    "number_of_replicas": 0,
                },
                'mappings': {
                    "enabled": False
                }
            })

    @property
    def address(self):
        return self.host

    # ES API OPS
    # ON "COLLECTION"

    def _exists(self, _id):
        return self.client.exists(self.name, _id)

    def _read(self, _id):
        doc = self.client.get(self.name, _id)
        return doc["_source"]

    def _write(self, _id, doc):
        assert doc.pop("_id", None) in (_id, None)
        self.client.index(self.name, doc, id=_id)
        self.client.indices.refresh(self.name)

    def _modify(self, _id, func):
        doc = self._read(_id)
        doc = func(doc) or doc
        self._write(_id, doc)

    # HIGH LEVEL
    # HUB_DB ABSTRACTION

    def create_collection(self, colname):
        return self[colname]

    def __getitem__(self, colname):
        return Collection(colname, self)


class Collection():

    def __init__(self, colname, db):
        self.name = colname
        self.db = db

        if not self.db._exists(colname):
            self._write({})

    # COLLECTION OPS

    def _read(self):
        return self.db._read(self.name)

    def _write(self, col):
        self.db._write(self.name, col)

    # COLLECTION DOC OPS

    def _exists_one(self, _id):
        return str(_id) in self._read()

    def _write_one(self, doc):
        def func(collection):
            collection[str(doc["_id"])] = doc
        self.db._modify(self.name, func)

    # HUB_DB ABSTRACTION

    def __getitem__(self, _id):
        return self.find_one({"_id": _id})

    def find_one(self, *args, **kwargs):
        results = self.find(*args, **kwargs)
        return results[0] if results else None

    def find(self, filter=None, projection=None, *args, **kwargs):

        if args or kwargs:
            raise NotImplementedError()

        results = []
        logger = logging.getLogger(__name__)
        for doc in self._read().values():
            _doc = dict(traverse(doc))  # dotdict
            _doc.update(dict(traverse(doc, True)))
            for k, v in (filter or {}).items():
                if isinstance(v, dict) and "$exists" in v:
                    logger.error("Ignored filter: {'%s': %s}", k, v)
                    continue
                if _doc.get(k) != v:
                    break
            else:  # no break
                results.append(_pyobj(doc))

        if projection:  # used by BuildManager.build_info
            logger.error("Ignored projection: %s", projection)

        return results

    def insert_one(self, document, *args, **kwargs):

        if args or kwargs:
            raise NotImplementedError()

        if self._exists_one(document["_id"]):
            raise ValueError("Document already exists.")

        self._write_one(document)

    def replace_one(self, filter, replacement, upsert=False, *args, **kwargs):

        if args or kwargs:
            raise NotImplementedError()

        doc = self.find_one(filter) or {}
        if not (doc or upsert):
            raise ValueError("No Match.")

        _id = doc.get("_id") or filter["_id"]
        replacement["_id"] = _id
        self._write_one(replacement)

    # update operations support
    # a subset of mongo operators
    # for partial document editing

    def _update_one(self, doc, update, *args, **kwargs):

        if args or kwargs:
            raise NotImplementedError()

        if not len(update) == 1:
            raise ValueError("Invalid operator.")

        if next(iter(update)) not in ("$set", "$unset", "$push", "$addToSet", "$pull"):
            raise NotImplementedError(next(iter(update)))

        # https://docs.mongodb.com/manual/reference/operator/update/set/
        # https://docs.mongodb.com/manual/reference/operator/update/unset/
        # https://docs.mongodb.com/manual/reference/operator/update/push/
        # https://docs.mongodb.com/manual/reference/operator/update/addToSet/
        # https://docs.mongodb.com/manual/reference/operator/update/pull/

        if "$set" in update:
            _update = json.loads(to_json(update["$set"]))
            _update = parse_dot_fields(_update)
            doc = update_dict_recur(doc, _update)

        elif "$unset" in update:
            for dotk, v in traverse(doc):
                if dotk in update["$unset"]:
                    v["__REMOVE__"] = True
            doc = merge({}, doc)

        elif "$push" in update:
            for key, val in update["$push"].items():
                if "." in key:  # not all mongo operators are fully implemented
                    raise NotImplementedError("nested key in $push: %s" % key)
                doc.setdefault(key, []).append(val)

        elif "$addToSet" in update:
            for key, val in update["$addToSet"].items():
                if "." in key:  # not all mongo operators are fully implemented
                    raise NotImplementedError("nested key in $addToSet: %s" % key)
                field = doc.setdefault(key, [])
                if val not in field:
                    field.append(val)

        else:  # "$pull" in update:
            for key, val in update["$pull"].items():
                if "." in key:  # not all mongo operators are fully implemented
                    raise NotImplementedError("nested key in $pull: %s" % key)
                if not isinstance(val, (str, int)):
                    raise NotImplementedError("value or condition in $pull: %s" % val)
                if isinstance(doc.get(key), list):
                    doc[key][:] = [x for x in doc[key] if x != val]

        self._write_one(doc)

    def update_one(self, filter, update, upsert=False, *args, **kwargs):

        doc = self.find_one(filter) or {}
        if not (doc or upsert):
            raise ValueError("No Match.")

        self._update_one(doc, update, *args, **kwargs)

    def update_many(self, filter, update, upsert=False, *args, **kwargs):

        docs = self.find(filter)
        if not docs and upsert:
            if any("." in k for k in filter):
                raise ValueError("dotfield in upsert.")
            docs = [filter]

        for doc in docs:
            self._update_one(doc, update, *args, **kwargs)

    # DEPRECATED
    # -----------------

    def update(self, *args, **kwargs):
        # In the future,
        # Use replace_one(), update_one(), or update_many() instead.
        self.update_many(*args, **kwargs)

    def save(self, doc, *args, **kwargs):
        # In the future,
        # Use insert_one() or replace_one() instead.
        self._write_one(doc)

    def remove(self, query):
        # In the future,
        # Use delete_one() or delete_many() instead.
        docs = self.find(query)
        collection = self._read()
        for doc in docs:
            del collection[doc["_id"]]
        self._write(collection)

    def count(self):
        # In the future,
        # Use count_documents() or estimated_document_count() instead.
        return len(self._read())

# JSON <-> BSON
# -----------------
# The original author of the biothings.hub decided to have the interface
# of hubdb to model that of MongoDB, the prevailing choice of hub db.
# However, MongoDB stores BSON, extended from JSON to add some optional
# non-JSON-native data types, like dates and binary data, while ES only
# supports JSON documents. Upper layers of biothings.hub has subsequently
# been developed to take full advantage of BSON and specifically have
# stored document values in datetime type in hubdb. Thus, the ES as hubdb
# feature needs to provide the support of these additional data types.

# To achieve generic support of additional data types, non-JSON-serializable
# data by default are serialized to their string representations using "repr".
# For example, datetime objects are stored like datetime.datetime(...).

# Is using "eval" safe? For our internal-use purpose, and combining the fact
# that this module is designed for small demonstrations, better genericity
# currently outweighs the potential security risk, that we may easily support
# serializing additional data types without manually handling each type.
# when the circumstances change, it is advised to reconsider this implementation.


def _pyobj(doc):  # ES doc -> Python object

    for _, _doc in traverse(doc):
        if isinstance(_doc, dict):
            for k, v in list(_doc.items()):
                _doc[k] = _eval(v)
        elif isinstance(_doc, list):
            _doc[:] = map(_eval, _doc)

    return doc


_PY_REPR = re.compile(r"([\w.]+)\(.*\)")


def _eval(v):
    try:
        match = _PY_REPR.match(v)
        if match:
            clsstr = match.group(1)
            modstr = clsstr.rsplit(".", 1)[0]
            return eval(v, {modstr: import_module(modstr)})
    except:
        ...

    return v


class _HubDBEncoder(JSONSerializer):
    TYPES = (
        datetime.datetime,
        # ...
    )

    def default(self, obj):
        if isinstance(obj, self.TYPES):
            return repr(obj)
        return super().default(obj)
