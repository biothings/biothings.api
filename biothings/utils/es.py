import copy
import itertools
import json
import re
import time

from elasticsearch import Elasticsearch, NotFoundError, RequestError, TransportError
from elasticsearch import helpers

from biothings.utils.common import iter_n, splitstr
from biothings.utils.dataload import dict_walk

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

class ESIndexer():
    def __init__(self, index, doc_type, es_host, step=10000,
                 number_of_shards=10, number_of_replicas=0, 
                 check_index=True, **kwargs):
        self.es_host = es_host
        self._es = get_es(es_host, **kwargs)
        if check_index:
            # if index is actually an alias, resolve the alias to
            # the real underlying index
            try:
                res = self._es.indices.get_alias(index)
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
        self.step = step   # the bulk size when doing bulk operation.
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
            _res = self._es.count(self._index, self._doc_type, q)
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
        if not self._es.indices.exists(self._index):
            body = {
                'settings': {
                    'number_of_shards': self.number_of_shards,
                    "number_of_replicas": self.number_of_replicas,
                }
            }
            extra_settings = extra_settings or {}
            body["settings"].update(extra_settings)
            if mapping:
                mapping = {"mappings": mapping}
                body.update(mapping)
            self._es.indices.create(index=self._index, body=body)

    @wrapper
    def exists_index(self):
        return self._es.indices.exists(self._index)

    def index(self, doc, id=None, action="index"):       # pylint: disable=redefined-builtin
        '''add a doc to the index. If id is not None, the existing doc will be
           updated.
        '''
        self._es.index(self._index, self._doc_type, doc, id=id, params={"op_type": action})

    def index_bulk(self, docs, step=None, action='index'):
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
            return ndoc
        actions = (_get_bulk(doc) for doc in docs)
        return helpers.bulk(self._es, actions, chunk_size=step)

    def delete_doc(self, id):                # pylint: disable=redefined-builtin
        '''delete a doc from the index based on passed id.'''
        return self._es.delete(self._index, self._doc_type, id)

    def delete_docs(self, ids, step=None):
        '''delete a list of docs in bulk.'''
        index_name = self._index
        doc_type = self._doc_type
        step = step or self.step

        def _get_bulk(_id):
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
        self._es.indices.delete(self._index)

    def update(self, id, extra_doc, upsert=True):          # pylint: disable=redefined-builtin
        '''update an existing doc with extra_doc.
           allow to set upsert=True, to insert new docs.
        '''
        body = {'doc': extra_doc}
        if upsert:
            body['doc_as_upsert'] = True
        return self._es.update(self._index, self._doc_type, id, body)

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
        m = self._es.indices.get_mapping(index=self._index, doc_type=self._doc_type)
        return m[self._index]["mappings"]

    def update_mapping(self, m):
        assert list(m) == [self._doc_type], "Bad mapping format, should have one doc_type, got: %s" % list(m)
        assert 'properties' in m[self._doc_type], "Bad mapping format, no 'properties' key"
        return self._es.indices.put_mapping(index=self._index, doc_type=self._doc_type, body=m)

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
        self._es.indices.put_settings(body, index_name)
        try:
            self._build_index_sequential(collection, verbose, query=query, bulk=bulk, update=update, allow_upsert=True)
        finally:
            # restore some settings after bulk indexing is done.
            body = {
                "index": {
                    "refresh_interval": "1s"              # default settings
                }
            }
            self._es.indices.put_settings(body, index_name)

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

        res = self._es.search(self._index, self._doc_type, body=q,
                              size=_size, search_type='scan', scroll=scroll, **kwargs)
        # double check initial scroll request returns no hits
        # assert len(res['hits']['hits']) == 0
        assert not res['hits']['hits']

        while 1:
            # if verbose:
            #     t1 = time.time()
            res = self._es.scroll(res['_scroll_id'], scroll=scroll)
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
        body = {"indices": self._index}
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
            raise IndexerException("Can't snapshot '%s': %s" % (self._index, e))

    def restore(self, repo_name, snapshot_name, index_name=None, purge=False, body=None):
        index_name = index_name or snapshot_name
        if purge:
            try:
                self._es.indices.get(index_name)
                # if we get there, it exists, delete it
                self._es.indices.delete(index_name)
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
        recov = self._es.indices.recovery(index_name)
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
                mapping[rootk] = map_tpl[typ]
            except Exception:              # pylint: disable=broad-except
                errors.append("Can't find map type %s for key %s", inspect_doc[rootk], rootk)
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
    return db[getattr(db.CONFIG,"HUB_CONFIG_COLLECTION","hub_config")]

def get_source_fullname(col_name):
    pass

def get_last_command():
    conn = get_hub_db_conn().get_conn()
    cmd = get_cmd()
    res = conn.search(cmd.dbname, cmd.colname, {
        "query": {
            "match_all": {}
        },
        "size": 1,
        "sort": [
            {
                "_id": {
                    "order": "desc"
                }
            }
        ]
    })
    if res["hits"]["hits"]:
        return res["hits"]["hits"][0]
    else:
        return None


class Database(IDatabase):

    CONFIG = None   # will be set by bt.utils.hub_db.setup()

    def __init__(self):
        super(Database, self).__init__()
        self.name = self.CONFIG.DATA_HUB_DB_DATABASE
        self.es_host = self.CONFIG.HUB_DB_BACKEND["host"]
        self.cols = {}

    @property
    def address(self):
        return self.es_host

    def setup(self):
        pass

    def get_conn(self):
        return get_es(self.es_host)

    def create_collection(self, colname):
        return self[colname]

    def create_if_needed(self, colname):
        conn = self.get_conn()
        # add dot to make it a special index so it's hidden by default in ES gui
        idxcolname = "%s_%s" % (self.name, colname)
        # it's not usefull to scale internal hubdb
        body = {
            'settings': {
                'number_of_shards': 1,
                "number_of_replicas": 0,
            }
        }
        if not conn.indices.exists(idxcolname):
            conn.indices.create(idxcolname, body=body)
            conn.indices.put_mapping(colname, {"dynamic": True}, index=idxcolname)

    def __getitem__(self, colname):
        if colname not in self.cols:
            self.create_if_needed(colname)
            self.cols[colname] = Collection(colname, self)
        return self.cols[colname]


class Collection(object):

    def __init__(self, colname, db):
        self.colname = colname
        self.db = db

    def get_conn(self):
        return self.db.get_conn()

    @property
    def dbname(self):
        return "%s_%s" % (self.db.name, self.colname)

    @property
    def name(self):
        return self.colname

    @property
    def database(self):
        return self.db

    def find_one(self, *args, **kwargs):
        return self.find(*args, find_one=True)

    def find(self, *args, **kwargs):
        results = []
        query = {}
        # if args and len(args) == 1 and isinstance(args[0], dict) and len(args[0]) > 0:
        if args and len(args) == 1 and isinstance(args[0], dict) and args[0]:
            query = {"query": {"match": args[0]}}
        # it's key/value search, let's iterate
        res = self.get_conn().search(self.dbname, self.colname, query, size=10000)
        for _src in res["hits"]["hits"]:
            doc = {"_id": _src["_id"]}
            doc.update(_src["_source"])
            if "find_one" in kwargs:
                return doc
            else:
                results.append(doc)
        if "find_one" in kwargs:
            # we didn't find it if we get there
            return None
        return results

    def insert_one(self, doc, check_unique=True):
        assert "_id" in doc
        _id = doc.pop("_id")
        res = self.get_conn().index(self.dbname, self.colname, doc, id=_id, refresh=True)
        if check_unique and not res["result"] == "created":
            raise Exception("Couldn't insert document '%s'" % doc)

    def update_one(self, query, what, upsert=False):
        assert (len(what) == 1 and ("$set" in what or "$unset" in what or "$push" in what)), \
               "$set/$unset/$push operators not found"
        doc = self.find_one(query)
        if doc:
            if "$set" in what:
                # parse_dot_fields uses json.dumps internally, we can to make
                # sure everything is serializable first
                what = json.loads(json.dumps(what, default=json_serial))
                what = parse_dot_fields(what["$set"])
                doc = update_dict_recur(doc, what)
            elif "$unset" in what:
                for keytounset in what["$unset"].keys():
                    doc.pop(keytounset, None)
            elif "$push" in what:
                for listkey, elem in what["$push"].items():
                    assert "." not in listkey, "$push not supported for nested keys: %s" % listkey
                    doc.setdefault(listkey, []).append(elem)

            self.save(doc)
        elif upsert:
            assert "_id" in query, "Can't upsert without _id"
            assert "$set" in what, "Upsert needs $set operator (it makes sense...)"
            doc = what["$set"]
            doc["_id"] = query["_id"]
            self.save(doc)

    def update(self, query, what):
        docs = self.find(query)
        for doc in docs:
            self.update_one({"_id": doc["_id"]}, what)

    def save(self, doc):
        return self.insert_one(doc, check_unique=False)

    def replace_one(self, query, doc, *args, **kwargs):
        orig = self.find_one(query)
        if orig:
            self.insert_one(doc, check_unique=False)

    def remove(self, query):
        docs = self.find(query)
        conn = self.get_conn()
        for doc in docs:
            conn.delete(self.dbname, self.colname, id=doc["_id"], refresh=True)

    def count(self):
        return self.get_conn().count(self.dbname, self.colname)["count"]

    def __getitem__(self, _id):
        return self.find_one({"_id": _id})
