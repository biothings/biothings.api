'''
Utils to compare two list of gene documents
'''
import os
import time
import os.path
from .common import timesofar, dump, get_timestamp, filter_dict
from .backend import DocMongoDBBackend
from ..hub.databuild.backend import create_backend
from .es import ESIndexer
from .jsondiff import make as jsondiff


def diff_doc(doc_1, doc_2, exclude_attrs=['_timestamp']):
    diff_d = {'update': {},
              'delete': [],
              'add': {}}
    if exclude_attrs:
        doc_1 = filter_dict(doc_1,exclude_attrs)
        doc_2 = filter_dict(doc_2,exclude_attrs)
    for attr in set(doc_1) | set(doc_2):
        if exclude_attrs and attr in exclude_attrs:
            continue
        if attr in doc_1 and attr in doc_2:
            _v1 = doc_1[attr]
            _v2 = doc_2[attr]
            if _v1 != _v2:
                diff_d['update'][attr] = _v2
        elif attr in doc_1 and attr not in doc_2:
            diff_d['delete'].append(attr)
        else:
            diff_d['add'][attr] = doc_2[attr]
    if diff_d['update'] or diff_d['delete'] or diff_d['add']:
        return diff_d


def full_diff_doc(doc_1, doc_2, exclude_attrs=['_timestamp']):
    diff_d = {'update': {},
              'delete': [],
              'add': {}}
    if exclude_attrs:
        doc_1 = filter_dict(doc_1,exclude_attrs)
        doc_2 = filter_dict(doc_2,exclude_attrs)
    for attr in set(doc_1) | set(doc_2):
        if exclude_attrs and attr in exclude_attrs:
            continue
        if attr in doc_1 and attr in doc_2:
            _v1 = doc_1[attr]
            _v2 = doc_2[attr]
            difffound = False
            if isinstance(_v1, dict) and isinstance(_v2, dict):
                if full_diff_doc(_v1, _v2, exclude_attrs):
                    difffound = True
            elif isinstance(_v1, list) and isinstance(_v2, list):
                # there can be unhashable/unordered dict in these lists
                for i in _v1:
                    if i not in _v2:
                        difffound = True
                        break
                # check the other way
                if not difffound:
                    for i in _v2:
                        if i not in _v1:
                            difffound = True
                            break
            elif _v1 != _v2:
                difffound = True

            if difffound:
                diff_d['update'][attr] = _v2

        elif attr in doc_1 and attr not in doc_2:
            diff_d['delete'].append(attr)
        else:
            diff_d['add'][attr] = doc_2[attr]
    if diff_d['update'] or diff_d['delete'] or diff_d['add']:
        return diff_d

def two_docs_iterator(b1, b2, id_list, step=10000, verbose=False):
    t0 = time.time()
    n = len(id_list)
    for i in range(0, n, step):
        t1 = time.time()
        if verbose:
            print("Processing %d-%d documents..." % (i + 1, min(i + step, n)), end='')
        _ids = id_list[i:i+step]
        iter1 = sorted([d for d in b1.mget_from_ids(_ids, asiter=True)],key=lambda a: a["_id"])
        iter2 = sorted([d for d in b2.mget_from_ids(_ids, asiter=True)],key=lambda a: a["_id"])
        for doc1, doc2 in zip(iter1, iter2):
            yield doc1, doc2
        if verbose:
            print('Done.[%.1f%%,%s]' % (i*100./n, timesofar(t1)))
    if verbose:
        print("="*20)
        print('Finished.[total time: %s]' % timesofar(t0))


def _diff_doc_worker(args):
    _b1, _b2, ids, _path = args
    import biothings.utils.diff
    import importlib
    importlib.reload(biothings.utils.diff)
    from biothings.utils.diff import _diff_doc_inner_worker, get_backend

    b1 = get_backend(*_b1)
    b2 = get_backend(*_b2)

    _updates = _diff_doc_inner_worker(b1, b2, ids)
    return _updates


def _diff_doc_inner_worker(b1, b2, ids, fastdiff=False, diff_func=full_diff_doc):
    '''if fastdiff is True, only compare the whole doc,
       do not traverse into each attributes.
    '''
    _updates = []
    for doc1, doc2 in two_docs_iterator(b1, b2, ids):
        assert doc1['_id'] == doc2['_id'], repr((ids, len(ids)))
        if fastdiff:
            if doc1 != doc2:
                _updates.append({'_id': doc1['_id']})
        else:
            _diff = diff_func(doc1, doc2)
            if _diff:
                _diff['_id'] = doc1['_id']
                _updates.append(_diff)
    return _updates

def diff_docs_jsonpatch(b1, b2, ids, fastdiff=False, exclude_attrs=[]):
    '''if fastdiff is True, only compare the whole doc,
       do not traverse into each attributes.
    '''
    _updates = []
    for doc1, doc2 in two_docs_iterator(b1, b2, ids):
        assert doc1['_id'] == doc2['_id'], "Different ids: '%s' != '%s'" % \
                (doc1['_id'], doc2['_id'])
        if exclude_attrs:
            doc1 = filter_dict(doc1,exclude_attrs)
            doc2 = filter_dict(doc2,exclude_attrs)
        if fastdiff:
            if doc1 != doc2:
                _updates.append(doc1['_id'])
        else:
            _patch = jsondiff(doc1, doc2)
            if _patch:
                _diff = {}
                _diff['patch'] = _patch
                _diff['_id'] = doc1['_id']
                _updates.append(_diff)
    return _updates




# TODO: move to mongodb backend class
def get_mongodb_uri(backend):
    opt = backend.target_collection.database.client._MongoClient__options.credentials
    username = opt and opt.username or None
    password = opt and opt.password or None
    dbase = opt and opt.source or None
    uri = "mongodb://"
    if username:
        if password:
            uri += "%s:%s@" % (username,password)
        else:
            uri += "%s@" % username
    host,port = backend.target_collection.database.client.address
    uri += "%s:%s" % (host,port)
    uri += "/%s" % (dbase or backend.target_collection.database.name)
    #uri += "/%s" % backend.target_collection.name
    print("uri: %s" % uri)
    return uri


def diff_collections(b1, b2, use_parallel=True, step=10000):
    """
    b1, b2 are one of supported backend class in databuild.backend.
    e.g.,
        b1 = DocMongoDBBackend(c1)
        b2 = DocMongoDBBackend(c2)
    """

    id_s1 = set(b1.get_id_list())
    id_s2 = set(b2.get_id_list())
    print("Size of collection 1:\t", len(id_s1))
    print("Size of collection 2:\t", len(id_s2))

    id_in_1 = id_s1 - id_s2
    id_in_2 = id_s2 - id_s1
    id_common = id_s1 & id_s2
    print("# of docs found only in collection 1:\t", len(id_in_1))
    print("# of docs found only in collection 2:\t", len(id_in_2))
    print("# of docs found in both collections:\t", len(id_common))

    print("Comparing matching docs...")
    _updates = []
    if len(id_common) > 0:
        if not use_parallel:
            _updates = _diff_doc_inner_worker(b1, b2, list(id_common))
        else:
            from .parallel import run_jobs_on_ipythoncluster
            _path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0] + "/.."
            id_common = list(id_common)
            _b1 = (get_mongodb_uri(b1), b1.target_collection.database.name, b1.target_name, b1.name)
            _b2 = (get_mongodb_uri(b2), b2.target_collection.database.name, b2.target_name, b2.name)
            task_li = [(_b1, _b2, id_common[i: i + step], _path) for i in range(0, len(id_common), step)]
            job_results = run_jobs_on_ipythoncluster(_diff_doc_worker, task_li)
            _updates = []
            if job_results:
                for res in job_results:
                    _updates.extend(res)
            else:
                print("Parallel jobs failed or were interrupted.")
                return None

        print("Done. [{} docs changed]".format(len(_updates)))

    _deletes = []
    if len(id_in_1) > 0:
        _deletes = sorted(id_in_1)

    _adds = []
    if len(id_in_2) > 0:
        _adds = sorted(id_in_2)

    changes = {'update': _updates,
               'delete': _deletes,
               'add': _adds}
    return changes


def get_backend(uri, db, col, bk_type):
    if bk_type != "mongodb":
        raise NotImplemented("Backend type '%s' not supported" % bk_type)
    from biothings.utils.mongo import MongoClient
    colobj = MongoClient(uri)[db][col]
    return DocMongoDBBackend(colobj)


def diff_collections_batches(b1, b2, result_dir, step=10000):
    '''
    b2 is new collection, b1 is old collection
    '''
    DIFFFILE_PATH = '/home/kevinxin/diff_result/'
    DATA_FOLDER = os.path.join(DIFFFILE_PATH, result_dir)
    if not os.path.exists(DATA_FOLDER):
        os.mkdir(DATA_FOLDER)
    data_new = doc_feeder(b2.target_collection, step=step, inbatch=True, fields=[])
    data_old = doc_feeder(b1.target_collection, step=step, inbatch=True, fields=[])
    cnt = 0
    cnt_update = 0
    cnt_add = 0
    cnt_delete = 0

    for _batch in data_new:
        cnt += 1
        id_list_new = [_doc['_id'] for _doc in _batch]
        docs_common = b1.target_collection.find({'_id': {'$in': id_list_new}}, projection=[])
        ids_common = [_doc['_id'] for _doc in docs_common]
        id_in_new = list(set(id_list_new) - set(ids_common))
        _updates = []
        if len(ids_common) > 0:
            _updates = diff_docs_jsonpatch(b1, b2, list(ids_common), fastdiff=True)
        file_name = DATA_FOLDER + '/' + str(cnt) + '.pyobj'
        _result = {'add': id_in_new,
                   'update': _updates,
                   'delete': [],
                   'source': b2.target_collection.name,
                   'timestamp': get_timestamp()}
        if len(_updates) != 0 or len(id_in_new) != 0:
            dump(_result, file_name)
            print("(Updated: {}, Added: {})".format(len(_updates), len(id_in_new)), end='')
            cnt_update += len(_updates)
            cnt_add += len(id_in_new)
    print("Finished calculating diff for the new collection. Total number of docs updated: {}, added: {}".format(cnt_update, cnt_add))
    print("="*100)
    for _batch in data_old:
        cnt += 1
        id_list_old = [_doc['_id'] for _doc in _batch]
        docs_common = b2.target_collection.find({'_id': {'$in': id_list_old}}, projection=[])
        ids_common = [_doc['_id'] for _doc in docs_common]
        id_in_old = list(set(id_list_old)-set(ids_common))
        file_name = DATA_FOLDER + '/' + str(cnt) + '.pyobj'
        _result = {'delete': id_in_old,
                   'add': [],
                   'update': [],
                   'source': b2.target_collection.name,
                   'timestamp': get_timestamp()}
        if len(id_in_old) != 0:
            dump(_result, file_name)
            print("(Deleted: {})".format(len(id_in_old)), end='')
            cnt_delete += len(id_in_old)
    print("Finished calculating diff for the old collection. Total number of docs deleted: {}".format(cnt_delete))
    print("="*100)
    print("Summary: (Updated: {}, Added: {}, Deleted: {})".format(cnt_update, cnt_add, cnt_delete))

