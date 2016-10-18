from biothings.databuild.backend import DocESBackend, DocMongoBackend, DocMemoryBackend
from biothings.utils.es import ESIndexer
from multiprocessing import Pool
from glob import glob
from os.path import abspath
import time

# simple aggregation functions
# Can't pickle lambdas :(
#agg_by_sum = lambda prev, curr: prev + curr
#agg_by_append = lambda prev, curr: prev + [curr]

def agg_by_sum(prev, curr):
    return prev + curr

def agg_by_append(prev, curr):
    return prev + [curr]

class ESIndexer(ESIndexer):
    def doc_feeder(self, step=10000, verbose=True, query=None, scroll='10m', **kwargs):
        no_source = not kwargs.get('_source', True)
        q = query if query else {'query': {'match_all': {}}}
        _q_cnt = self.count(q=q, raw=True)
        n = _q_cnt['count']
        n_shards = _q_cnt['_shards']['total']
        assert n_shards == _q_cnt['_shards']['successful']
        _size = int(step / n_shards)
        assert _size * n_shards == step
        cnt = 0
        t0 = time.time()
        if verbose:
            print('\ttotal docs: {}'.format(n))
            t1 = time.time()

        res = self._es.search(self._index, self._doc_type, body=q,
                              size=_size, search_type='scan', scroll=scroll, **kwargs)
        # double check initial scroll request returns no hits
        assert len(res['hits']['hits']) == 0

        while 1:
            if verbose:
                t1 = time.time()
                if cnt < n:
                    print('\t{}-{}...'.format(cnt+1, min(cnt+step, n)), end='')
            res = self._es.scroll(res['_scroll_id'], scroll=scroll)
            if len(res['hits']['hits']) == 0:
                break
            else:
                for doc in res['hits']['hits']:
                    if no_source:
                        yield doc
                    else:
                        yield doc['_source']
                    cnt += 1
                if verbose:
                    print('done.[%.1f%%,%s]' % (min(cnt, n)*100./n, timesofar(t1)))

        if verbose:
            print("Finished! [{}]".format(timesofar(t0)))

        assert cnt == n, "Error: scroll query terminated early [{}, {}], please retry.\nLast response:\n{}".format(cnt, n, res)

    def get_docs(self, ids, step=10000, **mget_args):
        # chunkify, for iterators
        this_chunk = []
        this_len = 0
        for tid in ids:
            this_chunk.append(tid)
            this_len += 1
            if this_len < step:
                continue
            # process chunk
            chunk_res = self._es.mget(body={"ids": this_chunk}, index=self._index, doc_type=self._doc_type, **mget_args)
            this_chunk = []
            this_len = 0
            if 'docs' not in chunk_res:
                continue
            for doc in chunk_res['docs']:
                if (('found' not in doc) or (('found' in doc) and not doc['found'])):
                    continue
                yield doc
        if this_chunk:
            chunk_res = self._es.mget(body={"ids": this_chunk}, index=self._index, doc_type=self._doc_type, **mget_args)
            if 'docs' not in chunk_res:
                raise StopIteration
            for doc in chunk_res['docs']:
                if (('found' not in doc) or (('found' in doc) and not doc['found'])):
                    continue
                yield doc

 
class DocBackendOptions(object):
    def __init__(self, cls, es_index=None, es_host=None, es_doc_type=None,
                 mongo_target_db=None, mongo_target_collection=None):
        self.cls = cls
        self.es_index = es_index
        self.es_host = es_host
        self.es_doc_type = es_doc_type
        self.mongo_target_db = mongo_target_db
        self.mongo_target_collection = mongo_target_collection
        
class DocESBackend(DocESBackend):
    def query(self, query=None, verbose=False, step=10000, scroll="10m", **kwargs):
        ''' Function that takes a query and returns an iterator to query results. '''
        try:
            return self.target_esidxer.doc_feeder(query=query, verbose=verbose, step=step, scroll=scroll, **kwargs)
        except Exception as e:
            pass
    
    @classmethod
    def create_from_options(cls, options):
        if not options.es_index or not options.es_host or not options.es_doc_type:
            raise Exception("Cannot create backend class from options, ensure that es_index, es_host, and es_doc_type are set")
        return cls(ESIndexer(index=options.es_index, doc_type=options.es_doc_type, es_host=options.es_host))

def run_parallel_by_query(fun, backend_options=None, query=None, agg_function=agg_by_append, 
                        agg_function_init=[], chunk_size=1000000, num_workers=16, outpath=None, 
                        mget_chunk_size=10000, ignore_None=True, **query_kwargs):
    ''' Insert comments here. '''
    
    # Initialize return type
    ret = agg_function_init

    # callback function for each chunk
    def _apply_callback(result):
        #global ret
        ret = agg_function(ret, result)
    
    # assert backend_options is correct
    if not backend_options or not isinstance(backend_options, DocBackendOptions):
        raise Exception("backend_options must be a biothings.databuild.parallel2.DocBackendOptions class")

    # build backend from options
    backend = backend_options.cls.create_from_options(backend_options)

    # normalize path for out files
    if outpath:
        outpath = abspath(outpath)

    chunk_num = 0
    this_chunk = []
    this_chunk_size = 0
    
    # Initialize pool
    p = Pool(processes=num_workers)

    # Should make backend return chunks instead, would be a significant rewrite though...
    for doc in backend.query(query, _source=False, **query_kwargs):
        # chunkify
        this_chunk.append(doc['_id'])
        this_chunk_size += 1
        if this_chunk_size < chunk_size:
            continue
        # apply function to chunk
        p.apply_async(_run_one_chunk_ids_list, args=(chunk_num, this_chunk, fun, backend_options, agg_function, agg_function_init, outpath, mget_chunk_size, ignore_None), callback = _apply_callback)
        this_chunk = []
        this_chunk_size = 0
        chunk_num += 1
    # do final chunk if necessary
    if this_chunk:
        p.apply_async(_run_one_chunk_ids_list, args=(chunk_num, this_chunk, fun, backend_options, agg_function, agg_function_init, outpath, mget_chunk_size, ignore_None), callback = _apply_callback)
    # close pool and wait for completion of all workers
    p.close()
    p.join()
    return ret

def _run_one_chunk_ids_list(chunk_num, chunk, fun, backend_options, agg_function, 
                            agg_function_init, outpath, mget_chunk_size, ignore_None):
    # recreate backend object
    backend = backend_options.cls.create_from_options(backend_options)
   
    # make file handle for this chunk
    if outpath:
        _file = open(outpath + '_{}'.format(chunk_num), 'w')
    # initialize return for this chunk
    ret = agg_function_init
    # iterate through this chunk and run function on every doc
    for doc in backend.mget_from_ids(chunk, step=mget_chunk_size):
        if outpath:
            r = fun(doc, chunk_num, _file)
        else:
            r = fun(doc, chunk_num)
        # aggregate the results
        if r or not ignore_None:
            ret = agg_function(ret, r)
    # close handle
    if outpath and _file:
        _file.close()
    return ret

"""def _run_one_chunk_ids_dir(chunk_num, chunk_path, fun, backend, agg_function, agg_function_init, outpath, mget_chunk_size, ignore_None):


    if isinstance(chunk, str) and os.path.exists(chunk):
        chunk_file = open(chunk, 'r')
        iterator = _file_iterator(chunk_file)
    else:
        chunk_file = None
        iterator = backend.mget_from_ids(chunk, step=mget_chunk_size)
    

    this_chunk = []
    this_chunk_len = 0
    ret = agg_function_init
    for this_id in iterator:
        this_chunk.append(this_id)
        this_chunk_len += 1
        if this_chunk_len < mget_chunk_size:
            continue
        # Chunk full, get the docs from ES with mget and continue this chunk
        chunk_res = backend.mget_from_ids(this_chunk, mget_chunk_size)
        this_chunk_len = 0
        this_chunk = []
        if 'docs' not in chunk_res:
            continue
        ret_dict = _process_chunk(chunk_num, chunk_res, test_conf, files, ret_dict)
    # do partial chunks
    if this_chunk:
        chunk_res = _get_chunk(this_chunk, es, test_conf)
        this_chunk = []
        if 'docs' in chunk_res:
            ret_dict = _process_chunk(chunk_num, chunk_res, test_conf, files, ret_dict)
    # close files
    _close_file_struct(files)
    if chunk_file:
        chunk_file.close()
    return ret_dict

def _process_chunk(chunk_num, chunk_res, test_conf, files, ret_dict):
    for doc in chunk_res['docs']:
        if (('found' not in doc) or (('found' in doc) and not doc['found'])):
            continue
        for (index, test) in enumerate(test_conf.tests):
            # Run this test on the doc
            if index in files:
                test_ret = test.f(doc, chunk_num, files[index])
            else:
                test_ret = test.f(doc, chunk_num)
            if test_ret and test.ret_key:
                # TODO: Maybe should allow an aggregation function...
                ret_dict.setdefault(test.ret_key, []).append(test_ret)
    return ret_dict

def _chunk_iterator(chunk_list):
    for tid in chunk_list:
        yield tid

def _file_iterator(chunk_file):
    for line in chunk_file:
        yield line.strip('\n')
"""
