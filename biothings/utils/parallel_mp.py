from glob import glob
from multiprocessing import Pool
from os import cpu_count
from os.path import abspath, exists, isdir, join
from traceback import format_exc

from biothings.utils.backend import DocBackendOptions
from biothings.utils.common import iter_n

# How many cpus on this machine?
DEFAULT_THREADS = cpu_count()


# simple aggregation functions
def agg_by_sum(prev, curr):
    return prev + curr


def agg_by_append(prev, curr):
    if isinstance(curr, list):
        return prev + curr
    return prev + [curr]


# avoid the global variable and the callback function this way
class ParallelResult(object):
    def __init__(self, agg_function, agg_function_init):
        self.res = agg_function_init
        self.agg_function = agg_function

    def aggregate(self, curr):
        self.res = self.agg_function(self.res, curr)


# Handles errors in async apply
class ErrorHandler(object):
    def __init__(self, errpath, chunk_num):
        if errpath:
            self.error_file_path = errpath + "_{}".format(chunk_num)
        else:
            self.error_file_path = None

    def handle(self, exception):
        if self.error_file_path:
            f = open(self.error_file_path, "w")
            f.write(format_exc())
            f.write("\n{}\n".format(str(exception)))
            f.close()


def run_parallel_on_iterable(
    fun,
    iterable,
    backend_options=None,
    agg_function=agg_by_append,
    agg_function_init=[],
    chunk_size=1000000,
    num_workers=DEFAULT_THREADS,
    outpath=None,
    mget_chunk_size=10000,
    ignore_None=True,
    error_path=None,
    **query_kwargs,
):
    """This function will run a user function on all documents in a backend database in parallel using
    multiprocessing.Pool.  The overview of the process looks like this:

    Chunk (into chunks of size "chunk_size") items in iterable, and run the following
    script on each chunk using a multiprocessing.Pool object with "num_workers" processes:
        For each document in list of ids in this chunk (documents retrived in chunks of "mget_chunk_size"):
            Run function "fun" with parameters
            (doc, chunk_num, f <file handle only passed if "outpath" is not None>),
            and aggregate the result with the current results using function "agg_function".

    :param fun:
            The function to run on all documents.  If outpath is NOT specified, fun
            must accept two parameters: (doc, chunk_num), where doc is the backend document,
            and chunk_num is essentially a unique process id.
            If outpath IS specified, an additional open file handle (correctly tagged with
            the current chunk's chunk_num) will also be passed to fun, and thus it
            must accept three parameters: (doc, chunk_num, f)
    :param iterable:
            Iterable of ids.
    :param backend_options:
            An instance of biothings.utils.backend.DocBackendOptions.  This contains
            the options necessary to instantiate the correct backend class (ES, mongo, etc).
    :param agg_function:
            This function aggregates the return value of each run of function fun.  It should take
            2 parameters: (prev, curr), where prev is the previous aggregated result, and curr
            is the output of the current function run.  It should return some value that represents
            the aggregation of the previous aggregated results with the output of the current
            function.
    :param agg_function_init:
            Initialization value for the aggregated result.
    :param chunk_size:
            Length of the ids list sent to each chunk.
    :param num_workers:
            Number of processes that consume chunks in parallel.
            https://docs.python.org/2/library/multiprocessing.html#multiprocessing.pool.multiprocessing.Pool
    :param outpath:
            Base path for output files.  Because function fun can be run many times in parallel, each
            chunk is sequentially numbered, and the output file name for any chunk is outpath_{chunk_num},
            e.g., if outpath is out, all output files will be of the form: /path/to/cwd/out_1,
            /path/to/cwd/out_2, etc.
    :param error_path:
            Base path for error files.  If included, exceptions inside each chunk thread will be printed to
            these files.
    :param mget_chunk_size:
            The size of each mget chunk inside each chunk thread.  In each thread, the ids list
            is consumed by passing chunks to a mget_by_ids function.  This parameter controls the size of
            each mget.
    :param ignore_None:
            If set, then falsy values will not be aggregated (0, [], None, etc) in the aggregation step.
            Default True.

    All other parameters are fed to the backend query.
    """

    ret = ParallelResult(agg_function, agg_function_init)

    # assert backend_options is correct
    if not backend_options or not isinstance(backend_options, DocBackendOptions):
        raise Exception("backend_options must be a biothings.databuild.parallel2.DocBackendOptions class")

    # build backend from options
    backend = backend_options.cls.create_from_options(backend_options)

    # normalize path for out files
    if outpath:
        outpath = abspath(outpath)

    if error_path:
        error_path = abspath(error_path)

    with Pool(processes=num_workers) as p:
        for chunk_num, chunk in enumerate(iter_n(iterable, chunk_size)):
            # apply function to chunk
            p.apply_async(
                _run_one_chunk,
                args=(
                    chunk_num,
                    chunk,
                    fun,
                    backend_options,
                    agg_function,
                    agg_function_init,
                    outpath,
                    mget_chunk_size,
                    ignore_None,
                ),
                callback=ret.aggregate,
                error_callback=ErrorHandler(error_path, chunk_num).handle,
            )
        # close pool and wait for completion of all workers
        p.close()
        p.join()
    return ret.res


def run_parallel_on_ids_file(
    fun,
    ids_file,
    backend_options=None,
    agg_function=agg_by_append,
    agg_function_init=[],
    chunk_size=1000000,
    num_workers=DEFAULT_THREADS,
    outpath=None,
    mget_chunk_size=10000,
    ignore_None=True,
    error_path=None,
    **query_kwargs,
):
    """Implementation of run_parallel_on_iterable, where iterable comes from the lines of a file.

    All parameters are fed to run_on_ids_iterable, except:

    :param ids_file:    Path to file with ids, one per line."""

    def _file_iterator(fh):
        for line in fh:
            yield line.strip("\n")

    ids_file = abspath(ids_file)
    with open(ids_file, "r") as ids_handle:
        return run_parallel_on_iterable(
            fun=fun,
            iterable=_file_iterator(ids_handle),
            backend_options=backend_options,
            agg_function=agg_function,
            agg_function_init=agg_function_init,
            chunk_size=chunk_size,
            num_workers=num_workers,
            outpath=outpath,
            mget_chunk_size=mget_chunk_size,
            ignore_None=ignore_None,
            error_path=error_path,
            **query_kwargs,
        )


# TODO: allow mget args to be passed
def run_parallel_on_query(
    fun,
    backend_options=None,
    query=None,
    agg_function=agg_by_append,
    agg_function_init=[],
    chunk_size=1000000,
    num_workers=DEFAULT_THREADS,
    outpath=None,
    mget_chunk_size=10000,
    ignore_None=True,
    error_path=None,
    full_doc=False,
    **query_kwargs,
):
    """Implementation of run_parallel_on_ids_iterable, where the ids iterable comes from the result of a query
    on the specified backend.

    All parameters are fed to run_parallel_on_ids_iterable, except:

    :param query:       ids come from results of this query run on backend, default: "match_all"
    :param full_doc:    If True, a list of documents is passed to each subprocess, rather than ids that are
                        looked up later.  Should be faster?
                        Unknown how this works with very large query sets...
    """

    def _query_iterator(q):
        for doc in q:
            if full_doc:
                yield doc
            else:
                yield doc["_id"]

    # assert backend_options is correct
    if not backend_options or not isinstance(backend_options, DocBackendOptions):
        raise Exception("backend_options must be a biothings.databuild.parallel2.DocBackendOptions class")

    # build backend from options
    backend = backend_options.cls.create_from_options(backend_options)

    return run_parallel_on_iterable(
        fun=fun,
        iterable=_query_iterator(backend.query(query, _source=full_doc, only_source=False, **query_kwargs)),
        backend_options=backend_options,
        agg_function=agg_function,
        agg_function_init=agg_function_init,
        chunk_size=chunk_size,
        outpath=outpath,
        num_workers=num_workers,
        mget_chunk_size=mget_chunk_size,
        ignore_None=ignore_None,
        error_path=error_path,
        **query_kwargs,
    )


def run_parallel_on_ids_dir(
    fun,
    ids_dir,
    backend_options=None,
    agg_function=agg_by_append,
    agg_function_init=[],
    outpath=None,
    num_workers=DEFAULT_THREADS,
    mget_chunk_size=10000,
    ignore_None=True,
    error_path=None,
    **query_kwargs,
):
    """This function will run function fun on chunks defined by the files in ids_dir.

    All parameters are fed to run_parallel_on_iterable, except:

    :params ids_dir:    Directory containing only files with ids, one per line.  The number of files
                        defines the number of chunks.

    """
    ids_dir = abspath(ids_dir)
    assert isdir(ids_dir)
    return run_parallel_on_iterable(
        fun=fun,
        iterable=glob(join(ids_dir, "*")),
        backend_options=backend_options,
        agg_function=agg_function,
        agg_function_init=agg_function_init,
        chunk_size=1,
        outpath=outpath,
        num_workers=num_workers,
        mget_chunk_size=mget_chunk_size,
        ignore_None=ignore_None,
        error_path=error_path,
        **query_kwargs,
    )


def _run_one_chunk(
    chunk_num, chunk, fun, backend_options, agg_function, agg_function_init, outpath, mget_chunk_size, ignore_None
):
    # iterator if chunk is a file path
    def _file_path_iterator(fp):
        with open(fp, "r") as fh:
            for line in fh:
                yield line.strip("\n")

    # recreate backend object
    backend = backend_options.cls.create_from_options(backend_options)

    # check to see chunk is a list of ids, a list of objects, or a tuple with a path and make
    # the correct iterator for the type
    if isinstance(chunk, list) or isinstance(chunk, tuple):
        if isinstance(chunk[0], str) and exists(chunk[0]):
            iterator = backend.mget_from_ids(_file_path_iterator(chunk[0]), step=mget_chunk_size, only_source=False)
        elif isinstance(chunk[0], str):
            iterator = backend.mget_from_ids(chunk, step=mget_chunk_size, only_source=False)
        elif isinstance(chunk[0], dict):
            iterator = iter(chunk)
        else:
            raise Exception(
                "lists and tuples can contain either strings (containing ids or a file path), or dicts (containing full documents)"
            )
    else:
        raise Exception(
            "chunk can only be a list/tuple of ids, a list/tuple of objects, or a list/tuple with a file path to a list of ids."
        )

    # initialize return for this chunk
    ret = ParallelResult(agg_function, agg_function_init)

    # make file handle for this chunk
    if outpath:
        _file = open(outpath + "_{}".format(chunk_num), "w")

    # iterate through this chunk and run function on every doc
    for doc in iterator:
        # Actually call the function
        if outpath:
            r = fun(doc, chunk_num, _file)
        else:
            r = fun(doc, chunk_num)

        # aggregate the results
        if r or not ignore_None:
            ret.aggregate(r)

    # close handle
    if outpath and _file:
        _file.close()

    return ret.res
