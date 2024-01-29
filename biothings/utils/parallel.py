"""
Utils for running parallel jobs.
"""
from concurrent.futures import ProcessPoolExecutor


def run_jobs_on_parallel(worker, task_list, executor_args=None):
    """
    This method will run multiple workers to handle the task_list, in a process pool,
    which is an easy way to run and manage processes.

    Parameters:
    - worker: a callable, which will be apply for an item of the task_list
    - task_list: a iterable, which contains task data should be processed.
    - executor_args: should be valid parameters for initializing a ProcessPoolExecutor.
    """

    executor_args = executor_args or {}
    with ProcessPoolExecutor(**executor_args) as executor:
        return executor.map(worker, task_list)


def run_jobs_on_ipythoncluster(worker, task_list, shutdown_ipengines_after_done=False):
    import warnings
    warnings.warn(DeprecationWarning("This function is deprecated! Use run_jobs_on_parallel function instead."))

    import os
    import time

    from ipyparallel import Client
    import biothings

    # ipcluster expecting to be in the folder where the app config is
    # because ipcluster will just import this file/module, kind of as if
    # it was building a main script from this lib
    import biothings.config as config

    biothings.config_for_app(config)
    from biothings.utils.common import ask, timesofar

    t0 = time.time()
    rc = Client(config.CLUSTER_CLIENT_JSON)
    lview = rc.load_balanced_view()
    cnt_nodes = len(lview.targets or rc.ids)
    print("\t# nodes in use: {}".format(cnt_nodes))
    lview.block = False
    # move to app path
    lview.map(os.chdir, [config.APP_PATH] * cnt_nodes)
    print("\t# of tasks: {}".format(len(task_list)))
    print("\tsubmitting...", end="")
    job = lview.map_async(worker, task_list)
    print("done.")
    try:
        job.wait_interactive()
    except KeyboardInterrupt:
        # handle "Ctrl-C"
        if ask("\nAbort all submitted jobs?") == "Y":
            lview.abort()
            print("Aborted, all submitted jobs are cancelled.")
        else:
            print("Aborted, but your jobs are still running on the cluster.")
        return

    if len(job.result()) != len(task_list):
        print("WARNING:\t# of results returned ({}) != # of tasks ({}).".format(len(job.result()), len(task_list)))
    print("\ttotal time: {}".format(timesofar(t0)))

    if shutdown_ipengines_after_done:
        print("\tshuting down all ipengine nodes...", end="")
        lview.shutdown()
        print("Done.")
    return job.result()


def collection_partition(src_collection_list, step=100000):
    """This function is deprecated, not used anywhere"""
    import copy

    if not isinstance(src_collection_list, (list, tuple)):
        src_collection_list = [src_collection_list]

    kwargs = {}
    kwargs["limit"] = step
    for src_collection in src_collection_list:
        _kwargs = copy.copy(kwargs)
        _kwargs["src_collection"] = src_collection.name
        _kwargs["src_db"] = src_collection.database.name
        _kwargs["server"] = src_collection.database.connection.host
        _kwargs["port"] = src_collection.database.connection.port

        cnt = src_collection.count()
        for s in range(0, cnt, step):
            __kwargs = copy.copy(_kwargs)
            __kwargs["skip"] = s
            yield __kwargs
