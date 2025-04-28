import logging as loggingmod
import os
import pickle

# from biothings import config
from biothings.utils.loggers import get_logger


def upload_worker(name, storage_class, loaddata_func, col_name, batch_size, batch_num, *args, **kwargs):
    """
    Pickable job launcher, typically running from multiprocessing.
    storage_class will instantiate with col_name, the destination
    collection name. loaddata_func is the parsing/loading function,
    called with `*args`.
    """
    data = []
    db = kwargs.get("db", None)
    max_batch_num = kwargs.get("max_batch_num", None)
    try:
        data = loaddata_func(*args)
        if isinstance(storage_class, tuple):
            klass_name = "_".join([k.__class__.__name__ for k in storage_class])
            storage = type(klass_name, storage_class, {})(None, col_name, loggingmod)
        else:
            storage = storage_class(db, col_name, loggingmod)
        return storage.process(data, batch_size, max_batch_num)
    except Exception as gen_exc:
        logger_name = "%s_batch_%s" % (name, batch_num)
        logger, logfile = get_logger(logger_name)
        logger.exception(gen_exc)
        logger.error(
            "Parameters:\nname=%s\nstorage_class=%s\n" % (name, storage_class)
            + "loaddata_func=%s\ncol_name=%s\nbatch_size=%s\n" % (loaddata_func, col_name, batch_size)
            + "args=%s" % repr(args)
        )

        # when logfile is None, we're in CLI mode, so we just use the current directory
        pickfile = os.path.join(os.path.dirname(logfile) if logfile else ".", "%s.pick" % logger_name)
        try:
            pickle_traceback_blob = {
                "exc": gen_exc,
                "params": {"name": name, "storage_class": storage_class},
                "loaddata_func": repr(loaddata_func),  # loaddata_func may not be pickle-able
                "col_name": col_name,
                "batch_size": batch_size,
                "args": args,
            }
            with open(pickfile, "wb") as pickle_handle:
                pickle.dump(pickle_traceback_blob, pickle_handle)
        except (TypeError, pickle.PicklingError) as pickling_error:
            logger.warning("Could not pickle batch errors: %s", pickling_error)
        raise gen_exc
