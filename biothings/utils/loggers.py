import os, time, datetime
import logging
import asyncio
from functools import partial

from .hipchat import hipchat_msg, hipchat_file

def setup_default_log(default_logger_name,log_folder,level=logging.DEBUG):
    # this will affect any logging calls
    logging.basicConfig(level=level)
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    logfile = os.path.join(log_folder, '%s_%s_hub.log' % (default_logger_name,time.strftime("%Y%m%d",datetime.datetime.now().timetuple())))
    fh = logging.FileHandler(logfile)
    fh.setFormatter(logging.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S"))
    fh.name = "logfile"
    logger = logging.getLogger(default_logger_name)
    logger.setLevel(level)
    if not fh.name in [h.name for h in logger.handlers]:
        logger.addHandler(fh)
    return logger


def get_logger(logger_name,log_folder,handlers=["console","file"],timestamp="%Y%m%d"):
    # this will affect any logging calls
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    if timestamp:
        logfile = os.path.join(log_folder, '%s_%s.log' % (logger_name,time.strftime(timestamp,datetime.datetime.now().timetuple())))
    else:
        logfile = os.path.join(log_folder, '%s.log' % logger_name)
    fmt = logging.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s', datefmt="%H:%M:%S")
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    if "file" in handlers:
        fh = logging.FileHandler(logfile)
        fh.setFormatter(fmt)
        fh.name = "logfile"
        if not fh.name in [h.name for h in logger.handlers]:
            logger.addHandler(fh)
    #if "hipchat" in handlers:
    #    nh = HipchatHandler(config.HIPCHAT_CONFIG)
    #    nh.setFormatter(fmt)
    #    nh.name = "hipchat"
    #    if not nh.name in [h.name for h in logger.handlers]:
    #        logger.addHandler(nh)
    return logger


class HipchatHandler(logging.StreamHandler):

    colors = {logging.DEBUG : "gray",
              logging.INFO : "green",
              logging.WARNING : "yellow",
              logging.ERROR : "red",
              logging.CRITICAL : "purple"}

    def __init__(self,conf={}):
        super(HipchatHandler,self).__init__()
        pass

    def emit(self,record):
        @asyncio.coroutine
        def aioemit():
            fut = yield from loop.run_in_executor(None,partial(
                hipchat_msg,msg,color=color))
            return fut
        def aioshare():
            fut = yield from loop.run_in_executor(None,partial(
                hipchat_file,filepath,message=filepath))
            return fut
        if record.__dict__.get("notify"):
            loop = asyncio.get_event_loop()
            msg = self.format(record)
            color = self.__class__.colors.get(record.levelno,"gray")
            fut = aioemit()
            asyncio.ensure_future(fut)
            if record.__dict__.get("attach"):
                filepath = record.__dict__["attach"]
                fut = aioshare()
                asyncio.ensure_future(fut)


class EventRecorder(logging.StreamHandler):

    def __init__(self, *args, **kwargs):
        super(EventRecorder,self).__init__(*args,**kwargs)
        from biothings.utils.hub_db import get_event
        self.eventcol = get_event()

    def emit(self,record):
        @asyncio.coroutine
        def aioemit(msg):
            def recorded(f):
                res = f.result()
            fut = loop.run_in_executor(None,
                    partial(self.eventcol.save,msg))
            fut.add_done_callback(recorded)
            yield from fut
            return fut
        if record.__dict__.get("notify"):
            try:
                loop = asyncio.get_event_loop()
                msg = {
                        "_id" : record.created,
                        "asctime" : record.asctime,
                        "msg" : record.message,
                        "level" : record.levelname,
                        "name" : record.name,
                        "pid" : record.process,
                        "pname" : record.processName,
                        }
                fut = aioemit(msg)
                asyncio.ensure_future(fut)
            except Exception as e:
                logging.error("Couldn't record event: %s" % e)


