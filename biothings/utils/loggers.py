import os, time, datetime
import logging
import asyncio
from functools import partial

from .hipchat import hipchat_msg

def setup_default_log(default_logger_name,log_folder):
    # this will affect any logging calls
    logging.basicConfig(level=logging.DEBUG)
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    logfile = os.path.join(log_folder, '%s_%s_hub.log' % (default_logger_name,time.strftime("%Y%m%d",datetime.datetime.now().timetuple())))
    fh = logging.FileHandler(logfile)
    fh.setFormatter(logging.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',datefmt="%H:%M:%S"))
    fh.name = "logfile"
    logger = logging.getLogger(default_logger_name)
    logger.setLevel(logging.DEBUG)
    if not fh.name in [h.name for h in logger.handlers]:
        logger.addHandler(fh)
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
        if record.__dict__.get("notify"):
            loop = asyncio.get_event_loop()
            msg = self.format(record)
            color = self.__class__.colors.get(record.levelno,"gray")
            fut = aioemit()
            asyncio.ensure_future(fut)

