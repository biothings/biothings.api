import os, time, datetime
import logging
import asyncio
from functools import partial

from .slack import slack_msg

def setup_default_log(default_logger_name,log_folder,level=logging.DEBUG):
    # this will affect any logging calls
    logging.basicConfig(level=level)
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    logfile = os.path.join(log_folder, '%s_%s_hub.log' % (default_logger_name,time.strftime("%Y%m%d",datetime.datetime.now().timetuple())))
    fh = logging.FileHandler(logfile)
    fh.setFormatter(logging.Formatter('%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',
                                      datefmt="%Y-%m-%d %H:%M:%S"))
    fh.name = "logfile"
    logger = logging.getLogger(default_logger_name)
    logger.setLevel(level)
    if not fh.name in [h.name for h in logger.handlers]:
        logger.addHandler(fh)
    return logger


def get_logger(logger_name,log_folder=None,handlers=["console","file","slack"],timestamp="%Y%m%d"):
    """
    Configure a logger object from logger_name and return (logger, logfile)
    """
    from biothings import config as btconfig
    if not log_folder:
        log_folder = btconfig.LOG_FOLDER
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

    if "hipchat" in handlers:
        raise DeprecationWarning("Hipchat is dead...")

    if "slack" in handlers and getattr(btconfig,"SLACK_WEBHOOK",None):
        nh = SlackHandler(btconfig.SLACK_WEBHOOK)
        nh.setFormatter(fmt)
        nh.name = "slack"
        if not nh.name in [h.name for h in logger.handlers]:
            logger.addHandler(nh)

    return (logger, logfile)


class SlackHandler(logging.StreamHandler):

    colors = {logging.DEBUG : "#a1a1a1",
              logging.INFO : "good",
              logging.WARNING : "warning",
              logging.ERROR : "danger",
              logging.CRITICAL : "#7b0099"}

    def __init__(self,webhook):
        super(SlackHandler,self).__init__()
        self.webhook = webhook

    def emit(self,record):
        @asyncio.coroutine
        def aioemit():
            fut = yield from loop.run_in_executor(None,partial(
                slack_msg,self.webhook,msg,color=color))
            return fut
        if record.__dict__.get("notify"):
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # if the logger is running in a thread, there's no asyncio loop there
                # (we usually take it from job_manager.loop, but it's not accessible there)
                # so we use another loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            msg = self.format(record)
            color = self.__class__.colors.get(record.levelno,self.__class__.colors[logging.DEBUG])
            fut = aioemit()
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
        if record.__dict__.get("notify") or record.__dict__.get("event"):
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


