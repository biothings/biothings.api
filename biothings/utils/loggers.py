import logging
import asyncio
from functools import partial

from .hipchat import hipchat_msg


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
        if record.__dict__.get("notify"):
            loop = asyncio.get_event_loop()
            msg = self.format(record)
            color = self.__class__.colors.get(record.levelno,"gray")
            fut = loop.run_in_executor(None,partial(
                hipchat_msg,msg,color=color))
            asyncio.ensure_future(fut)

