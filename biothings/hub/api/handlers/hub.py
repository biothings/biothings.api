import asyncio
import datetime

from .base import BaseHandler
from biothings.utils.mongo import get_src_dump


class HubHandler(BaseHandler):

    @asyncio.coroutine
    def get(self):
        self.write({
                "name":"mytaxonomy",
                "description": "blabla",
                "now": datetime.datetime.now(),
                })

class StatsHandler(BaseHandler):

    @asyncio.coroutine
    def get(self):
        src_dump = get_src_dump()
        self.write({})

