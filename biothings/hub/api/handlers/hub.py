import asyncio
import datetime

from .base import BaseHandler


class HubHandler(BaseHandler):

    #@get(_path="/info",_produces=mediatypes.APPLICATION_JSON)
    #def info(self):
    #    """
    #    Return general information about the hub
    #    """
    #    return {"managers": list([m for m in self.managers])}

    @asyncio.coroutine
    def get(self):
        self.write({
                "name":"mytaxonomy",
                "description": "blabla",
                "now": datetime.datetime.now(),
                })
    #@get(_path="/icon",_produces=mediatypes.APPLICATION_JSON)
    #def icon(self):
    #    return {
    #            "favicon" : "/static/hub/biothings_logo.png",
    #            "app_icon" : "/static/hub/biothings_logo.png",
    #            }




