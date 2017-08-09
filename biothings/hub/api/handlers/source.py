import asyncio
import logging
import tornado.web

from .base import BaseHandler
from biothings.utils.dataload import to_boolean


class SourceHandler(BaseHandler):

    def sumup_source(self,src):
        """Return minimal info about src"""

        mini = {
                "_id" : src["_id"],
                "release" : src["release"],
                "dumper" : "???"}
        if src.get("download"):
            mini["download"] = {
                    "status" : src["download"]["status"],
                    "time" : src["download"]["time"]}
        count = 0
        if src.get("upload"):
            mini["upload"] = {}
            if len(src["upload"]["jobs"]) > 1:
                for job,info in src["upload"]["jobs"].items():
                    mini["upload"][job] = {
                            "time" : info["time"],
                            "status" : info["status"],
                            "count" : info["count"]}
                    count += info["count"]
            else:
                job,info = list(src["upload"]["jobs"].items())[0]
                mini["upload"][job] = {
                        "time" : info["time"],
                        "status" : info["status"],
                        "count" : info["count"]}
                count += info["count"]
        mini["count"] = count

        return mini

    def get_sources(self,debug=False):
        dm = self.managers.get("dump_manager")
        um = self.managers.get("upload_manager")
        sources = []
        if dm:
            srcs = dm.source_info()
            if debug:
                sources.extend(srcs)
            else:
                for src in srcs:
                    sources.append(self.sumup_source(src))
        #if um:
        #    sources.extend(um.source_info())
        return sources

    def get_source(self,name,debug=False):
        dm = self.managers.get("dump_manager")
        um = self.managers.get("upload_manager")
        m = dm or um # whatever available
        if m:
            src = m.source_info(name)
            if not src:
                raise tornado.web.HTTPError(404,reason="No such datasource")
            else:
                return src
        else:
            raise tornado.web.HTTPError(500,reason="No manager available to fetch information")


    @asyncio.coroutine
    def get(self,name=None):
        debug = to_boolean(self.get_query_argument("debug",False))
        logging.error("debug %s" % debug)
        if name:
            self.write(self.get_source(name,debug))
        else:
            self.write(self.get_sources(debug))

    #@get(_path="/source/{name}",_types=[str],_produces=mediatypes.APPLICATION_JSON)
    #def get_one_source(self,name):
    #    logging.error("name: %s" % name)
    #    return self.get_source(name)

