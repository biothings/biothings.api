import asyncio
import logging
import tornado.web

from .base import BaseHandler
from biothings.utils.dataload import to_boolean


class SourceHandler(BaseHandler):

    def sumup_source(self,src):
        """Return minimal info about src"""

        mini = {
                "name" : src["name"],
                "release" : src.get("release"),
                "dumper" : src["dumper"]}
        if src.get("locked"):
            mini["locked"] = src["locked"]
        if src.get("download"):
            mini["download"] = {
                    "status" : src["download"]["status"],
                    "time" : src["download"].get("time"),
                    "started_at" : src["download"]["started_at"]
                    }
        count = 0
        if src.get("upload"):
            mini["upload"] = {}
            all_status = set()
            if len(src["upload"]["jobs"]) > 1:
                for job,info in src["upload"]["jobs"].items():
                    mini["upload"][job] = {
                            "time" : info.get("time"),
                            "status" : info["status"],
                            "count" : info.get("count"),
                            "started_at" : info["started_at"]
                            }
                    count += info.get("count",0)
                    all_status.add(info["status"])
                if len(all_status) == 1:
                    mini["upload"]["status"] = all_status.pop()
                elif "uploading" in all_status:
                    mini["upload"]["status"] = "uploading"

            else:
                job,info = list(src["upload"]["jobs"].items())[0]
                mini["upload"][job] = {
                        "time" : info.get("time"),
                        "status" : info["status"],
                        "count" : info.get("count"),
                        "started_at" : info["started_at"]
                        }
                count += info.get("count",0)
                mini["upload"]["status"] = info["status"]
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
        if name:
            self.write(self.get_source(name,debug))
        else:
            self.write(self.get_sources(debug))

