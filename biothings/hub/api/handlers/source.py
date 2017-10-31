import asyncio
import logging
import tornado.web
from collections import OrderedDict

from .base import BaseHandler
from biothings.utils.dataload import to_boolean


class SourceHandler(BaseHandler):

    def sumup_source(self,src):
        """Return minimal info about src"""

        mini = OrderedDict()
        mini["name"] = src["name"]
        mini["release"] = src.get("release")
        if src.get("download"):
            mini["download"] = {
                    "status" : src["download"].get("status"),
                    "time" : src["download"].get("time"),
                    "started_at" : src["download"].get("started_at")
                    }
            mini["download"]["dumper"] = src["download"].get("dumper",{})
            if src["download"].get("err"):
                mini["download"]["error"] = src["download"]["err"]
        count = 0
        if src.get("upload"):
            mini["upload"] = {}
            all_status = set()
            if len(src["upload"]["jobs"]) > 1:
                for job,info in src["upload"]["jobs"].items():
                    mini["upload"][job] = {
                            "time" : info.get("time"),
                            "status" : info.get("status"),
                            "count" : info.get("count"),
                            "started_at" : info.get("started_at")
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
                        "status" : info.get("status"),
                        "count" : info.get("count"),
                        "started_at" : info.get("started_at")
                        }
                count += info.get("count",0)
                mini["upload"]["status"] = info.get("status")
            if src["upload"].get("err"):
                mini["upload"]["error"] = src["upload"]["err"]
        if src.get("locked"):
            mini["locked"] = src["locked"]
        mini["count"] = count

        return mini

    def get_sources(self,debug=False):
        dm = self.managers.get("dump_manager")
        um = self.managers.get("upload_manager")
        sources = {}
        if dm:
            srcs = dm.source_info()
            if debug:
                for src in srcs:
                    sources[src["name"]] = src
            else:
                for src in srcs:
                    sources[src["name"]] = self.sumup_source(src)
        # complete with uploader info
        if um:
            for src_name in um.register.keys():
                # collection-only source don't have dumpers and only exist in
                # the uploader manager
                up_info = um.source_info(src_name)
                if not src_name in dm.register:
                    sources[src_name] = self.sumup_source(up_info)
                for subname in up_info["upload"]["jobs"]:
                    sources[up_info["name"]].setdefault("upload",{}).setdefault(subname,{})
                    sources[up_info["name"]]["upload"][subname]["uploader"] = up_info["upload"]["jobs"][subname]["uploader"]


        return list(sources.values())

    def get_source(self,name,debug=False):
        raise NotImplementedError()
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


class DumpSourceHandler(BaseHandler):

    def post(self,name):
        dm = self.managers.get("dump_manager")
        dm.dump_src(name)
        self.write({"dump" : name})

class UploadSourceHandler(BaseHandler):

    def post(self,name):
        um = self.managers.get("upload_manager")
        um.upload_src(name)
        self.write({"upload" : name})

