import asyncio
import logging

from biothings.utils.hub_db import get_data_plugin
from biothings.utils.dataload import to_boolean
from biothings.utils.manager import BaseSourceManager


class SourceManager(BaseSourceManager):
    """
    Helper class to get information about a datasource,
    whether it has a dumper and/or uploaders associated.
    """

    def __init__(self, source_list, dump_manager, upload_manager):
        self.source_list = source_list
        self.dump_manager = dump_manager
        self.upload_manager = upload_manager
        self.dump_manager.register_sources(self.source_list)
        self.upload_manager.register_sources(self.source_list)
        # honoring BaseSourceManager interface (gloups...-
        self.register = {}

    def sumup_source(self,src):
        """Return minimal info about src"""

        mini = {}
        mini["_id"] = src.get("_id",src["name"])
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
                    count += info.get("count") or 0
                    all_status.add(info["status"])
                if len(all_status) == 1:
                    mini["upload"]["status"] = all_status.pop()
                elif "uploading" in all_status:
                    mini["upload"]["status"] = "uploading"

            elif len(src["upload"]["jobs"]) == 1:
                job,info = list(src["upload"]["jobs"].items())[0]
                mini["upload"][job] = {
                        "time" : info.get("time"),
                        "status" : info.get("status"),
                        "count" : info.get("count"),
                        "started_at" : info.get("started_at")
                        }
                count += info.get("count") or 0
                mini["upload"]["status"] = info.get("status")
            if src["upload"].get("err"):
                mini["upload"]["error"] = src["upload"]["err"]
        if src.get("locked"):
            mini["locked"] = src["locked"]
        mini["count"] = count

        return mini

    def get_sources(self,debug=False):
        dm = self.dump_manager
        um = self.upload_manager
        ids = set(dm.register)
        ids.update(um.register)
        sources = {}
        bydsrcs = {}
        byusrcs = {}
        bydpsrcs = {}
        plugins = get_data_plugin().find()
        [bydsrcs.setdefault(src["_id"],src) for src in dm.source_info() if dm]
        [byusrcs.setdefault(src["_id"],src) for src in um.source_info() if um]
        [bydpsrcs.setdefault(src["_id"],src) for src in plugins]
        for _id in ids:
            # start with dumper info
            if dm:
                src = bydsrcs.get(_id)
                if src:
                    if debug:
                        sources[src["name"]] = src
                    else:
                        sources[src["name"]] = self.sumup_source(src)
            # complete with uploader info
            if um:
                src = byusrcs.get(_id)
                if src:
                    # collection-only source don't have dumpers and only exist in
                    # the uploader manager
                    if not src["_id"] in sources:
                        sources[src["_id"]] = self.sumup_source(src)
                    if src.get("upload"):
                        for subname in src["upload"].get("jobs",{}):
                            sources[src["name"]].setdefault("upload",{}).setdefault(subname,{})
                            sources[src["name"]]["upload"][subname]["uploader"] = src["upload"]["jobs"][subname]["uploader"]
            # deal with plugin info if any
            dp = bydpsrcs.get(_id)
            if dp:
                dp.pop("_id")
                sources.setdefault(_id,{"data_plugin": {}})
                sources[_id]["data_plugin"] = dp 

        return list(sources.values())

    def get_source(self,name,debug=False):
        dm = self.dump_manager
        um = self.upload_manager
        dp = get_data_plugin().find_one({"_id":name})
        src = {}
        for m in [dm,um]:
            if m:
                dsrc = m.source_info(name)
                if dsrc:
                    src.update(dsrc)
        if dp:
            dp.pop("_id")
            src["data_plugin"] = dp

        return src
