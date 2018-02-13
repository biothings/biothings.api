import asyncio
import logging

from biothings.utils.hub_db import get_data_plugin
from biothings.utils.dataload import to_boolean
from biothings.utils.manager import BaseSourceManager
from biothings.utils.hub_db import get_src_master


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
        self.src_master = get_src_master()
        # honoring BaseSourceManager interface (gloups...-
        self.register = {}

    def sumup_source(self,src,detailed=False):
        """Return minimal info about src"""

        mini = {}
        mini["_id"] = src.get("_id",src["name"])
        mini["name"] = src["name"]
        mini["release"] = src.get("release")
        mini["data_folder"] = src.get("data_folder")

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
            mini["upload"] = {"sources" : {}}
            all_status = set()
            if len(src["upload"]["jobs"]) > 1:
                for job,info in src["upload"]["jobs"].items():
                    mini["upload"]["sources"][job] = {
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
                mini["upload"]["sources"][job] = {
                        "time" : info.get("time"),
                        "status" : info.get("status"),
                        "count" : info.get("count"),
                        "started_at" : info.get("started_at")
                        }
                count += info.get("count") or 0
                mini["upload"]["status"] = info.get("status")
            if src["upload"].get("err"):
                mini["upload"]["error"] = src["upload"]["err"]

        if detailed and src.get("inspect"):
            mini["inspect"] = {"sources" : {}}
            all_status = set()
            for job,info in src["inspect"]["jobs"].items():
                mini["inspect"]["sources"][job] = info["inspect"]

        if detailed:
            m = self.src_master.find_one({"_id":src["_id"]})
            if m:
                # some keys are already present, don't override (even if they should be the same)
                for k in [k for k in m.keys() if not k in ["_id","name","timestamp"]]:
                    mini[k] = m.get(k)

        if src.get("locked"):
            mini["locked"] = src["locked"]
        mini["count"] = count

        return mini

    def get_sources(self,id=None,debug=False,detailed=False):
        dm = self.dump_manager
        um = self.upload_manager
        ids = set()
        if id and id in dm.register:
            ids.add(id)
        elif id and id in um.register:
            ids.add(id)
        else:
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
                        sources[src["name"]] = self.sumup_source(src,detailed)
            # complete with uploader info
            if um:
                src = byusrcs.get(_id)
                if src:
                    # collection-only source don't have dumpers and only exist in
                    # the uploader manager
                    if not src["_id"] in sources:
                        sources[src["_id"]] = self.sumup_source(src,detailed)
                    if src.get("upload"):
                        for subname in src["upload"].get("jobs",{}):
                            sources[src["name"]].setdefault("upload",{"sources" : {}})["sources"].setdefault(subname,{})
                            sources[src["name"]]["upload"]["sources"][subname]["uploader"] = src["upload"]["jobs"][subname]["uploader"]
            # deal with plugin info if any
            dp = bydpsrcs.get(_id)
            if dp:
                dp.pop("_id")
                sources.setdefault(_id,{"data_plugin": {}})
                sources[_id]["data_plugin"] = dp
        if id:
            return list(sources.values()).pop()
        else:
            return list(sources.values())

    def get_source(self,name,debug=False):
        return self.get_sources(id=name,debug=debug,detailed=True)

    def save_mapping(self,name,mapping=None):
        m = self.src_master.find_one({"_id":name}) or {"_id":name}
        m["mapping"] = mapping
        self.src_master.save(m)
