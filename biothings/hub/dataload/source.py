import asyncio
import logging

from biothings.utils.dataload import to_boolean


class SourceManager(object):
    """
    Helper class to get information about a datasource,
    whether it has a dumper and/or uploaders associated.
    """

    def __init__(self, dump_manager, upload_manager):
        self.dump_manager = dump_manager
        self.upload_manager = upload_manager

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

            else:
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
            srcs = um.source_info()
            dsrcs = dict([(src["name"],src) for src in srcs])
            for src_name in um.register.keys():
                # collection-only source don't have dumpers and only exist in
                # the uploader manager
                up_info = dsrcs.get(src_name,{"name":src_name})
                if not src_name in dm.register:
                    sources[src_name] = self.sumup_source(up_info)
                if up_info.get("upload"):
                    for subname in up_info["upload"].get("jobs",{}):
                        sources[up_info["name"]].setdefault("upload",{}).setdefault(subname,{})
                        sources[up_info["name"]]["upload"][subname]["uploader"] = up_info["upload"]["jobs"][subname]["uploader"]

        return list(sources.values())

    def get_source(self,name,debug=False):
        dm = self.dump_manager
        um = self.upload_manager
        m = dm or um # whatever available
        if m:
            src = m.source_info(name)
            if not src:
                raise ValueError("No such datasource")
            else:
                return src
        else:
            raise ValueError("No manager available to fetch information")

