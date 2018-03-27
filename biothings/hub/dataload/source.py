import asyncio
import logging
from pprint import pformat

from biothings.utils.hub_db import get_data_plugin
from biothings.utils.dataload import to_boolean
from biothings.utils.manager import BaseSourceManager
from biothings.utils.hub_db import get_src_master, get_source_fullname, \
                                   get_src_dump


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
        self.src_dump = get_src_dump()
        # honoring BaseSourceManager interface (gloups...-
        self.register = {}

    def set_mapping_src_meta(self, subsrc, mini):
        # get mapping from uploader klass first (hard-coded), then src_master (generated/manual)
        src_meta = {}
        mapping = {}
        origin = None
        try:
            upk = self.upload_manager["%s.%s" % (mini["_id"],subsrc)]
            assert len(upk) == 1, "More than 1 uploader found, can't handle that..."
            upk = upk.pop()
            src_meta = getattr(upk,"__metadata__",{})
            mapping = upk.get_mapping()
            origin = "uploader"
            if not mapping:
                raise AttributeError("Not hard-coded mapping")
        except (IndexError, KeyError, AttributeError) as e:
            logging.debug("Can't find hard-coded mapping, now searching src_master: %s" % e)
            m = self.src_master.find_one({"_id":subsrc})
            mapping = m and m.get("mapping")
            origin = "master"
            # use metadata from upload or reconstitute(-ish)
            src_meta = src_meta or \
                    m and dict([(k,v) for (k,v) in m.items() if not k in ["_id","name","timestamp","mapping"]])
        if mapping:
            mini.setdefault("mapping",{}).setdefault(subsrc,{}).setdefault("mapping",mapping)
            mini.setdefault("mapping",{}).setdefault(subsrc,{}).setdefault("origin",origin)
        if src_meta:
            mini.setdefault("src_meta",{}).setdefault(subsrc,src_meta)

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
                    if info.get("err"):
                         mini["upload"]["sources"][job]["error"] = info["err"]
                    count += info.get("count") or 0
                    all_status.add(info["status"])

                    if detailed:
                        self.set_mapping_src_meta(job,mini)

                if len(all_status) == 1:
                    mini["upload"]["status"] = all_status.pop()
                elif "uploading" in all_status:
                    mini["upload"]["status"] = "uploading"

            # TODO: this is a duplication of above, dealing with different multiplicity
            elif len(src["upload"]["jobs"]) == 1:
                job,info = list(src["upload"]["jobs"].items())[0]
                mini["upload"]["sources"][job] = {
                        "time" : info.get("time"),
                        "status" : info.get("status"),
                        "count" : info.get("count"),
                        "started_at" : info.get("started_at")
                        }
                if info.get("err"):
                     mini["upload"]["sources"][job]["error"] = info["err"]
                if detailed:
                    self.set_mapping_src_meta(job,mini)
                count += info.get("count") or 0
                mini["upload"]["status"] = info.get("status")
            if src["upload"].get("err"):
                mini["upload"]["error"] = src["upload"]["err"]

        if src.get("inspect"):
            mini["inspect"] = {"sources" : {}}
            all_status = set()
            for job,info in src["inspect"]["jobs"].items():
                if not detailed:
                    # remove big inspect data but preserve inspect status/info
                    info.get("inspect",{}).pop("results",None)
                mini["inspect"]["sources"][job] = info


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
            # either no id passed, or doesn't exist
            if id and not len(ids):
                raise ValueError("Source %s doesn't exist" % repr(id))
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
                if dp.get("download",{}).get("err"):
                    dp["download"]["error"] = dp["download"].pop("err")
                sources[_id]["data_plugin"] = dp
        if id:
            return list(sources.values()).pop()
        else:
            return list(sources.values())

    def get_source(self,name,debug=False):
        return self.get_sources(id=name,debug=debug,detailed=True)

    def save_mapping(self, name, mapping=None, dest="master", mode="mapping"):
        logging.debug("Saving mapping for source '%s' destination='%s':\n%s" % (name,dest,pformat(mapping)))
        # either given a fully qualified source or just sub-source
        try:
            subsrc = name.split(".")[1]
        except IndexError:
            subsrc = name
        if dest == "master":
            m = self.src_master.find_one({"_id":subsrc}) or {"_id":subsrc}
            m["mapping"] = mapping
            self.src_master.save(m)
        elif dest == "inspect":
            m = self.src_dump.find_one({"_id":name})
            try:
                m["inspect"]["jobs"][subsrc]["inspect"]["results"][mode] = mapping
                self.src_dump.save(m)
            except KeyError as e:
                raise ValueError("Can't save mapping, document doesn't contain expected inspection data" % e)
        else:
            raise ValueError("Unknow saving destination: %s" % repr(dest))

