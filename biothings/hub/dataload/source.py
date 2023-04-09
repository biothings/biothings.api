import importlib
import logging
import os
import sys
import types
from pprint import pformat

from biothings.utils.hub_db import get_data_plugin, get_src_dump, get_src_master
from biothings.utils.manager import BaseSourceManager


class SourceManager(BaseSourceManager):
    """
    Helper class to get information about a datasource,
    whether it has a dumper and/or uploaders associated.
    """

    def __init__(self, source_list, dump_manager, upload_manager, data_plugin_manager):
        self._orig_source_list = source_list
        self.source_list = None
        self.dump_manager = dump_manager
        self.upload_manager = upload_manager
        self.data_plugin_manager = data_plugin_manager
        self.reload()
        self.src_master = get_src_master()
        self.src_dump = get_src_dump()
        # honoring BaseSourceManager interface (gloups...-
        self.register = {}

    def reload(self):
        # clear registers
        self.dump_manager.register.clear()
        self.upload_manager.register.clear()
        # re-eval source list (so if it's a string, it'll re-discover sources)
        self.source_list = self.find_sources(self._orig_source_list)
        self.dump_manager.register_sources(self.source_list)
        self.upload_manager.register_sources(self.source_list)

    def find_sources(self, paths):
        sources = []

        if not type(paths) == list:
            paths = [paths]

        def eval_one_source(one_path):
            if "/" in one_path:
                # it's path to directory
                # expecting
                if one_path not in sys.path:
                    logging.info("Adding '%s' to python path" % one_path)
                    sys.path.insert(0, one_path)
                for d in os.listdir(one_path):
                    if d.endswith("__pycache__"):
                        continue
                    sources.append(d)
            else:
                # assuming it's path to a python module (oath.to.module)
                sources.append(one_path)

        def eval_one_root(root):
            logging.debug("Discovering sources in %s" % root)
            # root is a module path where sources can be found
            rootdir, __init__ = os.path.split(root.__file__)
            for srcdir in os.listdir(rootdir):
                if srcdir.endswith("__pycache__"):
                    continue
                srcpath = os.path.join(rootdir, srcdir)
                if os.path.isdir(srcpath):
                    srcmod_str = "%s.%s" % (root.__name__, srcdir)
                    sources.append(srcmod_str)

        for path in paths:
            if type(path) == str:
                eval_one_source(path)
            elif isinstance(path, types.ModuleType):
                eval_one_root(path)

        # clean with only those which can be imported
        sources = set(sources)
        for s in [s for s in sources]:
            try:
                importlib.import_module(s)
            except Exception as e:
                logging.error("Failed to discover source '%s': %s" % (s, e))
                sources.remove(s)

        logging.info("Found sources: %s" % sorted(sources))
        return sources

    def set_mapping_src_meta(self, subsrc, mini):
        # get mapping from uploader klass first (hard-coded), then src_master (generated/manual)
        src_meta = {}
        mapping = {}
        origin = None
        try:
            upk = self.upload_manager["%s.%s" % (mini["_id"], subsrc)]
            assert len(upk) == 1, "More than 1 uploader found, can't handle that..."
            upk = upk.pop()
            src_meta = upk.__metadata__["src_meta"]
            mapping = upk.get_mapping()
            origin = "uploader"
            if not mapping:
                raise AttributeError("Not hard-coded mapping")
        except (IndexError, KeyError, AttributeError) as e:
            logging.debug("Can't find hard-coded mapping, now searching src_master: %s", e)
            m = self.src_master.find_one({"_id": subsrc})
            mapping = m and m.get("mapping")
            origin = "master"
            # use metadata from upload or reconstitute(-ish)
            src_meta = (
                src_meta
                or m
                and dict([(k, v) for (k, v) in m.items() if k not in ["_id", "name", "timestamp", "mapping"]])
            )
        if mapping:
            mini.setdefault("mapping", {}).setdefault(subsrc, {}).setdefault("mapping", mapping)
            mini.setdefault("mapping", {}).setdefault(subsrc, {}).setdefault("origin", origin)
        if src_meta:
            mini.setdefault("__metadata__", {}).setdefault(subsrc, src_meta)

    def sumup_source(self, src, detailed=False):
        """Return minimal info about src"""

        mini = {}
        mini["_id"] = src.get("_id", src["name"])
        mini["name"] = src["name"]

        if src.get("download"):
            mini["download"] = {
                "status": src["download"].get("status"),
                "time": src["download"].get("time"),
                "started_at": src["download"].get("started_at"),
                "release": src["download"].get("release"),
                "data_folder": src["download"].get("data_folder"),
            }
            mini["download"]["dumper"] = src["download"].get("dumper", {})
            if src["download"].get("err"):
                mini["download"]["error"] = src["download"]["err"]
            if src["download"].get("tb"):
                mini["download"]["traceback"] = src["download"]["tb"]

        count = 0
        if src.get("upload"):
            mini["upload"] = {"sources": {}}
            for job, info in src["upload"]["jobs"].items():
                mini["upload"]["sources"][job] = {
                    "time": info.get("time"),
                    "status": info.get("status"),
                    "count": info.get("count"),
                    "started_at": info.get("started_at"),
                    "release": info.get("release"),
                    "data_folder": info.get("data_folder"),
                }
                if info.get("err"):
                    mini["upload"]["sources"][job]["error"] = info["err"]
                if info.get("tb"):
                    mini["upload"]["sources"][job]["traceback"] = info["tb"]
                count += info.get("count") or 0
                if detailed:
                    self.set_mapping_src_meta(job, mini)
        if src.get("inspect"):
            mini["inspect"] = {"sources": {}}
            for job, info in src["inspect"]["jobs"].items():
                if not detailed:
                    # remove big inspect data but preserve inspect status/info and errors
                    mode_has_error = []
                    mode_ok = []
                    for mode in info.get("inspect", {}).get("results", {}):
                        if info["inspect"]["results"][mode].get("errors"):
                            mode_has_error.append(mode)
                        else:
                            mode_ok.append(mode)
                    for mode in mode_ok:
                        info["inspect"]["results"].pop(mode)
                    for mode in mode_has_error:
                        keys = list(info["inspect"]["results"][mode].keys())
                        # remove all except errors
                        for k in keys:
                            if k != "errors":
                                info["inspect"]["results"][mode].pop(k)

                mini["inspect"]["sources"][job] = info

        if src.get("locked"):
            mini["locked"] = src["locked"]
        mini["count"] = count

        return mini

    def get_sources(self, id=None, debug=False, detailed=False):
        dm = self.dump_manager
        um = self.upload_manager
        dpm = self.data_plugin_manager
        ids = set()
        if id and id in dm.register:
            ids.add(id)
        elif id and id in um.register:
            ids.add(id)
        elif id and id in dpm.register:
            ids.add(id)
        else:
            # either no id passed, or doesn't exist
            if id and not len(ids):
                raise ValueError("Source %s doesn't exist" % repr(id))
            ids = set(dm.register)
            ids.update(um.register)
            ids.update(dpm.register)
        sources = {}
        bydsrcs = {}
        byusrcs = {}
        bydpsrcs = {}
        plugins = get_data_plugin().find()
        [bydsrcs.setdefault(src["_id"], src) for src in dm.source_info() if dm]
        [byusrcs.setdefault(src["_id"], src) for src in um.source_info() if um]
        [bydpsrcs.setdefault(src["_id"], src) for src in plugins]
        for _id in ids:
            # start with dumper info
            if dm:
                src = bydsrcs.get(_id)
                if src:
                    if debug:
                        sources[src["name"]] = src
                    else:
                        sources[src["name"]] = self.sumup_source(src, detailed)
            # complete with uploader info
            if um:
                src = byusrcs.get(_id)
                if src:
                    # collection-only source don't have dumpers and only exist in
                    # the uploader manager
                    if not src["_id"] in sources:
                        sources[src["_id"]] = self.sumup_source(src, detailed)
                    if src.get("upload"):
                        for subname in src["upload"].get("jobs", {}):
                            try:
                                sources[src["name"]].setdefault("upload", {"sources": {}})["sources"].setdefault(
                                    subname, {}
                                )
                                sources[src["name"]]["upload"]["sources"][subname]["uploader"] = src["upload"]["jobs"][
                                    subname
                                ].get("uploader")
                            except Exception as e:
                                logging.error("Source is invalid: %s\n%s" % (e, pformat(src)))
            # deal with plugin info if any
            if dpm:
                src = bydpsrcs.get(_id)
                if src:
                    assert len(dpm[_id]) == 1, "Expected only one uploader, got: %s" % dpm[_id]
                    klass = dpm[_id][0]
                    src.pop("_id")
                    if hasattr(klass, "data_plugin_error"):
                        src["error"] = klass.data_plugin_error
                    sources.setdefault(_id, {"data_plugin": {}})
                    if src.get("download", {}).get("err"):
                        src["download"]["error"] = src["download"].pop("err")
                    if src.get("download", {}).get("tb"):
                        src["download"]["traceback"] = src["download"].pop("tb")
                    sources[_id]["data_plugin"] = src
                    sources[_id]["_id"] = _id
                    sources[_id]["name"] = _id
        if id:
            src = list(sources.values()).pop()
            # enrich with metadata (uploader > dumper)
            ks = []
            if dm:
                try:
                    ks.extend(dm.register[id])
                except KeyError:
                    pass
            if um:
                try:
                    ks.extend(um.register[id])
                except KeyError:
                    pass
            for upk in ks:
                # name either from uploader or dumper
                name = getattr(upk, "name", None) or upk.SRC_NAME
                if getattr(upk, "__metadata__", {}).get("src_meta"):
                    src.setdefault("__metadata__", {}).setdefault(name, {})
                    src["__metadata__"][name] = upk.__metadata__["src_meta"]
            # simplify as needed (if only one source in metadata, remove source key level,
            # or if licenses are the same amongst sources, keep one copy)
            if len(src.get("__metadata__", {})) == 1:
                src["__metadata__"] = list(src["__metadata__"].values()).pop()
            elif len(src.get("__metadata__", {})) > 1:
                metas = list(src["__metadata__"].values())
                simplified = [metas.pop()]
                same = True
                while metas:
                    m = metas.pop()
                    if m not in simplified:
                        same = False
                        break
                if same:
                    # we consume all of them, ie. they're all equals
                    src["__metadata__"] = list(src["__metadata__"].values()).pop()
                else:
                    # convert to a list of dict (so it's easier to detect if one or more
                    # licenses just by checking if type is dict (one) or array (more))
                    metas = src.pop("__metadata__")
                    src["__metadata__"] = []
                    for m in metas:
                        src["__metadata__"].append({m: metas[m]})
            return src
        else:
            return list(sources.values())

    def get_source(self, name, debug=False):
        return self.get_sources(id=name, debug=debug, detailed=True)

    def save_mapping(self, name, mapping=None, dest="master", mode="mapping"):
        logging.debug("Saving mapping for source '%s' destination='%s':\n%s", name, dest, pformat(mapping))
        # either given a fully qualified source or just sub-source
        try:
            subsrc = name.split(".")[1]
        except IndexError:
            subsrc = name
        if dest == "master":
            m = self.src_master.find_one({"_id": subsrc}) or {"_id": subsrc}
            m["mapping"] = mapping
            self.src_master.save(m)
        elif dest == "inspect":
            m = self.src_dump.find_one({"_id": name})
            try:
                m["inspect"]["jobs"][subsrc]["inspect"]["results"][mode] = mapping
                self.src_dump.save(m)
            except KeyError:
                raise ValueError("Can't save mapping, document doesn't contain expected inspection data")
        else:
            raise ValueError("Unknow saving destination: %s" % repr(dest))

    def reset(self, name, key="upload", subkey=None):
        """
        Reset, ie. delete, internal data (src_dump document) for given source name, key subkey.
        This method is useful to clean outdated information in Hub's internal database.

        Ex: key=upload, name=mysource, subkey=mysubsource, will delete entry in corresponding
            src_dump doc (_id=mysource), under key "upload", for sub-source named "mysubsource"

        "key" can be either 'download', 'upload' or 'inspect'. Because there's no such notion of subkey for
        dumpers (ie. 'download', subkey is optional.
        """
        doc = self.src_dump.find_one({"_id": name})
        if not doc:
            raise ValueError("No such datasource named '%s'" % name)
        try:
            # nested
            if key in ["upload", "inspect"]:
                del doc[key]["jobs"][subkey]
            # not nested
            elif key == "download":
                del doc[key]
            else:
                raise ValueError("key=%s not allowed" % repr(key))
            self.src_dump.save(doc)
        except KeyError as e:
            logging.exception(e)
            raise ValueError(f"Can't delete information, not found in document: {e}")
