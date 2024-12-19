"""
CLI backend for storing merged plugins after building
"""

from biothings.hub.databuild.backend import SourceDocBackendBase
from biothings.utils.hub_db import get_source_fullname


class CLISourceDocBackend(SourceDocBackendBase):
    def get_build_configuration(self, build_name):
        self._config = self.build_config.find_one({"_id": build_name})
        return self._config

    def validate_sources(self, sources=None):
        assert self._build_config, "'self._build_config' cannot be empty."

    def get_src_master_docs(self):
        if self.src_masterdocs is None:
            for table_name in self.sources.collection_names():
                if "archive" not in table_name:
                    source_connection = self.sources.get_conn()
                    select_statement = f"SELECT * FROM {table_name}"
                    self.src_masterdocs = {
                        src[0]: src[1] for src in source_connection.execute(select_statement).fetchall()
                    }
        breakpoint()
        return self.src_masterdocs

    def get_src_metadata(self):
        """
        Return source versions which have been previously accessed wit this backend object
        or all source versions if none were accessed. Accessing means going through __getitem__
        (the usual way) and allows to auto-keep track of sources of interest, thus returning
        versions only for those.
        """
        src_version = {}
        # what's registered in each uploader, from src_master.
        # also includes versions
        src_meta = {}
        srcs = []
        if self.sources_accessed:
            for src in self.sources_accessed:
                fullname = get_source_fullname(src)
                main_name = fullname.split(".")[0]
                doc = self.dump.find_one({"_id": main_name})
                srcs.append(doc["_id"])
            srcs = list(set(srcs))
        else:
            srcs = [d["_id"] for d in self.dump.find()]
        # we need to return main_source named, but if accessed, it's been through sub-source names
        # query is different in that case
        for src in self.dump.find({"_id": {"$in": srcs}}):
            # now merge other extra information from src_master (src_meta key). src_master _id
            # are sub-source names, not main source so we have to deal with src_dump as well
            # in order to resolve/map main/sub source name
            subsrc_versions = []

            if src and src.get("download"):
                # Store the latest success dump time
                src_meta.setdefault(src["_id"], {})
                last_success = src["download"].get("last_success")
                if not last_success and src["download"].get("status") == "success":
                    last_success = src["download"].get("started_at")
                if last_success:
                    src_meta[src["_id"]]["download_date"] = last_success

            if src and src.get("upload"):
                latest_upload_date = None
                meta = {}
                for job_name in src["upload"].get("jobs", {}):
                    job = src["upload"]["jobs"][job_name]
                    # "step" is the actual sub-source name
                    sub_source = job.get("step")
                    docm = self.find_one({"_id": sub_source})
                    if docm and docm.get("src_meta"):
                        meta[sub_source] = docm["src_meta"]
                    # Store the latest success upload time
                    if not latest_upload_date or latest_upload_date < job["started_at"]:
                        step_meta = meta.setdefault(sub_source, {})
                        sub_source_info = src["upload"]["jobs"][sub_source]
                        last_success = sub_source_info.get("last_success")
                        if not last_success and sub_source_info.get("status") == "success":
                            last_success = sub_source_info.get("started_at")
                        if last_success:
                            step_meta["upload_date"] = last_success

                # when more than 1 sub-sources, we can have different version in sub-sources
                # (not normal) if one sub-source uploaded, then dumper downloaded a new version,
                # then the other sub-source uploaded that version. This should never happen, just make sure
                subsrc_versions = [
                    {"sub-source": job.get("step"), "version": job.get("release")}
                    for job in src["upload"].get("jobs", {}).values()
                ]
                assert (
                    len(set([s["version"] for s in subsrc_versions])) == 1
                ), "Expecting one version " + "in upload sub-sources for main source '%s' but got: %s" % (
                    src["_id"],
                    subsrc_versions,
                )
                # usually, url & license are the same wathever the sub-sources are. They are
                # share common metadata, and we don't want them to be repeated for each sub-sources.
                # but, code key is always different for instance and must specific for each sub-sources
                # here we make sure to factor common keys, while the specific ones at sub-level
                if len(meta) > 1:
                    common = {}
                    any = list(meta)[0]
                    topop = []  # common keys
                    for anyk in meta[any]:
                        if len({meta[s].get(anyk) == meta[any][anyk] for s in meta}) == 1:
                            topop.append(anyk)
                    for k in topop:
                        common[k] = meta[any][k]
                        [meta[subname].pop(k, None) for subname in meta]

                    for k, v in common.items():
                        src_meta.setdefault(src["_id"], {}).setdefault(k, v)
                    for subname in meta:
                        for k, v in meta[subname].items():
                            src_meta.setdefault(src["_id"], {}).setdefault(k, {}).setdefault(subname, v)
                # we have metadata, but just one (ie. no sub-source), don't display it
                elif meta:
                    assert len(meta) == 1
                    subname, metad = meta.popitem()
                    for k, v in metad.items():
                        src_meta.setdefault(src["_id"], {}).setdefault(k, v)
            if subsrc_versions:
                version = subsrc_versions[0]["version"]
                src_version[src["_id"]] = version
                src_meta.setdefault(src["_id"], {}).setdefault("version", version)
        return src_meta
