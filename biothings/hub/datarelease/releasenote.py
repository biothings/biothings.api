from __future__ import annotations  # for cyclic type hints

from biothings.utils.hub_db import get_source_fullname
from biothings.utils.dataload import update_dict_recur
from biothings.utils.jsondiff import make as make_json_diff

from dateutil.parser import parse as dtparse
from datetime import datetime
import locale

locale.setlocale(locale.LC_ALL, '')


class ReleaseNoteSrcBuildReader:
    def __init__(self, src_build_doc: dict):
        self.src_build_doc = src_build_doc

        # If `self` is a "hot" src_build doc reader, it can refer to a "cold" reader to access the cold build info.
        # This works like a two-node linked list.
        self.cold_src_build_reader: ReleaseNoteSrcBuildReader = None

    @property
    def build_id(self) -> str:
        return self.src_build_doc["_id"]

    @property
    def build_version(self) -> str:
        return self.src_build_doc.get("_meta", {}).get("build_version")

    @property
    def cold_collection_name(self) -> str:
        return self.src_build_doc.get("build_config", {}).get("cold_collection", None)

    def has_cold_collection(self) -> bool:
        return self.cold_collection_name is not None

    def attach_cold_src_build_reader(self, other: ReleaseNoteSrcBuildReader):
        """
        Attach a cold src_build reader.

        It's required that `self` is a hot src_builder reader and `other` is cold.
        """
        if not self.has_cold_collection():
            raise ValueError(f"{self.build_id} is not a hot src_build doc, "
                             f"thus not able to attach a cold reader of {other.build_id}.")

        if other.has_cold_collection():
            raise ValueError(f"{other.build_id} is a hot src_build doc, "
                             f"thus not able to be attached to the reader of {self.build_id}")

        # src_build `_id`s and collection names are interchangeable
        # See https://github.com/biothings/biothings.api/blob/master/biothings/hub/databuild/builder.py#L311
        if self.cold_collection_name != other.build_id:
            raise ValueError(f"{self.build_id} has cold collection {self.cold_collection_name}, "
                             f"while the reader to be attached is for {other.build_id}")

        self.cold_src_build_reader = other

    @property
    def build_stats(self) -> dict:
        meta = self.src_build_doc.get("_meta", {})
        return meta.get("stats", {})

    def _get_datasource_stats(self) -> dict:
        return self.src_build_doc.get("merge_stats", {})

    def _get_datasource_versions(self) -> dict:
        meta = self.src_build_doc.get("_meta", {})

        # previous version format
        if "src_version" in meta:
            return meta["src_version"]

        # current version format
        src = meta.get("src", {})
        src_version = {src_name: src_info["version"] for src_name, src_info in src.items() if "version" in src_info}
        return src_version

    def _get_datasource_mapping(self) -> dict:
        return self.src_build_doc.get("mapping", {})

    @property
    def datasource_stats(self) -> dict:
        if not self.has_cold_collection():
            return self._get_datasource_stats()

        combined_stats = {
            **self._get_datasource_stats(),
            **self.cold_src_build_reader._get_datasource_stats()
        }
        return combined_stats

    @property
    def datasource_versions(self) -> dict:
        if not self.has_cold_collection():
            return self._get_datasource_versions()

        combined_versions = {
            **self._get_datasource_versions(),
            **self.cold_src_build_reader._get_datasource_versions()
        }
        return combined_versions

    @property
    def datasource_mapping(self) -> dict:
        if not self.has_cold_collection():
            return self._get_datasource_mapping()

        combined_mapping = {
            **self._get_datasource_mapping(),
            **self.cold_src_build_reader._get_datasource_mapping()
        }
        return combined_mapping


class ReleaseNoteDatasourceInfoReader:
    def __init__(self, src_build_reader: ReleaseNoteSrcBuildReader):
        self.src_build_reader = src_build_reader

    @property
    def _transformed_datasource_stats(self) -> dict:
        """
        Receive a stat dictionary of <datasource_name>:<doc_count>, fetch the full datasource name, and
        return a new stat dictionary.

        If a full datasource name is two-tier (e.g. "gnomad.gnomad_exomes_hg19", as the full datasource name of
        "gnomad_exomes_hg19"), the returned dictionary is formed as:

            { <main_datasource_name> : { <sub_datasource_name> : { "_count" : <doc_count> } } }

        e.g.

            { "gnomad" : { "gnomad_exomes_hg19" : { "_count" : 12345678 } } }

        If a full datasource name is one-tier:
            CASE 1: the full datasource name is identical to the input datasource name,
                e.g. "cosmic" is the full name of "cosmic";
            CASE 2: the full datasource is None,
                e.g. when the input datasource name is "observed" or "total" in MyVariant, or "total_*" in MyGene
                In this case, the input datasource name is not a merge stat from a source but a custom field stat.

        the returned stats dictionary has the following structure:

            { <datasource_name> : { "_count" : <doc_count> } }

        e.g.

            { "cosmic" : { "_count" : 12345678 } }
            { "total" : { "_count" : 12345678 } }
        """
        result = {}
        for datasource_name, doc_count in self.src_build_reader.datasource_stats.items():
            datasource_fullname = get_source_fullname(datasource_name)

            if (datasource_fullname is None) or (datasource_fullname == datasource_name):
                # one-tier fullname
                result[datasource_name] = {"_count": doc_count}
            else:
                # two-tier fullname
                main_name, sub_name = datasource_fullname.split(".")
                result.setdefault(main_name, {})
                result[main_name][sub_name] = {"_count": doc_count}

        return result

    @property
    def _transformed_datasource_versions(self) -> dict:
        """
        For each datasource (e.g. clinvar, dbsnp) attached to the build, get its version number (e.g. "2022-01",
        "155"). A dictionary of the following structure is returned:

            { <datasource_name> : {"_version" : <datasource_version>} }
        """
        return dict((k, {"_version": v}) for k, v in self.src_build_reader.datasource_versions.items())

    @property
    def datasource_info(self):
        datasource_versions = self._transformed_datasource_versions
        datasource_stats = self._transformed_datasource_stats

        datasource_info = update_dict_recur(datasource_versions, datasource_stats)
        return datasource_info


class ReleaseNoteSource:
    def __init__(self,
                 old_src_build_reader: ReleaseNoteSrcBuildReader,
                 new_src_build_reader: ReleaseNoteSrcBuildReader,
                 diff_stats_from_metadata_file: dict,
                 addon_note: str):
        self.old_src_build_reader = old_src_build_reader
        self.new_src_build_reader = new_src_build_reader

        self.old_datasource_info_reader = ReleaseNoteDatasourceInfoReader(self.old_src_build_reader)
        self.new_datasource_info_reader = ReleaseNoteDatasourceInfoReader(self.new_src_build_reader)

        self.diff_stats_from_metadata_file = diff_stats_from_metadata_file
        self.addon_note = addon_note

    @classmethod
    def _make_stats_diff(cls, old: dict, new: dict):
        result = {
            "added": {},
            "deleted": {},
            "updated": {},
        }

        diff = make_json_diff(old, new)
        for item in diff:
            # get main source / main field
            key = item["path"].strip("/").split("/")[0]
            if item["op"] == "add":
                result["added"][key] = new[key]
            elif item["op"] == "remove":
                result["deleted"][key] = old[key]
            elif item["op"] == "replace":
                result["updated"][key] = {"new": new[key], "old": old[key]}
            else:
                raise ValueError("Unknown operation '%s' while computing changes" % item["op"])

        return result

    @classmethod
    def _make_mapping_diff(cls, old: dict, new: dict):
        def mapping_path_to_field_name(path: str) -> str:
            """
            Convert a JSON-Pointer path in a mapping json to a field name.

                E.g. path "/dbnsfp/properties/altai_neandertal" => field name "dbnsfp.altai_neandertal".

            Note that "properties" should not be included as part of a field name.
            The strategy here is iterate over the path components and remove any "properties" found at odd
            indices (1, 3, 5...).
            """
            path_components = path.strip("/").split("/")
            path_components = [path_components[i] for i in range(len(path_components))
                               if (i % 2 == 0) or (i % 2 == 1 and path_components[i] != "properties")]
            return ".".join(path_components)

        fields = {}

        diff = make_json_diff(old, new)
        for item in diff:
            if item["op"] in ("add", "remove", "replace"):
                field_name = mapping_path_to_field_name(item["path"])
                fields.setdefault(item["op"], []).append(field_name)
            elif item["op"] == "move":
                add_field_name = mapping_path_to_field_name(item["path"])
                remove_field_name = mapping_path_to_field_name(item["from"])

                fields.setdefault("add", []).append(add_field_name)
                fields.setdefault("remove", []).append(remove_field_name)
            else:
                raise ValueError("Unknown operation '%s' while computing changes" % item["op"])

        return fields

    def diff_build_stats(self) -> dict:
        old_stats = self.old_src_build_reader.build_stats
        new_stats = self.new_src_build_reader.build_stats

        return self._make_stats_diff(old_stats, new_stats)

    def diff_datasource_info(self) -> dict:
        old_info = self.old_datasource_info_reader.datasource_info
        new_info = self.new_datasource_info_reader.datasource_info

        return self._make_stats_diff(old_info, new_info)

    def diff_datasource_mapping(self) -> dict:
        new_mapping = self.new_src_build_reader.datasource_mapping
        if not new_mapping:
            raise ValueError(f"New Mapping cannot be empty. Build id: {self.new_src_build_reader.build_id}")
        old_mapping = self.old_src_build_reader.datasource_mapping

        return self._make_mapping_diff(old_mapping, new_mapping)

    def to_dict(self) -> dict:
        result = {
            "old": {
                "_version": self.old_src_build_reader.build_version,
                "_count": self.old_src_build_reader.build_stats.get("total"),
            },
            "new": {
                "_version": self.new_src_build_reader.build_version,
                "_count": self.new_src_build_reader.build_stats.get("total"),
                "_fields": self.diff_datasource_mapping(),
                "_summary": self.diff_stats_from_metadata_file,
            },

            "stats": self.diff_build_stats(),
            "sources": self.diff_datasource_info(),

            "note": self.addon_note,
            "generated_on": str(datetime.now().astimezone()),
        }
        return result


class ReleaseNoteTxt(object):
    def __init__(self, source: ReleaseNoteSource):
        self.source = source  # member kept for debugging
        self.changes = source.to_dict()

    @classmethod
    def _format_number(cls, num, sign=None):
        try:
            sign_symbol = ""
            if sign:
                if num > 0:
                    sign_symbol = "+"
                elif num < 0:
                    sign_symbol = "-"

            num_str = locale.format_string("%d", abs(num), grouping=True)

            return "%s%s" % (sign_symbol, num_str)
        except TypeError:
            # something wrong with converting, maybe we don't even have a number to format...
            return "N.A"

    def save(self, filepath):
        try:
            import prettytable
        except ImportError:
            raise ImportError("Please install prettytable to use this rendered")

        txt = ""
        title = "Build version: '%s'" % self.changes["new"]["_version"]
        txt += title + "\n"
        txt += "".join(["="] * len(title)) + "\n"
        dt = dtparse(self.changes["generated_on"])
        txt += "Previous build version: '%s'\n" % self.changes["old"]["_version"]
        txt += "Generated on: %s\n" % dt.strftime("%Y-%m-%d at %H:%M:%S")
        txt += "\n"

        table = prettytable.PrettyTable([
            "Updated datasource", "prev. release", "new release",
            "prev. # of docs", "new # of docs"
        ])
        table.align["Updated datasource"] = "l"
        table.align["prev. release"] = "c"
        table.align["new release"] = "c"
        table.align["prev. # of docs"] = "r"
        table.align["new # of docs"] = "r"

        for src, info in sorted(self.changes["sources"]["added"].items(), key=lambda e: e[0]):
            main_info = dict([(k, v) for k, v in info.items() if k.startswith("_")])
            sub_infos = dict([(k, v) for k, v in info.items() if not k.startswith("_")])
            if sub_infos:
                for sub, sub_info in sub_infos.items():
                    table.add_row([
                        "%s.%s" % (src, sub), "-", main_info["_version"], "-", self._format_number(sub_info["_count"])
                    ])  # only _count avail there
            else:
                main_count = main_info.get("_count") and self._format_number(main_info["_count"]) or ""
                table.add_row([
                    src, "-", main_info.get("_version", ""), "-", main_count
                ])

        for src, info in sorted(self.changes["sources"]["deleted"].items(), key=lambda e: e[0]):
            main_info = dict([(k, v) for k, v in info.items() if k.startswith("_")])
            sub_infos = dict([(k, v) for k, v in info.items() if not k.startswith("_")])
            if sub_infos:
                for sub, sub_info in sub_infos.items():
                    table.add_row([
                        "%s.%s" % (src, sub), main_info.get("_version", ""), "-", self._format_number(sub_info["_count"]), "-"
                    ])  # only _count avail there
            else:
                main_count = main_info.get("_count") and self._format_number(main_info["_count"]) or ""
                table.add_row([
                    src, main_info.get("_version", ""), "-", main_count, "-"
                ])

        for src, info in sorted(self.changes["sources"]["updated"].items(), key=lambda e: e[0]):
            # extract information from main-source
            old_main_info = dict([(k, v) for k, v in info["old"].items() if k.startswith("_")])
            new_main_info = dict([(k, v) for k, v in info["new"].items() if k.startswith("_")])
            old_main_count = old_main_info.get("_count") and self._format_number(old_main_info["_count"]) or None
            new_main_count = new_main_info.get("_count") and self._format_number(new_main_info["_count"]) or None
            if old_main_count is None:
                assert new_main_count is None, \
                    "Sub-sources found for '%s', old and new count should " % src + "both be None. Info was: %s" % info
                old_sub_infos = dict([(k, v) for k, v in info["old"].items() if not k.startswith("_")])
                new_sub_infos = dict([(k, v) for k, v in info["new"].items() if not k.startswith("_")])
                # old & new sub_infos should have the same structure (same existing keys)
                # so we just use one of them to explore
                if old_sub_infos:
                    assert new_sub_infos
                    for sub, sub_info in old_sub_infos.items():
                        table.add_row([
                            "%s.%s" % (src, sub),
                            old_main_info.get("_version", ""),
                            new_main_info.get("_version", ""),
                            self._format_number(sub_info["_count"]),
                            self._format_number(new_sub_infos[sub]["_count"])
                        ])
            else:
                assert new_main_count is not None, \
                    "No sub-sources found, old and new count should NOT " + "both be None. Info was: %s" % info
                table.add_row([
                    src,
                    old_main_info.get("_version", ""),
                    new_main_info.get("_version", ""),
                    old_main_count,
                    new_main_count
                ])

        if table._rows:
            txt += table.get_string()
            txt += "\n"
        else:
            txt += "No datasource changed.\n"

        total_count = self.changes["new"].get("_count")
        if self.changes["sources"]["added"]:
            txt += "New datasource(s): %s\n" % ", ".join(sorted(list(self.changes["sources"]["added"])))
        if self.changes["sources"]["deleted"]:
            txt += "Deleted datasource(s): %s\n" % ", ".join(sorted(list(self.changes["sources"]["deleted"])))
        if self.changes["sources"]:
            txt += "\n"

        table = prettytable.PrettyTable(["Updated stats.", "previous", "new"])
        table.align["Updated stats."] = "l"
        table.align["previous"] = "r"
        table.align["new"] = "r"
        for stat_name, stat in sorted(self.changes["stats"]["added"].items(), key=lambda e: e[0]):
            table.add_row([stat_name, "-", self._format_number(stat["_count"])])
        for stat_name, stat in sorted(self.changes["stats"]["deleted"].items(), key=lambda e: e[0]):
            table.add_row([stat_name, self._format_number(stat["_count"]), "-"])
        for stat_name, stat in sorted(self.changes["stats"]["updated"].items(), key=lambda e: e[0]):
            table.add_row([
                stat_name,
                self._format_number(stat["old"]["_count"]),
                self._format_number(stat["new"]["_count"])
            ])

        if table._rows:
            txt += table.get_string()
            txt += "\n\n"

        if self.changes["new"]["_fields"]:
            new_fields = sorted(self.changes["new"]["_fields"].get("add", []))
            deleted_fields = sorted(self.changes["new"]["_fields"].get("remove", []))
            updated_fields = sorted(self.changes["new"]["_fields"].get("replace", []))
            if new_fields:
                txt += "New field(s): %s\n" % ", ".join(new_fields)
            if deleted_fields:
                txt += "Deleted field(s): %s\n" % ", ".join(deleted_fields)
            if updated_fields:
                txt += "Updated field(s): %s\n" % ", ".join(updated_fields)
            txt += "\n"

        if total_count is not None:
            txt += "Overall, %s documents in this release\n" % (self._format_number(total_count))

        if self.changes["new"]["_summary"]:
            sumups = []
            sumups.append("%s document(s) added" % self._format_number(self.changes["new"]["_summary"].get("add", 0)))
            sumups.append("%s document(s) deleted" % self._format_number(self.changes["new"]["_summary"].get("delete", 0)))
            sumups.append("%s document(s) updated" % self._format_number(self.changes["new"]["_summary"].get("update", 0)))
            txt += ", ".join(sumups) + "\n"
        else:
            txt += "No information available for added/deleted/updated documents\n"

        if self.changes.get("note"):
            txt += "\n"
            txt += "Note: %s\n" % self.changes["note"]

        with open(filepath, "w") as fout:
            fout.write(txt)

        return txt
