from dateutil.parser import parse as dtparse
import locale

locale.setlocale(locale.LC_ALL, '')


class ReleaseNoteTxt(object):
    def __init__(self, changes):
        self.changes = changes
        #pprint(self.changes)

    def save(self, filepath):
        try:
            import prettytable
        except ImportError:
            raise ImportError(
                "Please install prettytable to use this rendered")

        def format_number(n, sign=None):
            s = ""
            if sign:
                if n > 0:
                    s = "+"
                elif n < 0:
                    s = "-"
            try:
                n = abs(n)
                strn = "%s%s" % (s, locale.format("%d", n, grouping=True))
            except TypeError:
                # something wrong with converting, maybe we don't even have a number to format...
                strn = "N.A"
            return strn

        txt = ""
        title = "Build version: '%s'" % self.changes["new"]["_version"]
        txt += title + "\n"
        txt += "".join(["="] * len(title)) + "\n"
        dt = dtparse(self.changes["generated_on"])
        txt += "Previous build version: '%s'\n" % self.changes["old"][
            "_version"]
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

        for src, info in sorted(self.changes["sources"]["added"].items(),
                                key=lambda e: e[0]):
            main_info = dict([(k, v) for k, v in info.items()
                              if k.startswith("_")])
            sub_infos = dict([(k, v) for k, v in info.items()
                              if not k.startswith("_")])
            if sub_infos:
                for sub, sub_info in sub_infos.items():
                    table.add_row([
                        "%s.%s" % (src, sub), "-", main_info["_version"], "-",
                        format_number(sub_info["_count"])
                    ])  # only _count avail there
            else:
                main_count = main_info.get("_count") and format_number(
                    main_info["_count"]) or ""
                table.add_row(
                    [src, "-",
                     main_info.get("_version", ""), "-", main_count])
        for src, info in sorted(self.changes["sources"]["deleted"].items(),
                                key=lambda e: e[0]):
            main_info = dict([(k, v) for k, v in info.items()
                              if k.startswith("_")])
            sub_infos = dict([(k, v) for k, v in info.items()
                              if not k.startswith("_")])
            if sub_infos:
                for sub, sub_info in sub_infos.items():
                    table.add_row([
                        "%s.%s" % (src, sub),
                        main_info.get("_version", ""), "-",
                        format_number(sub_info["_count"]), "-"
                    ])  # only _count avail there
            else:
                main_count = main_info.get("_count") and format_number(
                    main_info["_count"]) or ""
                table.add_row(
                    [src,
                     main_info.get("_version", ""), "-", main_count, "-"])
        for src, info in sorted(self.changes["sources"]["updated"].items(),
                                key=lambda e: e[0]):
            # extract information from main-source
            old_main_info = dict([(k, v) for k, v in info["old"].items()
                                  if k.startswith("_")])
            new_main_info = dict([(k, v) for k, v in info["new"].items()
                                  if k.startswith("_")])
            old_main_count = old_main_info.get("_count") and format_number(
                old_main_info["_count"]) or None
            new_main_count = new_main_info.get("_count") and format_number(
                new_main_info["_count"]) or None
            if old_main_count is None:
                assert new_main_count is None, "Sub-sources found for '%s', old and new count should " % src + \
                                               "both be None. Info was: %s" % info
                old_sub_infos = dict([(k, v) for k, v in info["old"].items()
                                      if not k.startswith("_")])
                new_sub_infos = dict([(k, v) for k, v in info["new"].items()
                                      if not k.startswith("_")])
                # old & new sub_infos should have the same structure (same existing keys)
                # so we just use one of them to explore
                if old_sub_infos:
                    assert new_sub_infos
                    for sub, sub_info in old_sub_infos.items():
                        table.add_row([
                            "%s.%s" % (src, sub),
                            old_main_info.get("_version", ""),
                            new_main_info.get("_version", ""),
                            format_number(sub_info["_count"]),
                            format_number(new_sub_infos[sub]["_count"])
                        ])
            else:
                assert new_main_count is not None, "No sub-sources found, old and new count should NOT " + \
                                                   "both be None. Info was: %s" % info
                table.add_row([
                    src,
                    old_main_info.get("_version", ""),
                    new_main_info.get("_version", ""), old_main_count,
                    new_main_count
                ])

        if table._rows:
            txt += table.get_string()
            txt += "\n"
        else:
            txt += "No datasource changed.\n"

        total_count = self.changes["new"].get("_count")
        if self.changes["sources"]["added"]:
            txt += "New datasource(s): %s\n" % ", ".join(
                sorted(list(self.changes["sources"]["added"])))
        if self.changes["sources"]["deleted"]:
            txt += "Deleted datasource(s): %s\n" % ", ".join(
                sorted(list(self.changes["sources"]["deleted"])))
        if self.changes["sources"]:
            txt += "\n"

        table = prettytable.PrettyTable(["Updated stats.", "previous", "new"])
        table.align["Updated stats."] = "l"
        table.align["previous"] = "r"
        table.align["new"] = "r"
        for stat_name, stat in sorted(self.changes["stats"]["added"].items(),
                                      key=lambda e: e[0]):
            table.add_row([stat_name, "-", format_number(stat["_count"])])
        for stat_name, stat in sorted(self.changes["stats"]["deleted"].items(),
                                      key=lambda e: e[0]):
            table.add_row([stat_name, format_number(stat["_count"]), "-"])
        for stat_name, stat in sorted(self.changes["stats"]["updated"].items(),
                                      key=lambda e: e[0]):
            table.add_row([
                stat_name,
                format_number(stat["old"]["_count"]),
                format_number(stat["new"]["_count"])
            ])
        if table._rows:
            txt += table.get_string()
            txt += "\n\n"

        if self.changes["new"]["_fields"]:
            new_fields = sorted(self.changes["new"]["_fields"].get("add", []))
            deleted_fields = self.changes["new"]["_fields"].get("remove", [])
            updated_fields = self.changes["new"]["_fields"].get("replace", [])
            if new_fields:
                txt += "New field(s): %s\n" % ", ".join(new_fields)
            if deleted_fields:
                txt += "Deleted field(s): %s\n" % ", ".join(deleted_fields)
            if updated_fields:
                txt += "Updated field(s): %s\n" % ", ".join(updated_fields)
            txt += "\n"

        if total_count is not None:
            txt += "Overall, %s documents in this release\n" % (
                format_number(total_count))
        if self.changes["new"]["_summary"]:
            sumups = []
            sumups.append(
                "%s document(s) added" %
                format_number(self.changes["new"]["_summary"].get("add", 0)))
            sumups.append("%s document(s) deleted" % format_number(
                self.changes["new"]["_summary"].get("delete", 0)))
            sumups.append("%s document(s) updated" % format_number(
                self.changes["new"]["_summary"].get("update", 0)))
            txt += ", ".join(sumups) + "\n"
        else:
            txt += "No information available for added/deleted/updated documents\n"

        if self.changes.get("note"):
            txt += "\n"
            txt += "Note: %s\n" % self.changes["note"]

        with open(filepath, "w") as fout:
            fout.write(txt)

        return txt
