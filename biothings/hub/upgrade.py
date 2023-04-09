import os
import subprocess
import sys

from biothings import config
from biothings.hub.dataload.dumper import GitDumper
from biothings.utils.hub_db import backup, get_data_plugin, get_hub_db_conn, get_src_dump, restore

logging = config.logger


def migrate_0dot1_to_0dot2():
    """
    mongodb src_dump/data_plugin changed:
        1. "data_folder" and "release" under "download"
        2. "data_folder" and "release" in upload.jobs[subsrc] taken from "download"
        3. no more "err" under "upload"
        4. no more "status" under "upload"
        5. "pending_to_upload" is now "pending": ["upload"]
    """
    src_dump = get_src_dump()
    data_plugin = get_data_plugin()
    for srccol in [src_dump, data_plugin]:
        logging.info("Converting collection %s" % srccol)
        srcs = [src for src in srccol.find()]
        wasdue = False
        for src in srcs:
            logging.info("\tConverting '%s'" % src["_id"])
            # 1.
            for field in ["data_folder", "release"]:
                if field in src:
                    logging.debug("\t\t%s: found '%s' in document, moving under 'download'" % (src["_id"], field))
                    try:
                        src["download"][field] = src.pop(field)
                        wasdue = True
                    except KeyError as e:
                        logging.warning(
                            "\t\t%s: no such field '%s' found, skip it (error: %s)" % (src["_id"], field, e)
                        )
            # 2.
            for subsrc_name in src.get("upload", {}).get("jobs", {}):
                for field in ["data_folder", "release"]:
                    if field not in src["upload"]["jobs"][subsrc_name]:
                        logging.debug(
                            "\t\t%s: no '%s' found in upload jobs, taking it from 'download' (or from root keys)"
                            % (src["_id"], field)
                        )
                        try:
                            src["upload"]["jobs"][subsrc_name][field] = src["download"][field]
                            wasdue = True
                        except KeyError:
                            try:
                                src["upload"]["jobs"][subsrc_name][field] = src[field]
                                wasdue = True
                            except KeyError:
                                logging.warning("\t\t%s: no such field '%s' found, skip it" % (src["_id"], field))
            # 3. & 4.
            for field in ["err", "status"]:
                if field in src.get("upload", {}):
                    logging.debug("\t\t%s: removing '%s' key from 'upload'" % (src["_id"], field))
                    src["upload"].pop(field)
                    wasdue = True
            # 5.
            if "pending_to_upload" in src:
                logging.debug("\t%s: found 'pending_to_upload' field, moving to 'pending' list" % src["_id"])
                src.pop("pending_to_upload")
                wasdue = True
                if "upload" not in src.get("pending", []):
                    src.setdefault("pending", []).append("upload")
            if wasdue:
                logging.info("\tFinishing converting document for '%s'" % src["_id"])
                srccol.save(src)
            else:
                logging.info("\tDocument for '%s' already converted" % src["_id"])


def migrate(from_version, to_version, restore_if_failure=True):
    func_name = "migrate_%s_to_%s" % (from_version.replace(".", "dot"), to_version.replace(".", "dot"))
    # backup
    db = get_hub_db_conn()[config.DATA_HUB_DB_DATABASE]
    logging.info("Backing up %s" % db.name)
    path = backup()
    logging.info("Backup file: %s" % path)
    thismodule = sys.modules[__name__]
    try:
        func = getattr(thismodule, func_name)
    except AttributeError:
        logging.error("Can't upgrade, no such function to migrate from '%s' to '%s'", from_version, to_version)
        raise
    # resolve A->C = A->B then B->C
    logging.info("Start upgrading from '%s' to '%s'", from_version, to_version)
    try:
        func()
    except Exception as e:
        logging.exception("Failed upgrading: %s" % e)
        if restore_if_failure:
            logging.info("Now restoring original database from '%s" % path)
            restore(db, path, drop=True)
            logging.info("Done. If you want to keep converted data for inspection, use restore_if_failure=False")
        else:
            logging.info("*not* restoring original data. It can still be restored using file '%s'", path)


class BaseSystemUpgrade(GitDumper):
    def _pull(self, *args):
        old = os.path.abspath(os.curdir)
        try:
            # SRC_ROOT_FOLDER is set to code base root
            os.chdir(self.__class__.SRC_ROOT_FOLDER)
            cmd = ["git", "pull", "--rebase"]
            self.logger.debug("Pulling new code in '%s' using: %s" % (self.__class__.SRC_ROOT_FOLDER, cmd))
            subprocess.check_call(cmd)
        finally:
            os.chdir(old)


class BioThingsSystemUpgrade(BaseSystemUpgrade):
    """Used to upgrade biothings SDK code"""

    SRC_NAME = "__biothings_sdk"  # "__" means private, not listed


class ApplicationSystemUpgrade(BaseSystemUpgrade):
    """Used to upgrade application (api app) code"""

    SRC_NAME = "__application"  # "__" means private, not listed
