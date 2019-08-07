import asyncio
import logging

from .dumper import BiothingsDumper, LATEST
from .uploader import BiothingsUploader


def update(dumper_manager, uploader_manager, src_name, version=LATEST, dry=False):
    """
    Update hub's data up to the given version (default is latest available),
    using full and incremental updates to get up to that given version (if possible).
    """
    @asyncio.coroutine
    def do(version):
        dklass = dumper_manager[src_name][0] # only one dumper allowed / source
        dobj = dumper_manager.create_instance(dklass)
        update_path = dobj.find_update_path(version,backend_version=dobj.target_backend.version)
        version_path = [v["build_version"] for v in update_path]
        if not version_path:
            logging.info("No update path found")
            return

        logging.info("Found path for updating from version '%s' to version '%s': %s" % (dobj.target_backend.version,version,version_path))
        if dry:
            return version_path

        for step_version in version_path:
            logging.info("Downloading data for version '%s'" % step_version)
            jobs = dumper_manager.dump_src(src_name,version=step_version)
            download = asyncio.gather(*jobs)
            res = yield from download
            assert len(res) == 1
            if res[0] == None:
                # download ready, now update
                logging.info("Updating backend to version '%s'" % step_version)
                jobs = uploader_manager.upload_src(src_name)
                upload = asyncio.gather(*jobs)
                res = yield from upload

    return asyncio.ensure_future(do(version))

