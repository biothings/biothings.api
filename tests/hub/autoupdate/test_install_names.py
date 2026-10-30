"""
Data releases installed in several Elasticsearch environments are named <release>__<environment>:
installing one needs that name, unless it's installed in one environment only
"""

import asyncio
import logging
from types import SimpleNamespace

import mongomock
import pytest

from biothings.hub.dataload.dumper import DumperManager
from biothings.hub import standalone
from biothings.hub.manager import ResourceError, ResourceNotFound
from biothings.hub.standalone import AutoHubFeature


class FakeDumper:
    target_backend = SimpleNamespace(version="20260901")

    def find_update_path(self, version, backend_version=None):
        return [{"build_version": "20261001"}]


@pytest.mark.asyncio
async def test_install_names():
    dump_manager = DumperManager(job_manager=None)
    dump_manager.create_instance = lambda klass: klass()
    dump_manager.register = {
        "mychem.info__es8": [FakeDumper],
        "mychem.info__es9": [FakeDumper],
        "mydisease.info__es8": [FakeDumper],
    }
    feature = SimpleNamespace(
        managers={"dump_manager": dump_manager, "job_manager": SimpleNamespace(loop=asyncio.get_running_loop())}
    )
    with pytest.raises(ResourceNotFound, match="give one of them: mychem.info__es8, mychem.info__es9"):
        AutoHubFeature.install(feature, "mychem.info", dry=True)
    assert await AutoHubFeature.install(feature, "mychem.info__es9", dry=True) == ["20261001"]
    assert await AutoHubFeature.install(feature, "mydisease.info", dry=True) == ["20261001"]  # one environment


@pytest.mark.asyncio
async def test_retrying_an_install(monkeypatch):
    """
    A version downloaded already, eg. when its install failed, is installed without downloading it
    again (the download says "Nothing to dump"): it used to be skipped, the install doing nothing
    """
    loop = asyncio.get_running_loop()
    src_dump = mongomock.MongoClient()["hubdb"]["src_dump"]
    src_dump.insert_one({"_id": "mydisease.info__es8", "download": {"release": "20261001", "status": "success"}})
    monkeypatch.setattr(standalone, "get_src_dump", lambda: src_dump)

    def done(result):
        future = loop.create_future()
        future.set_result(result)
        return future

    uploads = []
    dump_manager = DumperManager(job_manager=None)
    dump_manager.create_instance = lambda klass: klass()
    dump_manager.register = {"mydisease.info__es8": [FakeDumper]}
    dump_manager.dump_src = lambda src, **kwargs: [done("Nothing to dump")]
    upload_manager = SimpleNamespace(upload_src=lambda src, **kwargs: uploads.append(src) or [done(352819)])
    feature = SimpleNamespace(
        managers={
            "dump_manager": dump_manager,
            "upload_manager": upload_manager,
            "job_manager": SimpleNamespace(loop=loop),
        },
        validator=SimpleNamespace(validate=lambda **kwargs: None),
        logger=logging.getLogger("test_install"),
    )
    assert await AutoHubFeature.install(feature, "mydisease.info") == "mydisease.info__es8: installed 20261001"
    assert uploads == ["mydisease.info__es8"]
    # downloaded, but not this version: not installed
    src_dump.update_one({"_id": "mydisease.info__es8"}, {"$set": {"download.release": "20260901"}})
    with pytest.raises(ResourceError, match="Version '20261001' of mydisease.info__es8 wasn't downloaded"):
        await AutoHubFeature.install(feature, "mydisease.info")
    assert uploads == ["mydisease.info__es8"]


@pytest.mark.asyncio
async def test_install_up_to_date():
    class UpToDate(FakeDumper):
        def find_update_path(self, version, backend_version=None):
            return []

    dump_manager = DumperManager(job_manager=None)
    dump_manager.create_instance = lambda klass: klass()
    dump_manager.register = {"mydisease.info__es8": [UpToDate]}
    feature = SimpleNamespace(
        managers={"dump_manager": dump_manager, "job_manager": SimpleNamespace(loop=asyncio.get_running_loop())}
    )
    assert (
        await AutoHubFeature.install(feature, "mydisease.info")
        == "mydisease.info__es8 is up to date (version 20260901)"
    )
