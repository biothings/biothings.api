"""
Data releases installed in several Elasticsearch environments are named <release>__<environment>:
installing one needs that name, unless it's installed in one environment only
"""

import asyncio
from types import SimpleNamespace

import pytest

from biothings.hub.dataload.dumper import DumperManager
from biothings.hub.manager import ResourceNotFound
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
