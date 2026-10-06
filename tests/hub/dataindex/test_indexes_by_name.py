import asyncio
import fnmatch
import logging
from types import SimpleNamespace

import elasticsearch
import pytest

from biothings.hub.dataindex import indexer
from biothings.hub.dataindex.indexer import IndexManager


def index(build_version, created):
    meta = {"biothing_type": "chem", "build_version": build_version, "stats": {"total": 1000}}
    return {"mappings": {"_meta": meta}, "settings": {"index": {"creation_date": created}}}


INDICES = {  # Elasticsearch host => its indices
    "http://su10:9200": {"chem_20250101_aaaaaaaa": index("20250101", 1), "other": index("1", 0)},
    "http://su12:9200": {
        "chem_20261001_bbbbbbbb": index("20261001", 3),
        "chem_20260901_cccccccc": index("20260901", 2),
    },
}


class FakeElasticsearch:
    def __init__(self, hosts):
        self.hosts = hosts
        self.indices = SimpleNamespace(get=self.get)

    async def get(self, index):
        if self.hosts == "http://down:9200":
            raise elasticsearch.ConnectionError("unreachable")
        found = {name: info for name, info in INDICES[self.hosts].items() if fnmatch.fnmatch(name, index)}
        if not found and "*" not in index:
            raise elasticsearch.NotFoundError("index_not_found_exception", None, None)
        return found

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
async def test_indexes_by_name_searches_every_environment(monkeypatch, caplog):
    monkeypatch.setattr(indexer, "AsyncElasticsearch", FakeElasticsearch)
    manager = SimpleNamespace(
        register={
            "local7": {"args": {"hosts": "http://su10:9200"}},
            "local8": {"args": {"hosts": "http://su12:9200"}},
            "down": {"args": {"hosts": "http://down:9200"}},
        },
        job_manager=SimpleNamespace(loop=asyncio.get_running_loop()),
        logger=logging.getLogger("test_indexes_by_name"),
    )
    with caplog.at_level(logging.WARNING):
        indexes = await IndexManager.get_indexes_by_name(manager, "chem_*")
    assert [(found["index_name"], found["environment"]["name"]) for found in indexes] == [
        ("chem_20261001_bbbbbbbb", "local8"),
        ("chem_20260901_cccccccc", "local8"),
        ("chem_20250101_aaaaaaaa", "local7"),
    ]
    assert "Can't list the indices of environment 'down'" in caplog.text
    only_local8 = await IndexManager.get_indexes_by_name(manager, "chem_*", env_name="local8")
    assert len(only_local8) == 2
