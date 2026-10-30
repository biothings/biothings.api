"""
Installing a data release restores its snapshot into a new index, then moves the API's alias to it:
only once all its shards are restored
"""

import asyncio
import logging
import time
from types import SimpleNamespace

import pytest
from elasticsearch import Elasticsearch

from biothings.hub.autoupdate import uploader as autoupdate_uploader
from biothings.hub.autoupdate.uploader import BiothingsUploader
from biothings.hub.dataload.uploader import ResourceError
from biothings.utils.es import ESIndexer

ELASTICSEARCH_HOST = "http://localhost:9200"


def indexer(es_client):
    """An ESIndexer using es_client"""
    idxr = ESIndexer.__new__(ESIndexer)
    idxr._es = es_client
    idxr._index = None
    return idxr


class FakeElasticsearch:
    """The Elasticsearch APIs used to follow a restore: 2 shards, recoveries as given"""

    def __init__(self, recoveries, can_allocate="no"):
        self.recoveries = recoveries
        self.indices = SimpleNamespace(
            recovery=lambda index: {index: {"shards": self.recoveries}} if self.recoveries else {},
            get_settings=lambda index: {index: {"settings": {"index": {"number_of_shards": "2"}}}},
        )
        self.cluster = SimpleNamespace(
            allocation_explain=lambda index, shard, primary: {
                "can_allocate": can_allocate,
                "allocate_explanation": "Elasticsearch isn't allowed to allocate this shard to any of the nodes",
                "node_allocation_decisions": [
                    {"deciders": [{"decision": "NO", "explanation": "the node is above the high watermark"}]}
                ],
            }
        )


def test_partial_restores_are_not_done():
    """A shard which can't be assigned to a node isn't listed in the recoveries: the restore isn't done"""
    restored = [{"id": 0, "primary": True, "stage": "DONE"}]
    status = indexer(FakeElasticsearch(restored)).get_restore_status("nde")
    assert status["status"] == "FAILED"
    assert status["progress"] == "50.00%"
    assert status["reason"] == (
        "shard 1: Elasticsearch isn't allowed to allocate this shard to any of the nodes "
        "(the node is above the high watermark)"
    )
    # waiting to be assigned (eg. throttled): in progress
    status = indexer(FakeElasticsearch(restored, can_allocate="throttled")).get_restore_status("nde")
    assert status == {"status": "IN_PROGRESS", "progress": "50.00%"}
    assert indexer(FakeElasticsearch([], can_allocate="throttled")).get_restore_status("nde")["status"] == "INIT"
    restored.append({"id": 1, "primary": True, "stage": "INDEX"})
    assert indexer(FakeElasticsearch(restored)).get_restore_status("nde")["status"] == "IN_PROGRESS"
    restored[1]["stage"] = "DONE"
    assert indexer(FakeElasticsearch(restored)).get_restore_status("nde") == {"status": "DONE", "progress": "100%"}


@pytest.fixture
def elasticsearch():
    client = Elasticsearch(ELASTICSEARCH_HOST)
    created = []

    def create_index(name, **settings):
        client.indices.create(
            index=name, settings=dict({"number_of_replicas": 0}, **settings), wait_for_active_shards=0
        )
        created.append(name)
        time.sleep(0.01)  # (distinct creation dates)

    yield SimpleNamespace(client=client, create_index=create_index)
    for name in created:
        client.options(ignore_status=404).indices.delete(index=name)


def test_restore_status_on_elasticsearch(elasticsearch):
    elasticsearch.create_index("bt_test_restore_ok", number_of_shards=2)
    # shards which can't be assigned to a node (as when they're all past their disk watermark)
    elasticsearch.create_index(
        "bt_test_restore_stuck", number_of_shards=2, **{"index.routing.allocation.require._name": "no_such_node"}
    )
    time.sleep(1)
    idxr = indexer(elasticsearch.client)
    assert idxr.get_restore_status("bt_test_restore_ok") == {"status": "DONE", "progress": "100%"}
    status = idxr.get_restore_status("bt_test_restore_stuck")
    assert status["status"] == "FAILED"
    assert "node does not match index setting [index.routing.allocation.require]" in status["reason"]


class FakeIndexer:
    """An ESIndexer for the API's alias, the indices it has, and the restore's statuses"""

    canonical_index_name = "nde_current"
    es_host = "http://es:9200"

    def __init__(self, indices, statuses):
        self.indices = dict(indices)  # name => creation date
        self.statuses = list(statuses)  # what get_restore_status() returns (or raises), in turn
        self.alias = None
        self.deleted = []

    def exists_index(self, index):
        return index in self.indices

    def restore(self, repo_name, snapshot_name, index_name, purge=False):
        self.indices[index_name] = max(self.indices.values(), default=0) + 1
        return {"accepted": True}

    def get_restore_status(self, index_name):
        status = self.statuses.pop(0)
        if isinstance(status, Exception):
            raise status
        return status

    def get_indice_names_by_settings(self, index, sort_by_creation_date=False, reverse=False):
        prefix = index.rstrip("*")
        names = {name for name in self.indices if name.startswith(prefix)}
        if self.alias and self.canonical_index_name.startswith(prefix):
            names.add(self.alias)  # (Elasticsearch expands a pattern matching an alias to its index)
        return sorted(names, key=self.indices.get, reverse=reverse)

    def update_alias(self, alias_name, index_name):
        self.alias = index_name

    def delete_index(self, index_name):
        self.deleted.append(index_name)
        self.indices.pop(index_name, None)

    def get_internal_number_of_replicas(self):
        return 1

    def set_internal_number_of_replicas(self, number):
        pass

    def check_index(self):
        pass


class Installer:
    """What BiothingsUploader.restore_snapshot() needs from its uploader"""

    AUTO_PURGE_INDEX = False
    name = "nde"

    def __init__(self, idxr):
        self.target_backend = SimpleNamespace(target_esidxer=idxr, count=lambda: 1000)
        self.logger = logging.getLogger("test_restore")

    def get_snapshot_repository_config(self, build_meta):
        return "repo", {"type": "s3", "settings": {"client": "default"}}

    def _get_repository(self, es_host, repo_name, auth):
        return {"repo": {"type": "s3", "settings": {"client": "default", "readonly": True}}}

    def get_pinfo(self):
        return {}


class JobManager:
    async def defer_to_thread(self, pinfo, func):
        future = asyncio.get_running_loop().create_future()
        future.set_result(func())
        return future


def install(idxr, monkeypatch, keep=1):
    monkeypatch.setattr(
        autoupdate_uploader,
        "btconfig",
        SimpleNamespace(
            STANDALONE_CONFIG={"_default": {}}, MONITOR_SNAPSHOT_DELAY=0, RELEASE_KEEP_N_RECENT_INDICES=keep
        ),
    )
    build_meta = {"metadata": {"snapshot_name": "nde_20261020"}}
    return BiothingsUploader.restore_snapshot(Installer(idxr), build_meta, JobManager(), append_ts=False)


@pytest.mark.asyncio
async def test_install_restores_then_moves_the_alias(monkeypatch):
    done = {"status": "DONE", "progress": "100%"}
    # the new index's name is taken (eg. the same release installed again): a random suffix, not a hang;
    # Elasticsearch doesn't answer a few times while restoring: still restoring
    idxr = FakeIndexer({"nde_20261015": 1, "nde_20261020": 2}, [TimeoutError("timed out")] * 4 + [done])
    idxr.alias = "nde_20261015"
    assert await install(idxr, monkeypatch, keep=0) == 1000
    assert idxr.alias.startswith("nde_20261020_") and idxr.alias in idxr.indices
    assert idxr.deleted == ["nde_20261015"]  # the index it pointed to (RELEASE_KEEP_N_RECENT_INDICES = 0)


@pytest.mark.asyncio
async def test_install_fails_when_a_shard_cant_be_restored(monkeypatch):
    failed = {"status": "FAILED", "progress": "50.00%", "reason": "shard 1: the node is above the high watermark"}
    idxr = FakeIndexer({"nde_20261015": 1}, [{"status": "IN_PROGRESS", "progress": "0%"}, failed])
    idxr.alias = "nde_20261015"
    with pytest.raises(ResourceError, match="status: FAILED .shard 1: the node is above the high watermark"):
        await install(idxr, monkeypatch)
    assert idxr.alias == "nde_20261015"  # unchanged
    assert idxr.deleted == ["nde_20261020"]  # the new index, useless
    # Elasticsearch not answering, again and again: fails, without deleting the index being restored
    idxr = FakeIndexer({}, [TimeoutError("timed out")] * 5)
    with pytest.raises(ResourceError, match="status: ERROR .TimeoutError: timed out"):
        await install(idxr, monkeypatch)
    assert idxr.deleted == [] and idxr.alias is None
