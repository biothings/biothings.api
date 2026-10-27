"""
Installing a data release restores its snapshot into a new index, then moves the API's alias to it:
only once all its shards are restored, and the indices restored by previous installs are deleted,
beyond the number to keep (RELEASE_KEEP_N_RECENT_INDICES)
"""

import time
from types import SimpleNamespace

import pytest
from elasticsearch import Elasticsearch

from biothings.hub.autoupdate.uploader import installed_indices
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


def test_installed_indices(elasticsearch):
    """The indices restored for an alias: named after it, and the one it points to (named otherwise before)"""
    elasticsearch.create_index("bt_test_nde_20260901_abcdefgh_202609020300")  # restored before (old naming)
    elasticsearch.create_index("bt_test_nde_current__bt_test_nde_20261001_ijklmnop_202610020300")
    elasticsearch.create_index("bt_test_nde_current__bt_test_nde_20261015_qrstuvwx_202610160300")
    elasticsearch.create_index("bt_test_nde_current_v2__bt_test_nde_20261015_qrstuvwx_202610160300")  # another alias
    elasticsearch.create_index("bt_test_nde_20261020_yzabcdef")  # eg. the hub's own index for a build
    elasticsearch.client.indices.put_alias(
        index="bt_test_nde_20260901_abcdefgh_202609020300", name="bt_test_nde_current"
    )
    new_index = "bt_test_nde_current__bt_test_nde_20261020_yzabcdef_202610210300"
    elasticsearch.create_index(new_index)
    assert installed_indices(indexer(elasticsearch.client), "bt_test_nde_current", exclude=new_index) == [
        "bt_test_nde_20260901_abcdefgh_202609020300",
        "bt_test_nde_current__bt_test_nde_20261001_ijklmnop_202610020300",
        "bt_test_nde_current__bt_test_nde_20261015_qrstuvwx_202610160300",
    ]
    assert installed_indices(indexer(elasticsearch.client), "bt_test_first_install") == []
