from copy import deepcopy
from unittest.mock import Mock

import pytest

from biothings.utils import es


@pytest.fixture
def indexer(monkeypatch):
    client = Mock()
    client.info.return_value = {"version": {"number": "8.18.0"}}
    client.snapshot.get.return_value = {"snapshots": [{"indices": ["source_index"]}]}
    client.snapshot.restore.return_value = {"accepted": True}
    monkeypatch.setattr(es, "get_es", lambda *args, **kwargs: client)
    return es.ESIndexer("test_index", check_index=False)


def test_close_client_releases_connections_without_closing_index(indexer):
    indexer.close_client()

    indexer._es.close.assert_called_once_with()
    indexer._es.indices.close.assert_not_called()


def test_close_still_closes_index_without_closing_client(indexer):
    indexer.close()

    indexer._es.indices.close.assert_called_once_with(index="test_index")
    indexer._es.close.assert_not_called()


def test_close_client_propagates_cleanup_errors(indexer):
    indexer._es.close.side_effect = RuntimeError("Connection cleanup failed")

    with pytest.raises(RuntimeError, match="Connection cleanup failed"):
        indexer.close_client()

    indexer._es.indices.close.assert_not_called()


def test_restore_preserves_defaults_and_leaves_client_open(indexer):
    result = indexer.restore("test_repo", "test_snapshot")

    assert result == {"accepted": True}
    indexer._es.snapshot.get.assert_called_once_with(repository="test_repo", snapshot="test_snapshot")
    indexer._es.snapshot.restore.assert_called_once_with(
        repository="test_repo",
        snapshot="test_snapshot",
        body={
            "indices": "source_index",
            "rename_replacement": "source_index",
            "ignore_unavailable": True,
            "rename_pattern": "(.+)",
            "include_global_state": False,
        },
    )
    assert indexer._es.indices.mock_calls == []
    indexer._es.close.assert_not_called()


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"index_settings": {"index.number_of_replicas": 0}}, id="replica-override"),
        pytest.param({"index_settings": {"index": {"number_of_replicas": 1}}}, id="nested-settings"),
        pytest.param({"ignore_index_settings": ["index.routing.allocation.*"]}, id="ignore-allocation"),
        pytest.param(
            {
                "index_settings": {"index.number_of_replicas": 0},
                "ignore_index_settings": ["index.routing.allocation.require._name"],
            },
            id="override-and-ignore",
        ),
        pytest.param({"index_settings": {}}, id="empty-settings"),
        pytest.param({"ignore_index_settings": []}, id="empty-ignored-settings"),
        pytest.param({"index_settings": None, "ignore_index_settings": None}, id="explicit-none"),
    ],
)
def test_restore_forwards_settings_without_modifying_input(indexer, options):
    original = deepcopy(options)

    indexer.restore("test_repo", "test_snapshot", **options)

    body = indexer._es.snapshot.restore.call_args.kwargs["body"]
    for name in ("index_settings", "ignore_index_settings"):
        if original.get(name) is None:
            assert name not in body
        else:
            assert body[name] == original[name]
    assert options == original


def test_restore_preserves_positional_destination_and_purge_arguments(indexer):
    indexer.restore("test_repo", "test_snapshot", "destination_index", True)

    indexer._es.indices.get.assert_called_once_with(index="destination_index")
    indexer._es.indices.delete.assert_called_once_with(index="destination_index")
    body = indexer._es.snapshot.restore.call_args.kwargs["body"]
    assert body["indices"] == "source_index"
    assert body["rename_replacement"] == "destination_index"
