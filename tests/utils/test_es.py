from unittest.mock import Mock

import pytest

from biothings.utils import es


@pytest.fixture
def indexer(monkeypatch):
    client = Mock()
    client.info.return_value = {"version": {"number": "8.18.0"}}
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
