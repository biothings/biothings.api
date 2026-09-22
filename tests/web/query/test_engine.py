from unittest.mock import AsyncMock, Mock

import pytest
from elasticsearch import ApiError, NotFoundError, RequestError

from biothings.web import connections
from biothings.web.query.builder import ESScrollID
from biothings.web.query.engine import AsyncESQueryBackend, ESQueryBackend, EndScrollInterrupt, RawResultInterrupt


def test_adjust_index_overrided():
    class MyESQueryBackend(ESQueryBackend):
        def adjust_index(self, original_index, query, **options):
            return "any_index"

    client = connections.get_es_client("http://localhost:9200", True)
    backend = MyESQueryBackend(client)
    query = None
    index = "original_index"
    index = backend.adjust_index(index, query)
    assert index == "any_index"


def _search_phase_execution_exception(root_cause_types):
    """
    Build an ApiError shaped like ES's response to a scroll request that
    partially failed, e.g. sentry MYVARIANTINFO-9A: some shards report
    search_context_missing_exception (scroll expired) and/or
    illegal_state_exception (a node was briefly unreachable). 500 has no
    dedicated exception subclass, so the real client raises plain ApiError.
    """
    _meta = Mock()
    _meta.status = 500
    body = {
        "error": {
            "type": "search_phase_execution_exception",
            "reason": "all shards failed",
            "root_cause": [{"type": rc_type} for rc_type in root_cause_types],
        }
    }
    return ApiError(message="search_phase_execution_exception", meta=_meta, body=body)


@pytest.mark.asyncio
async def test_scroll_request_error_reports_invalid_scroll_id():
    _meta = Mock()
    _meta.status = 400
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(side_effect=RequestError(message="malformed scroll id", meta=_meta, body={}))

    with pytest.raises(ValueError, match="Invalid or stale scroll_id."):
        await backend._scroll("abc")
    assert backend.client.scroll.call_count == 1


@pytest.mark.asyncio
async def test_scroll_not_found_error_reports_invalid_scroll_id():
    _meta = Mock()
    _meta.status = 404
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(side_effect=NotFoundError(message="context fully gone", meta=_meta, body={}))

    with pytest.raises(ValueError, match="Invalid or stale scroll_id."):
        await backend._scroll("abc")
    assert backend.client.scroll.call_count == 1


@pytest.mark.asyncio
async def test_scroll_context_missing_on_some_shards_reports_invalid_scroll_id():
    # search_phase_execution_exception whose only root cause is
    # search_context_missing_exception is functionally the same as the clean
    # NotFoundError case above, just reported by ES as a partial shard failure.
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(
        side_effect=_search_phase_execution_exception(["search_context_missing_exception"])
    )

    with pytest.raises(ValueError, match="Invalid or stale scroll_id."):
        await backend._scroll("abc")
    assert backend.client.scroll.call_count == 1  # no retry for a genuinely expired context


@pytest.mark.asyncio
async def test_scroll_node_unavailable_retries_once_then_succeeds():
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(
        side_effect=[
            _search_phase_execution_exception(["illegal_state_exception"]),
            {"hits": {"hits": []}},
        ]
    )

    res = await backend._scroll("abc")
    assert res == {"hits": {"hits": []}}
    assert backend.client.scroll.call_count == 2


@pytest.mark.asyncio
async def test_scroll_node_unavailable_retries_once_then_gives_up():
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(side_effect=_search_phase_execution_exception(["illegal_state_exception"]))

    with pytest.raises(ApiError):
        await backend._scroll("abc")
    assert backend.client.scroll.call_count == 2  # exactly one retry, no retry loop


@pytest.mark.asyncio
async def test_scroll_mixed_root_causes_prioritizes_retry():
    # matches the actual MYVARIANTINFO-9A event: most root causes were
    # search_context_missing_exception, two were illegal_state_exception.
    # A retry can resolve both at once, so it takes priority.
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(
        side_effect=[
            _search_phase_execution_exception(["search_context_missing_exception", "illegal_state_exception"]),
            {"hits": {"hits": []}},
        ]
    )

    res = await backend._scroll("abc")
    assert res == {"hits": {"hits": []}}
    assert backend.client.scroll.call_count == 2


@pytest.mark.asyncio
async def test_scroll_unrecognized_root_cause_reraised_untouched():
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(
        side_effect=_search_phase_execution_exception(["index_not_found_exception"])
    )

    with pytest.raises(ApiError):
        await backend._scroll("abc")
    assert backend.client.scroll.call_count == 1


@pytest.mark.asyncio
async def test_execute_scroll_raw_option_wraps_result():
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(return_value={"hits": {"hits": [{"_id": "1"}]}})

    with pytest.raises(RawResultInterrupt) as exc_info:
        await backend.execute(ESScrollID("abc"), raw=True)
    assert exc_info.value.data == {"hits": {"hits": [{"_id": "1"}]}}


@pytest.mark.asyncio
async def test_execute_scroll_empty_hits_ends_scroll_and_clears_context():
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(return_value={"hits": {"hits": []}})
    backend.client.clear_scroll = AsyncMock()

    with pytest.raises(EndScrollInterrupt):
        await backend.execute(ESScrollID("abc"))
    backend.client.clear_scroll.assert_awaited_once_with(scroll_id="abc")


@pytest.mark.asyncio
async def test_execute_scroll_after_node_blip_retry_returns_result():
    backend = AsyncESQueryBackend(client=AsyncMock())
    backend.client.scroll = AsyncMock(
        side_effect=[
            _search_phase_execution_exception(["illegal_state_exception"]),
            {"hits": {"hits": [{"_id": "1"}]}},
        ]
    )

    res = await backend.execute(ESScrollID("abc"))
    assert res == {"hits": {"hits": [{"_id": "1"}]}}
    assert backend.client.scroll.call_count == 2
