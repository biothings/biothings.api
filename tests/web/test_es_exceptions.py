from unittest.mock import Mock

import pytest
from elastic_transport import ObjectApiResponse

from biothings.web.query.pipeline import (
    AuthenticationException,
    AuthorizationException,
    ConflictError,
    EndScrollInterrupt,
    NotFoundError,
    QueryPipelineException,
    QueryPipelineInterrupt,
    RawQueryInterrupt,
    RawResultInterrupt,
    RequestError,
    TransportError,
    capturesESExceptions,
)


@pytest.mark.asyncio
async def test_raw_query_interrupt():
    @capturesESExceptions
    async def func():
        raise RawQueryInterrupt({"error": "test_error"})

    with pytest.raises(QueryPipelineInterrupt) as exc_info:
        await func()
    assert exc_info.value.code == 200
    assert exc_info.value.summary is None
    assert exc_info.value.details == {"error": "test_error"}


@pytest.mark.asyncio
async def test_end_scroll_interrupt():
    @capturesESExceptions
    async def func():
        raise EndScrollInterrupt()

    with pytest.raises(QueryPipelineInterrupt) as exc_info:
        await func()
    assert exc_info.value.code == 200
    assert exc_info.value.summary is None
    assert exc_info.value.details == {"success": False, "error": "No more results to return."}


@pytest.mark.asyncio
async def test_raw_result_interrupt():
    # a single search returns an ObjectApiResponse, which is unwrapped via .body
    @capturesESExceptions
    async def func():
        raise RawResultInterrupt(ObjectApiResponse(body="test_body", meta=None))

    with pytest.raises(QueryPipelineInterrupt) as exc_info:
        await func()
    assert exc_info.value.code == 200
    assert exc_info.value.summary is None
    assert exc_info.value.details == "test_body"


@pytest.mark.asyncio
async def test_raw_result_interrupt_multisearch():
    # a multisearch (e.g. POST queries) returns a plain list of responses,
    # which must be passed through as-is (no .body attribute to unwrap)
    responses = [{"hits": {"total": 1}}, {"hits": {"total": 2}}]

    @capturesESExceptions
    async def func():
        raise RawResultInterrupt(responses)

    with pytest.raises(QueryPipelineInterrupt) as exc_info:
        await func()
    assert exc_info.value.code == 200
    assert exc_info.value.summary is None
    assert exc_info.value.details == responses


@pytest.mark.asyncio
async def test_assertion_error():
    @capturesESExceptions
    async def func():
        raise AssertionError("test_assertion_error")

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 500
    assert exc_info.value.summary == "test_assertion_error"
    assert exc_info.value.details is None


@pytest.mark.asyncio
async def test_value_error():
    @capturesESExceptions
    async def func():
        raise ValueError("test_value_error")

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 400
    assert exc_info.value.summary == "ValueError"
    assert exc_info.value.details == "test_value_error"


@pytest.mark.asyncio
async def test_connection_error():
    @capturesESExceptions
    async def func():
        raise ConnectionError(message="test_connection_error", meta={}, body={})

    with pytest.raises(Exception) as exc_info:
        await func()
    assert exc_info.value.code == 400
    assert exc_info.value.summary == "TypeError"
    assert exc_info.value.details == "ConnectionError() takes no keyword arguments"


@pytest.mark.asyncio
async def test_request_error():
    _meta = Mock()
    _meta.status = 400

    @capturesESExceptions
    async def func():
        raise RequestError(message="test_request_error", meta=_meta, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 400
    assert exc_info.value.summary == "test_request_error"


@pytest.mark.asyncio
async def test_not_found_error():
    _meta = Mock()
    _meta.status = 404

    @capturesESExceptions
    async def func():
        raise NotFoundError(message="test_not_found_error", meta=_meta, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()

    assert exc_info.value.code == 404
    assert exc_info.value.summary == "test_not_found_error"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_conflict_error():
    _meta = Mock()
    _meta.status = 409

    @capturesESExceptions
    async def func():
        raise ConflictError(message="test_conflict_error", meta=_meta, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 409
    assert exc_info.value.summary == "test_conflict_error"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_authentication_exception():
    _meta = Mock()
    _meta.status = 403

    @capturesESExceptions
    async def func():
        raise AuthenticationException(message="test_authentication_exception", meta=_meta, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 403
    assert exc_info.value.summary == "test_authentication_exception"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_authorization_exception():
    _meta = Mock()
    _meta.status = 403

    @capturesESExceptions
    async def func():
        raise AuthorizationException(message="test_authorization_exception", meta=_meta, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 403
    assert exc_info.value.summary == "test_authorization_exception"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_index_not_found_exception():
    @capturesESExceptions
    async def func():
        exc = Exception(message="test_index_not_found_exception", meta={}, body={})
        exc.status_code = 500
        exc.info = {"error": {"type": "index_not_found_exception", "reason": "test_reason"}}
        raise exc

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 400
    assert exc_info.value.summary == "TypeError"
    assert exc_info.value.details == "Exception() takes no keyword arguments"


@pytest.mark.asyncio
async def test_es_rejected_execution_exception():
    @capturesESExceptions
    async def func():
        exc = TransportError("test_es_rejected_execution_exception")
        exc.status_code = 503
        exc.info = {
            "error": {"type": "es_rejected_execution_exception", "reason": "rejected execution of TimedRunnable..."}
        }
        raise exc

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 503
    assert exc_info.value.summary == "Service Unavailable"
    assert exc_info.value.details == "Elasticsearch cluster overloaded"


@pytest.mark.asyncio
async def test_search_phase_execution_exception_rejected_execution():
    @capturesESExceptions
    async def func():
        exc = TransportError("test_generic_exception")
        exc.status_code = 500
        exc.info = {"error": {"type": "search_phase_execution_exception", "reason": "rejected execution"}}
        raise exc

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 503
    assert exc_info.value.summary == ""
    assert exc_info.value.details is None


@pytest.mark.asyncio
async def test_search_phase_execution_exception_not_rejected_execution():
    @capturesESExceptions
    async def func():
        exc = TransportError("test_generic_exception")
        exc.status_code = 500
        exc.error = "test_generic_exception"
        exc.info = {
            "error": {
                "type": "search_phase_execution_exception",
                "reason": "any kind of execution",
                "root_cause": [{"reason": "reason"}],
            }
        }
        raise exc

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 500
    assert exc_info.value.summary == "test_generic_exception"
    assert exc_info.value.details["debug"]["error"]["type"] == "search_phase_execution_exception"
    assert exc_info.value.details["debug"]["error"]["reason"] == "any kind of execution"
    assert exc_info.value.details["debug"]["error"]["root_cause"][0]["reason"] == "reason"


@pytest.mark.asyncio
async def test_too_many_requests_error():
    @capturesESExceptions
    async def func():
        exc = TransportError(
            {
                "status_code": 429,
            }
        )
        exc.status_code = 429
        exc.error = "too_many_requests"
        exc.info = "too_many_requests"
        raise exc

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()

    assert exc_info.value.code == 503
    assert exc_info.value.summary == ""
    assert exc_info.value.details is None
