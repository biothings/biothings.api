import pytest

from biothings.web.query.pipeline import (
    capturesESExceptions,
    RawQueryInterrupt,
    QueryPipelineInterrupt,
    EndScrollInterrupt,
    RawResultInterrupt,
    QueryPipelineException,
    RequestError,
    NotFoundError,
    ConflictError,
    AuthenticationException,
    AuthorizationException,
    TransportError,
)


@pytest.mark.asyncio
async def test_raw_query_interrupt():
    @capturesESExceptions
    async def func():
        raise RawQueryInterrupt({"error": "test_error"})

    with pytest.raises(QueryPipelineInterrupt) as exc_info:
        await func()
    assert exc_info.value.code == 200
    assert exc_info.value.summary == None
    assert exc_info.value.details == {"error": "test_error"}


@pytest.mark.asyncio
async def test_end_scroll_interrupt():
    @capturesESExceptions
    async def func():
        raise EndScrollInterrupt()

    with pytest.raises(QueryPipelineInterrupt) as exc_info:
        await func()
    assert exc_info.value.code == 200
    assert exc_info.value.summary == None
    assert exc_info.value.details == {"success": False, "error": "No more results to return."}


@pytest.mark.asyncio
async def test_raw_result_interrupt():
    @capturesESExceptions
    async def func():
        raise RawResultInterrupt(type("obj", (object,), {"body": "test_body"}))

    with pytest.raises(QueryPipelineInterrupt) as exc_info:
        await func()
    assert exc_info.value.code == 200
    assert exc_info.value.summary == None
    assert exc_info.value.details == "test_body"


@pytest.mark.asyncio
async def test_assertion_error():
    @capturesESExceptions
    async def func():
        raise AssertionError("test_assertion_error")

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 500
    assert exc_info.value.summary == "test_assertion_error"
    assert exc_info.value.details == None


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
        raise ConnectionError("test_connection_error")

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 500
    assert exc_info.value.summary == "ElasticsearchException"
    assert exc_info.value.details == "test_connection_error"


@pytest.mark.asyncio
async def test_request_error():
    @capturesESExceptions
    async def func():
        raise RequestError(message="test_request_error", meta={}, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 400
    assert exc_info.value.summary == "test_request_error"


@pytest.mark.asyncio
async def test_not_found_error():
    @capturesESExceptions
    async def func():
        raise NotFoundError(message="test_not_found_error", meta={}, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()

    assert exc_info.value.code == 404
    assert exc_info.value.summary == "test_not_found_error"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_conflict_error():
    @capturesESExceptions
    async def func():
        raise ConflictError(message="test_conflict_error", meta={}, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 409
    assert exc_info.value.summary == "test_conflict_error"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_authentication_exception():
    @capturesESExceptions
    async def func():
        raise AuthenticationException(message="test_authentication_exception", meta={}, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 403
    assert exc_info.value.summary == "test_authentication_exception"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_authorization_exception():
    @capturesESExceptions
    async def func():
        raise AuthorizationException(message="test_authorization_exception", meta={}, body={})

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 403
    assert exc_info.value.summary == "test_authorization_exception"
    assert exc_info.value.details == {}


@pytest.mark.asyncio
async def test_generic_exception():
    @capturesESExceptions
    async def func():
        exc = Exception("test_generic_exception")
        exc.status_code = 500
        exc.info = {"error": {"type": "index_not_found_exception", "reason": "test_reason"}}
        raise exc

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()
    assert exc_info.value.code == 500
    assert exc_info.value.summary == "ElasticsearchException"
    assert exc_info.value.details == "test_generic_exception"


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
    assert exc_info.value.details == None


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
        exc = TransportError({
            "status_code": 429,
        })
        exc.status_code = 429
        exc.error = "too_many_requests"
        exc.info = "too_many_requests"
        raise exc

    with pytest.raises(QueryPipelineException) as exc_info:
        await func()

    assert exc_info.value.code == 503
    assert exc_info.value.summary == ""
    assert exc_info.value.details == None
