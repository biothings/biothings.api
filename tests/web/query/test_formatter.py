import logging

import pytest

from biothings.web.query import ESResultFormatter
from biothings.web.query.formatter import FormatterDict


def _has_error_log(caplog):
    return bool([r for r in caplog.records if r.levelno >= logging.ERROR])


def test_es_error_response_surfaces_reason(caplog):
    # an error embedded in a (multisearch) response should surface the
    # underlying ES reason, preferring the more specific root_cause, rather
    # than a generic "Invalid response format" message.
    formatter = ESResultFormatter()
    error_response = {
        "error": {
            "root_cause": [
                {"type": "query_shard_exception", "reason": "No mapping found for [score] in order to sort on"}
            ],
            "type": "search_phase_execution_exception",
            "reason": "all shards failed",
        },
        "status": 400,
    }
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as exc_info:
        formatter.transform(error_response)
    assert str(exc_info.value) == "No mapping found for [score] in order to sort on"

    # falls back to the top-level reason when there is no root_cause
    with pytest.raises(ValueError) as exc_info:
        formatter.transform({"error": {"reason": "some top-level reason"}, "status": 400})
    assert str(exc_info.value) == "some top-level reason"


def test_es_client_errors_logged_below_error(caplog):
    # a 400 status means ES rejected the query as invalid for the index, a
    # user-input error, which must be logged BELOW error level so it is not
    # captured as a Sentry event.
    formatter = ESResultFormatter()
    reason = "it is mandatory to set the [nested] context on the nested sort field: [genomic_pos.start]."
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as exc_info:
        formatter.transform({"error": {"root_cause": [{"reason": reason}]}, "status": 400})
    assert str(exc_info.value) == reason
    assert not _has_error_log(caplog)


def test_es_other_errors_still_logged_at_error(caplog):
    # an error with any status other than 400 is a server-side failure, it
    # should still be surfaced to the user, but kept at ERROR so Sentry
    # reports it.
    formatter = ESResultFormatter()
    caplog.clear()
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as exc_info:
        formatter.transform({"error": {"reason": "something unexpected went wrong"}, "status": 500})
    assert str(exc_info.value) == "something unexpected went wrong"
    assert _has_error_log(caplog)

    # a response with no extractable reason is unexpected/malformed -> ERROR
    # and the generic "Invalid response format" message.
    caplog.clear()
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as exc_info:
        formatter.transform({"error": {}, "status": 500})
    assert str(exc_info.value) == "Invalid response format"
    assert _has_error_log(caplog)


def test_es_1():
    formatter = ESResultFormatter()
    print(formatter.transform({"hits": {"total": {}, "hits": []}}))


def test_es_2():
    formatter = ESResultFormatter()
    print(
        formatter.transform(
            [
                {"hits": {"total": {}, "hits": [{"_source": {"_id": "1"}}, {"_source": {"_id": "2"}}]}},
                {"hits": {"total": {}, "hits": [{"_source": {"_id": "1"}}]}},
                {"hits": {"total": {}, "hits": []}},
            ]
        )
    )


def test_es_3():
    formatter = ESResultFormatter()
    print(
        formatter.transform(
            {"hits": {"total": {}, "hits": []}},
            one=True,
        )
    )
    print(
        formatter.transform(
            {
                "hits": {
                    "total": {},
                    "hits": [
                        {"_source": {"_id": "1"}},
                    ],
                }
            },
            one=True,
        )
    )
    print(
        formatter.transform(
            {
                "hits": {
                    "total": {},
                    "hits": [
                        {"_source": {"_id": "1"}},
                        {"_source": {"_id": "2"}},
                    ],
                }
            },
            one=True,
        )
    )


def test_formatter_dict_include_keeps_only_requested_keys():
    data = FormatterDict(
        {
            "_id": "1017",
            "symbol": "CDK2",
            "name": "cyclin dependent kinase 2",
        }
    )

    data.include({"_id", "symbol"})

    assert data == {"_id": "1017", "symbol": "CDK2"}
