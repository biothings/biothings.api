import pytest

from biothings.web.query import ESResultFormatter


def test_es_error_response_surfaces_reason():
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
    with pytest.raises(ValueError) as exc_info:
        formatter.transform(error_response)
    assert str(exc_info.value) == "No mapping found for [score] in order to sort on"

    # falls back to the top-level reason when there is no root_cause
    with pytest.raises(ValueError) as exc_info:
        formatter.transform({"error": {"reason": "some top-level reason"}, "status": 400})
    assert str(exc_info.value) == "some top-level reason"


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
