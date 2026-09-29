"""
Tests the batch input size caps shipped in biothings.web.settings.default.

Both batch endpoints bound how many items a single POST may carry, so that one
request cannot fan out into an unbounded amount of Elasticsearch work:

    POST /query       q    -> 5000 terms, each becoming a clause in an msearch
    POST /<biothing>  ids  -> 1000 ids

"""

import pytest

from biothings.web.options import OptionError, OptionSet
from biothings.web.settings.default import ANNOTATION_KWARGS, QUERY_KWARGS

QUERY_POST_MAX = 5000
ANNOTATION_POST_MAX = 1000


def _terms(count):
    return [str(i) for i in range(count)]


@pytest.mark.parametrize(
    "kwargs, method, keyword, maximum",
    [
        (QUERY_KWARGS, "POST", "q", QUERY_POST_MAX),
        (ANNOTATION_KWARGS, "POST", "id", ANNOTATION_POST_MAX),
    ],
    ids=["query_q", "annotation_id"],
)
def test_batch_cap_declared_on_a_list_parameter(kwargs, method, keyword, maximum):
    # "max" is a no-op on non-list types, so the declared type matters
    # as much as the number itself.
    defdict = kwargs[method][keyword]
    assert defdict["type"] is list
    assert defdict["max"] == maximum


@pytest.mark.parametrize(
    "kwargs, method, keyword, maximum",
    [
        (QUERY_KWARGS, "POST", "q", QUERY_POST_MAX),
        (ANNOTATION_KWARGS, "POST", "id", ANNOTATION_POST_MAX),
    ],
    ids=["query_q", "annotation_id"],
)
def test_batch_cap_enforced_at_the_boundary(kwargs, method, keyword, maximum):
    optionset = OptionSet(kwargs)

    # at the limit: accepted, and every item is preserved
    args = optionset.parse(method, (None, None, {keyword: _terms(maximum)}))
    assert len(args[keyword]) == maximum

    # one over: rejected, and the error carries what the client needs to
    # correct the request (this is what the handler turns into a 400 body).
    with pytest.raises(OptionError) as err:
        optionset.parse(method, (None, None, {keyword: _terms(maximum + 1)}))
    assert err.value.info["keyword"] == keyword
    assert err.value.info["max"] == maximum
    assert err.value.info["size"] == maximum + 1


def test_query_post_cap_applies_to_comma_separated_input():
    # clients most often send q as a single comma-separated string rather than
    # a JSON list; the cap has to survive that conversion too.
    optionset = OptionSet(QUERY_KWARGS)

    ok = ",".join(_terms(QUERY_POST_MAX))
    assert len(optionset.parse("POST", (None, None, {"q": ok}))["q"]) == QUERY_POST_MAX

    too_many = ",".join(_terms(QUERY_POST_MAX + 1))
    with pytest.raises(OptionError) as err:
        optionset.parse("POST", (None, None, {"q": too_many}))
    assert err.value.info["size"] == QUERY_POST_MAX + 1


def test_query_get_is_not_capped():
    # GET q is a single query string, not a batch: it builds one Search rather
    # than an msearch, so it is deliberately left uncapped here and bounded by
    # the HTTP layer instead.
    assert "max" not in QUERY_KWARGS["GET"]["q"]

    optionset = OptionSet(QUERY_KWARGS)
    long_q = "x" * (QUERY_POST_MAX * 2)
    assert optionset.parse("GET", (None, {"q": long_q}))["q"] == long_q
