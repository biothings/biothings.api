"""
Tests for biothings.utils.serializer
"""

import datetime
from collections import UserDict

from biothings.utils.serializer import to_json

NAIVE = datetime.datetime(2026, 9, 1, 18, 47, 24)


def test_to_json_naive_datetimes():
    assert to_json({"date": NAIVE}) == '{"date":"2026-09-01T18:47:24"}'


def test_to_json_naive_utc():
    """Naive datetimes can be considered UTC, eg. dates from MongoDB sent to BioThings Studio"""
    data = {"date": NAIVE, "list": [NAIVE], "tuple": (NAIVE,), "user_dict": UserDict({"date": NAIVE}), "n": 1}
    assert to_json(data, naive_utc=True) == (
        '{"date":"2026-09-01T18:47:24Z","list":["2026-09-01T18:47:24Z"],"tuple":["2026-09-01T18:47:24Z"],'
        '"user_dict":{"date":"2026-09-01T18:47:24Z"},"n":1}'
    )
    pacific = datetime.timezone(datetime.timedelta(hours=-7))
    aware = datetime.datetime(2026, 9, 1, 11, 47, 24, tzinfo=pacific)
    assert to_json({"date": aware}, naive_utc=True) == '{"date":"2026-09-01T11:47:24-07:00"}'
