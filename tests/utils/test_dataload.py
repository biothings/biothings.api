"""
Tests for dict_sweep and _val_to_delete in biothings.utils.dataload,
specifically the handling of NaN-like values.

NaN-like values (float NaN, pandas.NA, pandas.NaT) are only removed
when explicitly included in the vals list (opt-in).
"""

import math

import pytest

from biothings.utils.dataload import _val_to_delete, dict_sweep

# ---------------------------------------------------------------------------
# Fake pandas-like NA / NaT types for testing without importing pandas.
# __module__ mirrors real pandas internals closely enough to exercise fallback
# detection without importing pandas.
# ---------------------------------------------------------------------------


class NAType:
    """Mimics pandas.NA (pandas.core.arrays.masked.NAType)."""

    __module__ = "pandas._libs.missing"

    def __bool__(self):
        raise TypeError("boolean value of NA is ambiguous")

    def __eq__(self, other):
        raise TypeError("boolean value of NA is ambiguous")

    def __hash__(self):
        return 0

    def __repr__(self):
        return "<NA>"


class NaTType:
    """Mimics pandas.NaT (pandas._libs.tslibs.nattype.NaTType)."""

    __module__ = "pandas._libs.tslibs.nattype"

    def __bool__(self):
        raise TypeError("boolean value of NaT is ambiguous")

    def __eq__(self, other):
        raise TypeError("boolean value of NaT is ambiguous")

    def __hash__(self):
        return 0

    def __repr__(self):
        return "NaT"


_NA = NAType()
_NaT = NaTType()

_DEFAULT_VALS = [".", "-", "", "NA", "none", " ", "Not Available", "unknown"]
_VALS_WITH_NAN = _DEFAULT_VALS + [float("nan"), _NA, _NaT]


@pytest.fixture(scope="module")
def pandas():
    return pytest.importorskip("pandas")


@pytest.fixture(scope="module")
def numpy():
    return pytest.importorskip("numpy")


# ---------------------------------------------------------------------------
# _val_to_delete helper tests
# ---------------------------------------------------------------------------


class TestValToDelete:
    # -- default vals (no NaN entries) -----------------------------------

    def test_default_vals_matched(self):
        for val in _DEFAULT_VALS:
            assert _val_to_delete(val, _DEFAULT_VALS) is True, f"should delete {val!r}"

    def test_regular_values_not_deleted(self):
        for val in [0, 1, -1, "hello", None, [], {}, 0.0, 1.5, True, False]:
            assert _val_to_delete(val, _DEFAULT_VALS) is False, f"should keep {val!r}"

    def test_string_vals_matched(self):
        assert _val_to_delete("NA", "NA") is True

    def test_float_nan_not_deleted_by_default(self):
        """float NaN is kept when vals does not contain a NaN float."""
        assert _val_to_delete(float("nan"), _DEFAULT_VALS) is False

    def test_na_not_deleted_by_default(self):
        """Mock pandas NA is kept when vals does not contain a pandas NA."""
        assert _val_to_delete(_NA, _DEFAULT_VALS) is False

    def test_nat_not_deleted_by_default(self):
        """Mock pandas NaT is kept when vals does not contain a pandas NaT."""
        assert _val_to_delete(_NaT, _DEFAULT_VALS) is False

    # -- vals with NaN entries (opt-in) ----------------------------------

    def test_float_nan_deleted_when_in_vals(self):
        assert _val_to_delete(float("nan"), _VALS_WITH_NAN) is True

    def test_na_deleted_when_in_vals(self):
        assert _val_to_delete(_NA, _VALS_WITH_NAN) is True

    def test_nat_deleted_when_in_vals(self):
        assert _val_to_delete(_NaT, _VALS_WITH_NAN) is True

    def test_na_and_nat_are_not_interchangeable(self):
        assert _val_to_delete(_NA, _DEFAULT_VALS + [_NaT]) is False
        assert _val_to_delete(_NaT, _DEFAULT_VALS + [_NA]) is False

    def test_regular_match_after_na_like_value(self):
        assert _val_to_delete("", [_NA, ""]) is True


# ---------------------------------------------------------------------------
# dict_sweep — default vals (NaN kept)
# ---------------------------------------------------------------------------


class TestDictSweepDefaultKeepsNan:
    def test_float_nan_kept(self):
        d = {"a": 1, "b": float("nan")}
        result = dict_sweep(d)
        assert "b" in result
        assert math.isnan(result["b"])

    def test_na_kept(self):
        d = {"a": 1, "b": _NA}
        result = dict_sweep(d)
        assert "b" in result
        assert result["b"] is _NA

    def test_nat_kept(self):
        d = {"a": 1, "b": _NaT}
        result = dict_sweep(d)
        assert "b" in result
        assert result["b"] is _NaT

    def test_nan_kept_in_list(self):
        d = {"a": [1, float("nan"), 2]}
        result = dict_sweep(d)
        assert len(result["a"]) == 3

    def test_default_vals_still_removed(self):
        d = {"a": ".", "b": "-", "c": "", "d": "keep"}
        result = dict_sweep(d)
        assert result == {"d": "keep"}


# ---------------------------------------------------------------------------
# dict_sweep — opt-in NaN removal (NaN in vals)
# ---------------------------------------------------------------------------


class TestDictSweepOptInNanRemoval:
    def test_float_nan_removed(self):
        d = {"a": 1, "b": float("nan")}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": 1}

    def test_na_removed(self):
        d = {"a": 1, "b": _NA}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": 1}

    def test_nat_removed(self):
        d = {"a": 1, "b": _NaT}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": 1}

    def test_multiple_nan_types_removed(self):
        d = {"a": float("nan"), "b": _NA, "c": _NaT, "d": "keep"}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"d": "keep"}


# ---------------------------------------------------------------------------
# dict_sweep — NaN inside lists (opt-in)
# ---------------------------------------------------------------------------


class TestDictSweepNanInList:
    def test_nan_removed_from_list(self):
        d = {"a": [1, float("nan"), 2]}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": [1, 2]}

    def test_na_removed_from_list(self):
        d = {"a": [1, _NA, 2]}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": [1, 2]}

    def test_nat_removed_from_list(self):
        d = {"a": [1, _NaT, 2]}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": [1, 2]}

    def test_list_becomes_empty_after_nan_removal(self):
        d = {"a": [float("nan")]}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert "a" not in result

    def test_nan_in_list_with_remove_invalid_list(self):
        d = {"a": [float("nan"), _NA, "valid"]}
        result = dict_sweep(d, vals=_VALS_WITH_NAN, remove_invalid_list=True)
        assert result == {"a": ["valid"]}

    def test_all_nan_list_removed_with_remove_invalid_list(self):
        d = {"a": [float("nan"), _NA, _NaT]}
        result = dict_sweep(d, vals=_VALS_WITH_NAN, remove_invalid_list=True)
        assert "a" not in result

    def test_all_invalid_list_preserves_false_mode_behavior(self):
        d = {"gene": [None, None], "site": ["Intron", None], "snp_build": 136}
        result = dict_sweep(d, vals=[None], remove_invalid_list=False)
        assert result == {"gene": [None], "site": ["Intron"], "snp_build": 136}

    def test_all_invalid_list_removed_with_remove_invalid_list(self):
        d = {"gene": [None, None], "site": ["Intron", None], "snp_build": 136}
        result = dict_sweep(d, vals=[None], remove_invalid_list=True)
        assert result == {"site": ["Intron"], "snp_build": 136}


# ---------------------------------------------------------------------------
# dict_sweep — NaN in nested dicts (opt-in)
# ---------------------------------------------------------------------------


class TestDictSweepNanNested:
    def test_nan_in_nested_dict(self):
        d = {"a": {"b": float("nan"), "c": 1}}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": {"c": 1}}

    def test_na_in_nested_dict(self):
        d = {"a": {"b": _NA, "c": 1}}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": {"c": 1}}

    def test_nested_dict_removed_when_empty_after_sweep(self):
        d = {"a": {"b": float("nan")}}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert "a" not in result

    def test_nan_in_list_of_dicts(self):
        d = {"a": [{"x": float("nan"), "y": 1}, {"x": _NA, "y": 2}]}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"a": [{"y": 1}, {"y": 2}]}


# ---------------------------------------------------------------------------
# dict_sweep — default vals behaviour unchanged
# ---------------------------------------------------------------------------


class TestDictSweepDefaultBehavior:
    def test_normal_values_kept(self):
        d = {"a": 1, "b": "hello", "c": [1, 2], "d": {"nested": True}}
        result = dict_sweep(d)
        assert result == {"a": 1, "b": "hello", "c": [1, 2], "d": {"nested": True}}

    def test_mixed_nan_and_default_vals_with_optin(self):
        d = {"a": float("nan"), "b": ".", "c": _NA, "d": "keep", "e": ""}
        result = dict_sweep(d, vals=_VALS_WITH_NAN)
        assert result == {"d": "keep"}

    def test_string_vals_supported(self):
        d = {"a": "NA", "b": "keep", "c": ["NA", "keep"]}
        result = dict_sweep(d, vals="NA")
        assert result == {"b": "keep", "c": ["keep"]}


# ---------------------------------------------------------------------------
# Tests using real pandas and numpy types, when the optional deps are installed.
# ---------------------------------------------------------------------------


class TestValToDeleteRealTypes:
    def test_numpy_nan_not_deleted_by_default(self, numpy):
        assert _val_to_delete(numpy.nan, _DEFAULT_VALS) is False

    def test_pandas_na_not_deleted_by_default(self, pandas):
        assert _val_to_delete(pandas.NA, _DEFAULT_VALS) is False

    def test_pandas_nat_not_deleted_by_default(self, pandas):
        assert _val_to_delete(pandas.NaT, _DEFAULT_VALS) is False

    def test_numpy_nan_deleted_when_in_vals(self, numpy):
        vals_with_nan = _DEFAULT_VALS + [float("nan")]
        assert _val_to_delete(numpy.nan, vals_with_nan) is True

    def test_pandas_na_deleted_when_in_vals(self, pandas):
        vals_with_na = _DEFAULT_VALS + [pandas.NA]
        assert _val_to_delete(pandas.NA, vals_with_na) is True

    def test_pandas_nat_deleted_when_in_vals(self, pandas):
        vals_with_nat = _DEFAULT_VALS + [pandas.NaT]
        assert _val_to_delete(pandas.NaT, vals_with_nat) is True

    def test_pandas_na_and_nat_are_not_interchangeable(self, pandas):
        assert _val_to_delete(pandas.NA, _DEFAULT_VALS + [pandas.NaT]) is False
        assert _val_to_delete(pandas.NaT, _DEFAULT_VALS + [pandas.NA]) is False

    def test_regular_match_after_pandas_na(self, pandas):
        assert _val_to_delete("", [pandas.NA, ""]) is True

    def test_numpy_float32_nan_deleted_when_in_vals(self, numpy):
        vals_with_nan = _DEFAULT_VALS + [float("nan")]
        assert _val_to_delete(numpy.float32("nan"), vals_with_nan) is True

    def test_numpy_float64_nan_deleted_when_in_vals(self, numpy):
        vals_with_nan = _DEFAULT_VALS + [float("nan")]
        assert _val_to_delete(numpy.float64("nan"), vals_with_nan) is True


class TestDictSweepRealPandas:
    """dict_sweep with real pandas/numpy NaN types and opt-in removal."""

    def _vals_with(self, *extras):
        return _DEFAULT_VALS + list(extras)

    def test_pandas_na_top_level(self, pandas):
        d = {"a": 1, "b": pandas.NA}
        result = dict_sweep(d, vals=self._vals_with(pandas.NA))
        assert result == {"a": 1}

    def test_pandas_nat_top_level(self, pandas):
        d = {"a": 1, "b": pandas.NaT}
        result = dict_sweep(d, vals=self._vals_with(pandas.NaT))
        assert result == {"a": 1}

    def test_numpy_nan_top_level(self, numpy):
        d = {"a": 1, "b": numpy.nan}
        result = dict_sweep(d, vals=self._vals_with(float("nan")))
        assert result == {"a": 1}

    def test_pandas_na_in_list(self, pandas):
        d = {"a": [1, pandas.NA, 2]}
        result = dict_sweep(d, vals=self._vals_with(pandas.NA))
        assert result == {"a": [1, 2]}

    def test_pandas_nat_in_list(self, pandas):
        d = {"a": [1, pandas.NaT, 2]}
        result = dict_sweep(d, vals=self._vals_with(pandas.NaT))
        assert result == {"a": [1, 2]}

    def test_numpy_nan_in_list(self, numpy):
        d = {"a": [1, numpy.nan, 2]}
        result = dict_sweep(d, vals=self._vals_with(float("nan")))
        assert result == {"a": [1, 2]}

    def test_pandas_na_in_nested_dict(self, pandas):
        d = {"a": {"b": pandas.NA, "c": 1}}
        result = dict_sweep(d, vals=self._vals_with(pandas.NA))
        assert result == {"a": {"c": 1}}

    def test_pandas_na_in_list_with_remove_invalid_list(self, pandas):
        d = {"a": [pandas.NA, pandas.NaT, "valid"]}
        result = dict_sweep(d, vals=self._vals_with(pandas.NA, pandas.NaT), remove_invalid_list=True)
        assert result == {"a": ["valid"]}

    def test_all_real_nan_types_removed(self, pandas, numpy):
        d = {"a": numpy.nan, "b": pandas.NA, "c": pandas.NaT, "d": "keep"}
        result = dict_sweep(d, vals=self._vals_with(float("nan"), pandas.NA, pandas.NaT))
        assert result == {"d": "keep"}

    def test_real_nan_kept_by_default(self, pandas, numpy):
        """Without opting in, real NaN types are preserved."""
        d = {"a": numpy.nan, "b": pandas.NA, "c": pandas.NaT, "d": "keep"}
        result = dict_sweep(d)
        assert "a" in result
        assert "b" in result
        assert "c" in result
        assert result["d"] == "keep"
