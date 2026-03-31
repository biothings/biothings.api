"""
Tests for dict_sweep and _is_nan in biothings.utils.dataload,
specifically the handling of NaN-like values.
"""

import math

import pytest

from biothings.utils.dataload import _is_nan, dict_sweep

# ---------------------------------------------------------------------------
# Fake pandas-like NA / NaT types for testing without importing pandas
# ---------------------------------------------------------------------------

class NAType:
    """Mimics pandas.NA (pandas.core.arrays.masked.NAType)."""
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


# ---------------------------------------------------------------------------
# _is_nan helper tests
# ---------------------------------------------------------------------------

class TestIsNan:
    def test_float_nan(self):
        assert _is_nan(float("nan")) is True

    def test_na_type(self):
        assert _is_nan(_NA) is True

    def test_nat_type(self):
        assert _is_nan(_NaT) is True

    def test_regular_values_are_not_nan(self):
        for val in [0, 1, -1, "", "hello", None, [], {}, 0.0, 1.5, True, False]:
            assert _is_nan(val) is False, f"_is_nan should be False for {val!r}"


# ---------------------------------------------------------------------------
# dict_sweep tests — NaN-like values at top level
# ---------------------------------------------------------------------------

class TestDictSweepNanTopLevel:
    def test_float_nan_removed(self):
        d = {"a": 1, "b": float("nan")}
        result = dict_sweep(d)
        assert result == {"a": 1}

    def test_pandas_na_removed(self):
        d = {"a": 1, "b": _NA}
        result = dict_sweep(d)
        assert result == {"a": 1}

    def test_pandas_nat_removed(self):
        d = {"a": 1, "b": _NaT}
        result = dict_sweep(d)
        assert result == {"a": 1}

    def test_multiple_nan_types_removed(self):
        d = {"a": float("nan"), "b": _NA, "c": _NaT, "d": "keep"}
        result = dict_sweep(d)
        assert result == {"d": "keep"}


# ---------------------------------------------------------------------------
# dict_sweep tests — NaN-like values inside lists
# ---------------------------------------------------------------------------

class TestDictSweepNanInList:
    def test_nan_removed_from_list(self):
        d = {"a": [1, float("nan"), 2]}
        result = dict_sweep(d)
        assert result == {"a": [1, 2]}

    def test_na_removed_from_list(self):
        d = {"a": [1, _NA, 2]}
        result = dict_sweep(d)
        assert result == {"a": [1, 2]}

    def test_nat_removed_from_list(self):
        d = {"a": [1, _NaT, 2]}
        result = dict_sweep(d)
        assert result == {"a": [1, 2]}

    def test_list_becomes_empty_after_nan_removal(self):
        d = {"a": [float("nan")]}
        result = dict_sweep(d)
        assert "a" not in result

    def test_nan_in_list_with_remove_invalid_list(self):
        d = {"a": [float("nan"), _NA, "valid"]}
        result = dict_sweep(d, remove_invalid_list=True)
        assert result == {"a": ["valid"]}

    def test_all_nan_list_removed_with_remove_invalid_list(self):
        d = {"a": [float("nan"), _NA, _NaT]}
        result = dict_sweep(d, remove_invalid_list=True)
        assert "a" not in result


# ---------------------------------------------------------------------------
# dict_sweep tests — NaN-like values in nested dicts
# ---------------------------------------------------------------------------

class TestDictSweepNanNested:
    def test_nan_in_nested_dict(self):
        d = {"a": {"b": float("nan"), "c": 1}}
        result = dict_sweep(d)
        assert result == {"a": {"c": 1}}

    def test_na_in_nested_dict(self):
        d = {"a": {"b": _NA, "c": 1}}
        result = dict_sweep(d)
        assert result == {"a": {"c": 1}}

    def test_nested_dict_removed_when_empty_after_sweep(self):
        d = {"a": {"b": float("nan")}}
        result = dict_sweep(d)
        assert "a" not in result

    def test_nan_in_list_of_dicts(self):
        d = {"a": [{"x": float("nan"), "y": 1}, {"x": _NA, "y": 2}]}
        result = dict_sweep(d)
        assert result == {"a": [{"y": 1}, {"y": 2}]}


# ---------------------------------------------------------------------------
# dict_sweep tests — default vals still work
# ---------------------------------------------------------------------------

class TestDictSweepDefaultBehavior:
    def test_default_vals_still_removed(self):
        d = {"a": ".", "b": "-", "c": "", "d": "keep"}
        result = dict_sweep(d)
        assert result == {"d": "keep"}

    def test_normal_values_kept(self):
        d = {"a": 1, "b": "hello", "c": [1, 2], "d": {"nested": True}}
        result = dict_sweep(d)
        assert result == {"a": 1, "b": "hello", "c": [1, 2], "d": {"nested": True}}

    def test_mixed_nan_and_default_vals(self):
        d = {"a": float("nan"), "b": ".", "c": _NA, "d": "keep", "e": ""}
        result = dict_sweep(d)
        assert result == {"d": "keep"}


# ---------------------------------------------------------------------------
# Tests using real pandas and numpy types
# ---------------------------------------------------------------------------

pandas = pytest.importorskip("pandas")
numpy = pytest.importorskip("numpy")


class TestIsNanRealTypes:
    def test_numpy_nan(self):
        assert _is_nan(numpy.nan) is True

    def test_pandas_na(self):
        assert _is_nan(pandas.NA) is True

    def test_pandas_nat(self):
        assert _is_nan(pandas.NaT) is True

    def test_numpy_float_nan(self):
        assert _is_nan(numpy.float64("nan")) is True


class TestDictSweepRealPandasNA:
    def test_pandas_na_top_level(self):
        d = {"a": 1, "b": pandas.NA}
        result = dict_sweep(d)
        assert result == {"a": 1}

    def test_pandas_nat_top_level(self):
        d = {"a": 1, "b": pandas.NaT}
        result = dict_sweep(d)
        assert result == {"a": 1}

    def test_numpy_nan_top_level(self):
        d = {"a": 1, "b": numpy.nan}
        result = dict_sweep(d)
        assert result == {"a": 1}

    def test_pandas_na_in_list(self):
        d = {"a": [1, pandas.NA, 2]}
        result = dict_sweep(d)
        assert result == {"a": [1, 2]}

    def test_pandas_nat_in_list(self):
        d = {"a": [1, pandas.NaT, 2]}
        result = dict_sweep(d)
        assert result == {"a": [1, 2]}

    def test_numpy_nan_in_list(self):
        d = {"a": [1, numpy.nan, 2]}
        result = dict_sweep(d)
        assert result == {"a": [1, 2]}

    def test_pandas_na_in_nested_dict(self):
        d = {"a": {"b": pandas.NA, "c": 1}}
        result = dict_sweep(d)
        assert result == {"a": {"c": 1}}

    def test_pandas_na_in_list_with_remove_invalid_list(self):
        d = {"a": [pandas.NA, pandas.NaT, "valid"]}
        result = dict_sweep(d, remove_invalid_list=True)
        assert result == {"a": ["valid"]}

    def test_all_real_nan_types_removed(self):
        d = {"a": numpy.nan, "b": pandas.NA, "c": pandas.NaT, "d": "keep"}
        result = dict_sweep(d)
        assert result == {"d": "keep"}

    def test_mixed_real_and_default_vals(self):
        d = {"a": pandas.NA, "b": ".", "c": numpy.nan, "d": "keep", "e": pandas.NaT}
        result = dict_sweep(d)
        assert result == {"d": "keep"}
