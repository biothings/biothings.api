import copy
import itertools

import pytest

import biothings.utils.jsondiff as jsondiff


@pytest.fixture
def unordered_mode(monkeypatch):
    monkeypatch.setattr(jsondiff, "UNORDERED_LIST", True)
    monkeypatch.setattr(jsondiff, "USE_LIST_OPS", False)


def _replace_root(value):
    return [{"op": "replace", "path": "", "value": value}]


def _assert_symmetric_multiset_result(source, destination, equivalent):
    expected_forward = [] if equivalent else _replace_root(destination)
    expected_reverse = [] if equivalent else _replace_root(source)

    assert jsondiff.make(source, destination) == expected_forward
    assert jsondiff.make(destination, source) == expected_reverse


def _has_equality_preserving_permutation(source, destination):
    if len(source) != len(destination):
        return False
    return any(
        all(left is right or left == right for left, right in zip(source, permutation))
        for permutation in itertools.permutations(destination)
    )


@pytest.mark.parametrize(
    "source,destination",
    [
        pytest.param([0, "x", None, False], [False, None, "x", 0], id="hashable"),
        pytest.param(
            [0, {"id": 1}, ["nested", {"x": 1}], "x"],
            ["x", ["nested", {"x": 1}], {"id": 1}, 0],
            id="mixed-unhashable",
        ),
        pytest.param([1, "x"], ["x", True], id="python-equality"),
    ],
)
def test_unordered_list_equivalent_values_have_no_patch(unordered_mode, source, destination):
    source_before = copy.deepcopy(source)
    destination_before = copy.deepcopy(destination)

    assert jsondiff.make({"values": source}, {"values": destination}) == []
    assert jsondiff.make({"values": destination}, {"values": source}) == []
    assert source == source_before
    assert destination == destination_before


@pytest.mark.parametrize(
    "source,destination",
    [
        pytest.param([1, "x", None], [1, "x", "changed"], id="hashable"),
        pytest.param([{"id": 1}, ["a"]], [{"id": 1}, ["b"]], id="unhashable"),
        pytest.param([1, 2], [2, 1, 3], id="different-length"),
    ],
)
def test_unordered_list_difference_replaces_whole_list(unordered_mode, source, destination):
    assert jsondiff.make({"values": source}, {"values": destination}) == [
        {"op": "replace", "path": "/values", "value": destination}
    ]
    assert jsondiff.make({"values": destination}, {"values": source}) == [
        {"op": "replace", "path": "/values", "value": source}
    ]


def test_unordered_list_preserves_nested_container_semantics(unordered_mode):
    source = [{"a": 1, "b": 2}, {"tags": [1, 2]}]
    reordered_dict = [{"tags": [1, 2]}, {"b": 2, "a": 1}]
    reordered_nested_list = [{"tags": [2, 1]}, {"b": 2, "a": 1}]

    assert jsondiff.make(source, reordered_dict) == []
    assert jsondiff.make(source, reordered_nested_list) == [
        {"op": "replace", "path": "", "value": reordered_nested_list}
    ]


@pytest.mark.parametrize(
    "source,destination,equivalent",
    [
        pytest.param([1, 1, 2], [2, 1, 1], True, id="scalar-reordered-same-counts"),
        pytest.param([1, 1, 2], [1, 2, 2], False, id="scalar-different-counts"),
        pytest.param(
            [{"id": 1, "tags": ["a"]}, {"id": 1, "tags": ["a"]}, {"id": 2}],
            [{"id": 2}, {"tags": ["a"], "id": 1}, {"tags": ["a"], "id": 1}],
            True,
            id="nested-reordered-same-counts",
        ),
        pytest.param(
            [{"id": 1, "tags": ["a"]}, {"id": 1, "tags": ["a"]}, {"id": 2}],
            [{"id": 1, "tags": ["a"]}, {"id": 2}, {"id": 2}],
            False,
            id="nested-different-counts",
        ),
        pytest.param([1, False, "x"], ["x", True, 0], True, id="python-numeric-equality"),
    ],
)
def test_unordered_list_compares_value_multiplicity(unordered_mode, source, destination, equivalent):
    _assert_symmetric_multiset_result(source, destination, equivalent)


def test_unordered_list_matches_multiset_rule(unordered_mode):
    atoms = (0, "x", {"k": 0}, ["nested"])
    list_specs = [list(items) for size in range(4) for items in itertools.product(atoms, repeat=size)]

    for source_spec, destination_spec in itertools.product(list_specs, repeat=2):
        source = [copy.deepcopy(item) for item in source_spec]
        destination = [copy.deepcopy(item) for item in destination_spec]
        equivalent = _has_equality_preserving_permutation(source, destination)
        expected = [] if equivalent else _replace_root(destination)

        assert jsondiff.make(source, destination) == expected, (source, destination)


def test_unordered_list_preserves_nan_identity_semantics(unordered_mode):
    first_nan = float("nan")
    second_nan = float("nan")

    _assert_symmetric_multiset_result(
        [first_nan, second_nan, 1],
        [1, second_nan, first_nan],
        equivalent=True,
    )

    source = [float("nan"), 1]
    destination = [1, float("nan")]
    _assert_symmetric_multiset_result(source, destination, equivalent=False)

    _assert_symmetric_multiset_result(
        [first_nan, first_nan, 1],
        [1, first_nan, second_nan],
        equivalent=False,
    )


def test_unordered_list_fallback_compares_multiplicity_without_hashing(unordered_mode):
    class EqualButUnindexable:
        def __init__(self, value):
            self.value = value

        def __eq__(self, other):
            return isinstance(other, EqualButUnindexable) and self.value == other.value

        def __hash__(self):
            raise AssertionError("fallback values must not be hashed")

    source = [EqualButUnindexable(1), EqualButUnindexable(1), EqualButUnindexable(2)]
    equivalent = [EqualButUnindexable(2), EqualButUnindexable(1), EqualButUnindexable(1)]
    different = [EqualButUnindexable(1), EqualButUnindexable(2), EqualButUnindexable(2)]

    _assert_symmetric_multiset_result(source, equivalent, equivalent=True)
    _assert_symmetric_multiset_result(source, different, equivalent=False)


def test_unordered_list_subclasses_use_element_multiset_semantics(unordered_mode):
    class NeverContains(list):
        def __contains__(self, item):
            raise AssertionError("multiset comparison must not use directional containment")

    source = [1, 1, 2]
    equivalent = NeverContains([2, 1, 1])
    different = NeverContains([1, 2, 2])

    _assert_symmetric_multiset_result(source, equivalent, equivalent=True)
    _assert_symmetric_multiset_result(source, different, equivalent=False)


def test_unordered_list_falls_back_for_cycles(unordered_mode):
    recursive = []
    recursive.append(recursive)
    source = [recursive, recursive, "x"]
    destination = ["x", recursive, recursive]

    _assert_symmetric_multiset_result(source, destination, equivalent=True)


def test_ordered_list_still_replaces_reordered_values(monkeypatch):
    monkeypatch.setattr(jsondiff, "UNORDERED_LIST", False)
    monkeypatch.setattr(jsondiff, "USE_LIST_OPS", False)

    assert jsondiff.make({"values": [1, 2]}, {"values": [2, 1]}) == [
        {"op": "replace", "path": "/values", "value": [2, 1]}
    ]


def test_list_operations_take_precedence_over_unordered_mode(monkeypatch):
    monkeypatch.setattr(jsondiff, "UNORDERED_LIST", True)
    monkeypatch.setattr(jsondiff, "USE_LIST_OPS", True)

    assert jsondiff.make({"values": [1, 2]}, {"values": [2, 1]}) == [
        {"op": "move", "path": "/values/1", "from": "/values/0"}
    ]
    assert jsondiff.make({"values": [1, 1, 2]}, {"values": [1, 2, 2]}) == [
        {"op": "replace", "path": "/values/1", "value": 2}
    ]
