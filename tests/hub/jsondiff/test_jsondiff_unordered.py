import copy
import itertools

import pytest

import biothings.utils.jsondiff as jsondiff


@pytest.fixture
def unordered_mode(monkeypatch):
    monkeypatch.setattr(jsondiff, "UNORDERED_LIST", True)
    monkeypatch.setattr(jsondiff, "USE_LIST_OPS", False)


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


def test_unordered_list_preserves_nested_container_semantics(unordered_mode):
    source = [{"a": 1, "b": 2}, {"tags": [1, 2]}]
    reordered_dict = [{"tags": [1, 2]}, {"b": 2, "a": 1}]
    reordered_nested_list = [{"tags": [2, 1]}, {"b": 2, "a": 1}]

    assert jsondiff.make(source, reordered_dict) == []
    assert jsondiff.make(source, reordered_nested_list) == [
        {"op": "replace", "path": "", "value": reordered_nested_list}
    ]


def test_unordered_list_preserves_legacy_duplicate_membership(unordered_mode):
    source = [1, 1, 2]
    destination = [1, 2, 2]

    # The optimized path intentionally preserves the existing membership
    # semantics. Multiplicity correctness should be addressed separately.
    assert source != destination
    assert jsondiff.make({"values": source}, {"values": destination}) == []


def test_unordered_list_matches_legacy_membership_rule(unordered_mode):
    atoms = (0, "x", {"k": 0}, ["nested"])
    lists = [list(items) for size in range(4) for items in itertools.product(atoms, repeat=size)]

    for source, destination in itertools.product(lists, repeat=2):
        equivalent = len(source) == len(destination) and all(item in destination for item in source)
        expected = [] if equivalent else [{"op": "replace", "path": "", "value": destination}]

        assert jsondiff.make(source, destination) == expected, (source, destination)


def test_unordered_list_preserves_nan_membership(unordered_mode):
    shared_nan = float("nan")
    source = [float("nan"), 1]
    destination = [1, float("nan")]

    assert jsondiff.make([shared_nan, 1], [1, shared_nan]) == []
    assert jsondiff.make(source, destination) == [{"op": "replace", "path": "", "value": destination}]


def test_unordered_list_falls_back_without_hashing_custom_values(unordered_mode):
    class EqualButUnhashable:
        def __init__(self, value):
            self.value = value

        def __eq__(self, other):
            return isinstance(other, EqualButUnhashable) and self.value == other.value

        def __hash__(self):
            raise AssertionError("the unordered-list index must not hash custom values")

    source = [EqualButUnhashable(1), "x"]
    destination = ["x", EqualButUnhashable(1)]

    assert jsondiff.make(source, destination) == []


def test_unordered_list_falls_back_for_list_subclasses(unordered_mode):
    class NeverContains(list):
        def __contains__(self, item):
            return False

    source = [1, 2]
    destination = NeverContains([2, 1])

    assert jsondiff.make(source, destination) == [{"op": "replace", "path": "", "value": destination}]


def test_unordered_list_falls_back_for_cycles(unordered_mode):
    recursive = []
    recursive.append(recursive)

    assert jsondiff.make([recursive, "x"], ["x", recursive]) == []


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
