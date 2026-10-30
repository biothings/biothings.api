from types import SimpleNamespace

import pytest

from biothings.hub.dataindex.snapshooter import SnapshotManager


def test_snapshot_in_unknown_environment():
    manager = SimpleNamespace(register={})  # no SNAPSHOT_CONFIG
    with pytest.raises(ValueError, match="Unknown snapshot environment 'local' \\(see SNAPSHOT_CONFIG\\)"):
        SnapshotManager.snapshot(manager, "local", "mygene_20240101_abcdefgh")
