from types import SimpleNamespace

import pytest

from biothings.hub.dataload.source import SourceManager


class FakeSrcDump:
    def __init__(self, doc):
        self.doc = doc

    def find_one(self, query):
        return self.doc if query["_id"] == self.doc["_id"] else None

    def save(self, doc):
        self.doc = doc


def test_reset_source_without_sub_sources():
    src_dump = FakeSrcDump(
        {"_id": "mygene", "download": {"status": "success"}, "upload": {"jobs": {"mygene": {"status": "failed"}}}}
    )
    manager = SimpleNamespace(src_dump=src_dump)
    SourceManager.reset(manager, "mygene")  # its upload, the sub-source being the source itself
    assert src_dump.doc["upload"] == {"jobs": {}}
    SourceManager.reset(manager, "mygene", key="download")
    assert "download" not in src_dump.doc
    with pytest.raises(ValueError, match="not found in document"):
        SourceManager.reset(manager, "mygene", subkey="other")
