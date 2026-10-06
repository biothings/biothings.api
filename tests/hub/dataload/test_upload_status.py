"""
Tests for the upload status registered by uploaders
"""

import pytest

from biothings.hub.dataload.uploader import BaseSourceUploader, ResourceNotReady


class FakeSrcDump:
    """In-memory src_dump collection, recording updates"""

    def __init__(self):
        self.updates = []

    def update_one(self, query, update):
        self.updates.append((query, update))


class ExampleUploader(BaseSourceUploader):
    name = "example"


def test_uploading_status_keeps_previous_count():
    """While uploading, the collection still has its documents: keep the count of the last upload"""
    uploader = ExampleUploader(db_conn_info="")
    uploader._state["src_dump"] = FakeSrcDump()
    uploader.src_doc = {"upload": {"jobs": {"example": {"status": "success", "count": 36015, "started_at": "then"}}}}
    uploader.register_status("uploading")
    ((query, update),) = uploader._state["src_dump"].updates
    info = update["$set"]["upload.jobs.example"]
    assert (info["status"], info["count"], info["last_success"]) == ("uploading", 36015, "then")


def test_uploading_status_without_previous_upload():
    uploader = ExampleUploader(db_conn_info="")
    uploader._state["src_dump"] = FakeSrcDump()
    uploader.src_doc = {}
    uploader.register_status("uploading")
    ((query, update),) = uploader._state["src_dump"].updates
    assert "count" not in update["$set"]["upload.jobs.example"]


@pytest.mark.asyncio
async def test_not_ready_keeps_the_last_upload(monkeypatch):
    """
    A source which can't be uploaded (eg. its last download failed) keeps the status of its last
    upload, and its data: merges can still use it (eg. after upload_all)
    """
    src_dump = FakeSrcDump()

    def prepare(self):
        """Read the source's information (from the hub's database)"""
        self._state["src_dump"] = src_dump
        self.src_doc = {
            "download": {"status": "failed", "data_folder": "/data/example/2026-10-01"},
            "upload": {"jobs": {"example": {"status": "success", "count": 36015}}},
        }
        self.prepared = True

    monkeypatch.setattr(ExampleUploader, "prepare", prepare)
    uploader = ExampleUploader(db_conn_info="")
    with pytest.raises(ResourceNotReady, match="No successful download found"):  # its information was read first
        await uploader.load()
    assert src_dump.updates == []  # no "failed" status registered
