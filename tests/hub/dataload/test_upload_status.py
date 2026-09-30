"""
Tests for the upload status registered by uploaders
"""

from biothings.hub.dataload.uploader import BaseSourceUploader


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
