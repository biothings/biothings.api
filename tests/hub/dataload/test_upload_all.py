import asyncio

import pytest

from biothings.hub.autoupdate import BiothingsUploader
from biothings.hub.dataload.manager import SourcesFailed
from biothings.hub.dataload.uploader import UploaderManager


async def uploaded(src):
    await asyncio.sleep(0.01)
    return "%s uploaded" % src


async def failed(src):
    raise ValueError("no data for %s" % src)


@pytest.mark.asyncio
async def test_upload_all_waits_for_every_upload(monkeypatch):
    class ReleaseUploader(BiothingsUploader):
        """Installs data releases in the API's Elasticsearch index: not a data source to upload"""

    manager = UploaderManager(job_manager=None)
    manager.register = {"umls": [], "mondo": [], "mydisease-disease__es9": [ReleaseUploader]}
    uploads = {"umls": failed, "mondo": uploaded}
    monkeypatch.setattr(manager, "upload_src", lambda src, **kwargs: [asyncio.ensure_future(uploads[src](src))])
    with pytest.raises(SourcesFailed) as error:  # once all are done: one failing doesn't stop the others
        await manager.upload_all()
    assert str(error.value) == "upload failed for 1 of 2 sources: umls (ValueError: no data for umls). Done: mondo"
    assert error.value.summary == {"umls": "failed: ValueError: no data for umls", "mondo": "mondo uploaded"}
    with pytest.raises(ValueError, match="no data for umls"):
        await manager.upload_all(raise_on_error=True)
