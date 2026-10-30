import asyncio

import pytest

from biothings.hub.autoupdate import BiothingsUploader
from biothings.hub.dataload.manager import SourcesFailed
from biothings.hub.dataload.uploader import DummySourceUploader, ResourceNotReady, UploaderManager


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


@pytest.mark.asyncio
async def test_upload_all_skips_what_cant_be_uploaded(monkeypatch):
    """
    Dummy uploaders (their data is uploaded separately) and sources which can't be uploaded yet
    (eg. their last download failed) are skipped: they keep their last upload, so merges can use it
    """

    class ExternalUploader(DummySourceUploader):
        """Its data is uploaded by another process"""

    async def not_ready(src):
        raise ResourceNotReady("No successful download found for resource '%s'" % src)

    manager = UploaderManager(job_manager=None)
    manager.register = {"mondo": [], "hpo": [], "chembl": [ExternalUploader]}
    uploads = {"mondo": uploaded, "hpo": not_ready}
    monkeypatch.setattr(manager, "upload_src", lambda src, **kwargs: [asyncio.ensure_future(uploads[src](src))])
    task = manager.upload_all()
    assert await task == {
        "mondo": "mondo uploaded",
        "hpo": "skipped: No successful download found for resource 'hpo'",
        "chembl": "skipped: dummy uploader, its data is uploaded separately (upload chembl --release <release>)",
    }
    assert task.progress == [
        "upload mondo, hpo (skipped: chembl)",
        "hpo skipped (1/2): No successful download found for resource 'hpo'",
        "mondo done (2/2)",
    ]
