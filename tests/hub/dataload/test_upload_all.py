import asyncio

import pytest

from biothings.hub.dataload.uploader import UploaderManager


async def uploaded(src):
    await asyncio.sleep(0.01)
    return "%s uploaded" % src


async def failed(src):
    raise ValueError("no data for %s" % src)


@pytest.mark.asyncio
async def test_upload_all_waits_for_every_upload(monkeypatch):
    manager = UploaderManager(job_manager=None)
    manager.register = {"umls": [], "mondo": []}
    uploads = {"umls": failed, "mondo": uploaded}
    monkeypatch.setattr(manager, "upload_src", lambda src, **kwargs: [asyncio.ensure_future(uploads[src](src))])
    results = await manager.upload_all()  # one failing doesn't fail the others
    assert [str(result) for result in results] == ["no data for umls", "mondo uploaded"]
    with pytest.raises(ValueError, match="no data for umls"):
        await manager.upload_all(raise_on_error=True)
