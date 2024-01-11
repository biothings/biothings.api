"""
Tests for exercising the functionality and structure of our
plugin design
"""
from pathlib import Path
from types import SimpleNamespace
import logging

import pytest

from biothings import config
from biothings.hub.dataload.dumper import DumperManager
from biothings.hub.dataload.uploader import UploaderManager
from biothings.hub.dataplugin.assistant import AdvancedPluginLoader, LocalAssistant, ManifestBasedPluginLoader, AssistantManager, GithubAssistant
from biothings.hub.dataplugin.manager import DataPluginManager
from biothings.utils import hub_db
from biothings.utils.common import get_loop
from biothings.utils.manager import JobManager
from biothings.utils.workers import upload_worker


logger = logging.getLogger(__name__)

def test_assistant_manager():
    """
    Tests the construction of an AssistantManager instance and the API
    provided by the object
    <biothings.hub.dataplugin.assistant AssistantManager
    Ensures that we can construct and leverage the API provided
    by the <biothings.hub.dataplugin.assistant LocalAssistant>
    """
    remote_plugin_repository = "https://github.com/biothings/tutorials.git"

    job_manager = JobManager(loop=get_loop())
    data_plugin_manager = DataPluginManager(job_manager=None)
    dumper_manager = DumperManager(job_manager=None)
    uploader_manager = UploaderManager(job_manager=None)

    assistant_manager = AssistantManager(
        job_manager=job_manager,
        data_plugin_manager=data_plugin_manager,
        dumper_manager=dumper_manager,
        uploader_manager=uploader_manager,
        keylookup=None,
        default_export_folder="hub/dataload/sources",
    )

    assert assistant_manager.dumper_manager == dumper_manager
    assert assistant_manager.uploader_manager == uploader_manager
    assert assistant_manager.job_manager == job_manager
    assert assistant_manager.data_plugin_manager == data_plugin_manager

    assert assistant_manager.default_export_folder == "hub/dataload/sources"

    assert not assistant_manager.register
    assistant_manager.configure()
    assert isinstance(assistant_manager.register["github"], type)
    assert assistant_manager.register["github"] == GithubAssistant
    assert isinstance(assistant_manager.register["local"], type)
    assert assistant_manager.register["local"] == LocalAssistant
    generated_assistant = assistant_manager.submit(url=remote_plugin_repository)
    assert isinstance(generated_assistant, GithubAssistant)
