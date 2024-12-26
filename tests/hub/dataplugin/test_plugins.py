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
from biothings.hub.dataplugin.assistant import AdvancedPluginLoader, LocalAssistant, ManifestBasedPluginLoader
from biothings.hub.dataplugin.manager import DataPluginManager
from biothings.utils import hub_db
from biothings.utils.workers import upload_worker


logger = logging.getLogger(__name__)

plugin_designs = ["single_uploader_plugin", "multiple_uploader_plugin"]


def test_local_assistant_construction():
    """
    Ensures that we can construct and leverage the API provided
    by the <biothings.hub.dataplugin.assistant LocalAssistant>
    """
    mock_plugin = Path("/mock/plugins/nuclear-plugin")
    plugin_name = mock_plugin.name

    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)

    assistant_url = f"local://{plugin_name}"
    assistant_instance = LocalAssistant(assistant_url)

    assert isinstance(assistant_instance.dumper_manager, DumperManager)
    assert assistant_instance.dumper_manager.register == {}
    assert assistant_instance.dumper_manager.get_source_ids() == []

    assert isinstance(assistant_instance.data_plugin_manager, DataPluginManager)

    assert isinstance(assistant_instance.uploader_manager, UploaderManager)
    assert assistant_instance.uploader_manager.register == {}
    assert assistant_instance.uploader_manager.get_source_ids() == []

    assert assistant_instance.plugin_name == plugin_name
    assert assistant_instance.plugin_type == "local"
    assert assistant_instance.url == assistant_url

    assert assistant_instance.loaders["manifest"] == ManifestBasedPluginLoader
    assert assistant_instance.loaders["advanced"] == AdvancedPluginLoader

    assert Path(assistant_instance.logfile).exists()


@pytest.mark.parametrize("plugin", plugin_designs, indirect=True)
def test_plugin_loading(plugin):
    """
    Test the plugin loading capability across different plugin implementations

    *** NOTE ***
    The download folder must be the exact same directory as the plugin directory
    due to the search process when attempting to load the plugin

    When the LocalAssistant object attempts to load the plugin when we access the "loader"
    property instance (see below) it will search in the download folder directory. If this is not
    specified to the plugin directory where we define the manifest location then the search will
    fail and we will be unable to load the plugin
    619     @property
    620     def loader(self):

    624         if not self._loader:
    625             # iterate over known loaders, the first one which can interpret plugin content is kept
    626             for klass in self.loaders.values():
    627                 # propagate managers
    628                 klass.dumper_manager = self.dumper_manager
    629                 klass.uploader_manager = self.uploader_manager
    630                 klass.data_plugin_manager = self.data_plugin_manager
    631                 klass.keylookup = self.keylookup
    632                 loader = klass(self.plugin_name)
    634                 if loader.can_load_plugin():
    biothings/hub/dataplugin/assistant.py
    """
    hub_db.setup(config)
    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)

    plugin_name = plugin.name
    assistant_url = f"local://{plugin_name}"

    assistant_instance = LocalAssistant(assistant_url)

    dp = hub_db.get_data_plugin()
    dp.remove({"_id": assistant_instance.plugin_name})
    plugin_entry = {
        "_id": assistant_instance.plugin_name,
        "plugin": {
            "url": assistant_url,
            "type": assistant_instance.plugin_type,
            "active": True,
        },
        "download": {"data_folder": str(Path(plugin))},
    }

    dp.insert_one(plugin_entry)

    p_loader = assistant_instance.loader
    p_loader.load_plugin()


@pytest.mark.parametrize("plugin", plugin_designs, indirect=True)
def test_plugin_dump(plugin):
    """
    Test the dumper capabilities associated with the plugin architectures
    """
    hub_db.setup(config)
    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)

    plugin_name = plugin.name
    assistant_url = f"local://{plugin_name}"

    assistant_instance = LocalAssistant(assistant_url)

    dp = hub_db.get_data_plugin()
    dp.remove({"_id": assistant_instance.plugin_name})
    plugin_entry = {
        "_id": assistant_instance.plugin_name,
        "plugin": {
            "url": assistant_url,
            "type": assistant_instance.plugin_type,
            "active": True,
        },
        "download": {"data_folder": str(Path(plugin))},
    }

    dp.insert_one(plugin_entry)

    p_loader = assistant_instance.loader
    p_loader.load_plugin()
    dumper_manager = p_loader.__class__.dumper_manager

    current_plugin = SimpleNamespace(
        plugin_name=plugin_name,
        data_plugin_dir=plugin,
        in_plugin_dir=plugin_name is None,
    )

    # Generate dumper instance
    dumper_class = dumper_manager[plugin_name][0]
    dumper_instance = dumper_class()
    current_plugin.dumper = dumper_instance
    current_plugin.dumper.prepare()

    current_plugin.dumper.create_todump_list(force=True)
    for item in current_plugin.dumper.to_dump:
        current_plugin.dumper.download(item["remote"], item["local"])

    current_plugin.dumper.steps = ["post"]
    current_plugin.dumper.post_dump()
    current_plugin.dumper.register_status("success")
    current_plugin.dumper.release_client()

    dp.remove({"_id": current_plugin.plugin_name})
    data_folder = current_plugin.dumper.new_data_folder


@pytest.mark.parametrize("plugin", plugin_designs, indirect=True)
def test_plugin_upload(plugin):
    hub_db.setup(config)
    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)

    plugin_name = plugin.name
    assistant_url = f"local://{plugin_name}"

    assistant_instance = LocalAssistant(assistant_url)

    dp = hub_db.get_data_plugin()
    dp.remove({"_id": assistant_instance.plugin_name})
    plugin_entry = {
        "_id": assistant_instance.plugin_name,
        "plugin": {
            "url": assistant_url,
            "type": assistant_instance.plugin_type,
            "active": True,
        },
        "download": {"data_folder": str(Path(plugin))},
    }

    dp.insert_one(plugin_entry)

    p_loader = assistant_instance.loader
    p_loader.load_plugin()

    uploader_manager = assistant_instance.__class__.uploader_manager
    current_plugin = SimpleNamespace(
        plugin_name=plugin_name,
        data_plugin_dir=plugin,
        in_plugin_dir=plugin_name is None,
    )

    # Generate uploader instance
    uploader_classes = uploader_manager[plugin_name]
    if not isinstance(uploader_classes, list):
        uploader_classes = [uploader_classes]
    current_plugin.uploader_classes = uploader_classes

    for uploader_cls in current_plugin.uploader_classes:
        uploader = uploader_cls.create(db_conn_info="")
        uploader.make_temp_collection()
        uploader.prepare()

        assert Path(uploader.data_folder).exists()

        upload_worker(
            uploader.fullname,
            uploader.__class__.storage_class,
            uploader.load_data,
            uploader.temp_collection_name,
            10000,
            1,
            uploader.data_folder,
            db=uploader.db,
        )
        uploader.switch_collection()
        uploader.keep_archive = 3  # keep 3 archived collections, that's probably good enough for CLI, default is 10
        uploader.clean_archived_collections()
