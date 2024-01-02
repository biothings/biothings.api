"""
Tests for exercising the functionality and structure of our
plugin design
"""
from pathlib import Path
import sys
from types import SimpleNamespace
import logging

import pytest

from biothings.hub.dataload.dumper import DumperManager
from biothings.hub.dataload.uploader import UploaderManager
from biothings.hub.dataplugin.assistant import AssistantManager
from biothings.hub.dataplugin.assistant import LocalAssistant
from biothings.hub.dataplugin.manager import DataPluginManager
from biothings.utils.hub_db import get_data_plugin


logger = logging.getLogger(__name__)

plugin_designs = ["single_uploader_plugin", "multiple_uploader_plugin"]


@pytest.mark.parametrize("plugin", plugin_designs, indirect=True)
def test_plugin_loading(plugin):
    """
    Test the plugin loading capability across different plugin implementations
    """

    """
    Because we moved the plugin contents to the /tmp/ directory to avoid
    writing over the data stored in the repository, we need to ensure we
    add the plugin to the python system path for when we attempt to load the
    plugin via 

    p_loader = assistant_instance.loader
    p_loader.load_plugin()

    This is so when we attempt to import the modules via importlib
    (version, parser, etc ...) we can properly find the modules we've moved
    off the python system path
    """
    sys.path.append(str(plugin))
    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)

    plugin_name = plugin.name
    assistant_url = f"local://{plugin_name}"

    assistant_instance = LocalAssistant(assistant_url)

    logger.info(
        (f"Plugin Assistant Plugin Name: {assistant_instance.plugin_name} | " f"Plugin Path: {plugin.as_posix()}")
    )

    dp = get_data_plugin()
    dp.remove({"_id": assistant_instance.plugin_name})
    """
    The download folder must be the exact same directory as the plugin directory
    due to the search process when attempting to load the plugin

    When the LocalAssistant object attempts to load the plugin when we access the "loader"
    property instance (see below) it will search in the download folder directory. If this is not
    specified to the plugin directory where we define the manifest location then the search will
    fail and we will be unable to load the plugin
    @property
    620     def loader(self):
    621         \"""
    622         Return loader object able to interpret plugin's folder content
    623         \"""
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
    biothings/hub/dataplugin/assistant.py" 1175 lines --52%--
    """
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
    sys.path.append(str(plugin))
    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)

    plugin_name = plugin.name
    assistant_url = f"local://{plugin_name}"

    assistant_instance = LocalAssistant(assistant_url)

    dp = get_data_plugin()
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
    _dumper = dumper_class()
    _dumper.prepare()
    current_plugin.dumper = _dumper


@pytest.mark.parametrize("plugin", plugin_designs, indirect=True)
def test_plugin_upload(plugin):
    sys.path.append(str(plugin))
    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)

    plugin_name = plugin.name
    assistant_url = f"local://{plugin_name}"

    assistant_instance = LocalAssistant(assistant_url)

    dp = get_data_plugin()
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
