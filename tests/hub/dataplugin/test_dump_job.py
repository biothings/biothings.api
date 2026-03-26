"""
Integration test for evaluating the various pieces
required to execute a dump job within our hub
"""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from biothings import config
from biothings.hub.dataload.dumper import DumperManager
from biothings.utils.manager import JobManager
from biothings.hub.dataload.uploader import UploaderManager
from biothings.hub.dataplugin.assistant import LocalAssistant
from biothings.hub.dataplugin.manager import DataPluginManager
from biothings.utils import hub_db

plugin_designs = ["single_uploader_plugin", "multiple_uploader_plugin"]


@pytest.mark.asyncio
@pytest.mark.parametrize("plugin", plugin_designs, indirect=True)
async def test_job_dump_operation(plugin):
    hub_db.setup(config)

    # construct our manager instances
    job_manager = JobManager(
        loop=asyncio.get_running_loop(),
        process_queue=None,
        thread_queue=None,
        max_memory_usage=None,
        num_workers=None,
        num_threads=None,
        auto_recycle=True,
    )

    LocalAssistant.data_plugin_manager = DataPluginManager(job_manager=None)
    LocalAssistant.uploader_manager = UploaderManager(job_manager=None)
    LocalAssistant.dumper_manager = DumperManager(job_manager=job_manager, poll_schedule=None, datasource_path=None)

    plugin_name = plugin.name
    assistant_url = f"local://{plugin_name}"
    assistant_instance = LocalAssistant(assistant_url)

    data_plugin_entry = hub_db.get_data_plugin()
    data_plugin_entry.remove({"_id": assistant_instance.plugin_name})
    plugin_entry = {
        "_id": assistant_instance.plugin_name,
        "plugin": {
            "url": assistant_url,
            "type": assistant_instance.plugin_type,
            "active": True,
        },
        "download": {"data_folder": str(Path(plugin))},
    }

    data_plugin_entry.insert_one(plugin_entry)

    plugin_loader = assistant_instance.loader
    plugin_loader.load_plugin()

    current_plugin = SimpleNamespace(
        plugin_name=plugin_name,
        data_plugin_dir=plugin,
        in_plugin_dir=plugin_name is None,
    )

    await assistant_instance.dumper_manager.dump_all()
