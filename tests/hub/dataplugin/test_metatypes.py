"""
Evaluate our metatype classes to ensure we can properly build custom dumper / uploader
instances from our manifest specification
"""

from pathlib import Path
import json

import pytest

from biothings.hub.dataplugin.metatypes import manifest_dumper_factory


@pytest.mark.parametrize("plugin", ["basic_dumper_plugin"], indirect=True)
def test_building_metadumper(plugin, tmpdir: Path):
    plugin_name = "mock"
    plugin_directory = Path(tmpdir)
    source_root_directory = Path(tmpdir)

    with open(plugin.joinpath("manifest.json"), "r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    metadumper_class_type = manifest_dumper_factory(
        plugin_name=plugin_name,
        plugin_directory=plugin_directory,
        source_root_directory=source_root_directory,
        manifest_mapping=manifest,
    )
    assert isinstance(metadumper_class_type, type)
    metadumper_class = metadumper_class_type()
    metadumper_class.set_release()
    assert metadumper_class.release
