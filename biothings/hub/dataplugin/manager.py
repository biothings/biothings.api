import biothings.hub.dataload.dumper as dumper
from biothings.utils.hub_db import get_data_plugin


class GitDataPlugin(dumper.GitDumper):
    # override to point to "data_plugin" collection instead of src_dump
    # so we don't mix data sources and plugins
    def prepare_src_dump(self):
        self.src_dump = get_data_plugin()
        self.src_doc = self.src_dump.find_one({"_id": self.src_name}) or {}


class ManualDataPlugin(dumper.ManualDumper):
    # override to point to "data_plugin" collection instead of src_dump
    # so we don't mix data sources and plugins
    def prepare_src_dump(self):
        self.src_dump = get_data_plugin()
        self.src_doc = self.src_dump.find_one({"_id": self.src_name}) or {}

    async def dump(self, *args, **kwargs):
        await super(ManualDataPlugin, self).dump(
            path="",  # it's the version is original method implemention
            # but no version here available
            release="",
            *args,
            **kwargs,
        )


class DataPluginManager(dumper.DumperManager):
    def load(self, plugin_name, *args, **kwargs):
        return super(DataPluginManager, self).dump_src(plugin_name, *args, **kwargs)
