"""
Unregistering a data plugin by its URL, as the Hub API shows it: credentials in it hidden (eg. a token
to clone a private repository), which BioThings Studio sends back
"""

from types import SimpleNamespace

from biothings.hub.dataplugin import manager
from biothings.hub.dataplugin.manager import AssistantManager


class Plugins:
    """The data_plugin collection of the hub db, in memory"""

    def __init__(self, *docs):
        self.docs = list(docs)

    def find(self, query=None):
        return list(self.docs)

    def find_one(self, query):
        return next((doc for doc in self.docs if doc["plugin"]["url"] == query["plugin.url"]), None)

    def remove(self, query):
        self.docs = [doc for doc in self.docs if doc["_id"] != query["_id"]]


def test_unregister_url_with_hidden_credentials(monkeypatch):
    url = "https://user:ghp_token@github.com/org/plugin.git"
    plugins = Plugins({"_id": "plugin", "plugin": {"url": url}, "download": {}})
    monkeypatch.setattr(manager, "get_data_plugin", lambda: plugins)
    submitted = []
    fake = SimpleNamespace(
        logger=None,
        submit=lambda plugin_url: submitted.append(plugin_url) or SimpleNamespace(plugin_name="plugin"),
        data_plugin_manager=SimpleNamespace(register={"plugin": object()}),
        dumper_manager=SimpleNamespace(register={}),
        uploader_manager=SimpleNamespace(register={}),
    )
    AssistantManager.unregister_url(fake, url="https://********@github.com/org/plugin.git")
    assert plugins.docs == []
    assert submitted == [url]  # the actual URL
