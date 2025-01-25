import abc
import os
import urllib.parse

import requests

from biothings import config as btconfig
from biothings.hub.dataplugin.plugins import GitDataPlugin, ManualDataPlugin
from biothings.hub.dataplugin.loaders.loader import ManifestBasedPluginLoader, AdvancedPluginLoader
from biothings.utils.common import (
    get_plugin_name_from_local_manifest,
    get_plugin_name_from_remote_manifest,
    parse_folder_name_from_url,
)
from biothings.utils.hub_db import get_data_plugin
from biothings.utils.loggers import get_logger


class AssistantException(Exception):
    pass


class BaseAssistant(abc.ABC):
    plugin_type = None  # to be defined in subblass
    data_plugin_manager = None  # set by assistant manager
    dumper_manager = None  # set by assistant manager
    uploader_manager = None  # set by assistant manager
    keylookup = None  # set by assistant manager

    # known plugin loaders
    loaders = {
        "manifest": ManifestBasedPluginLoader,
        "advanced": AdvancedPluginLoader,
    }

    def __init__(self, url: str):
        self.url = url
        self._plugin_name = None
        self._src_folder = None
        self._loader = None
        self.logfile = None
        self.logger = None
        self.setup_log()

    def setup_log(self):
        """
        Setup and return a logger instance
        """
        self.logger, self.logfile = get_logger("assistant_%s" % self.__class__.plugin_type)

    def register_loader(self):
        dp = get_data_plugin()
        dp.update(
            {"_id": self.plugin_name},
            {"$set": {"plugin.loader": self.loader.loader_type}},
            upsert=True,
        )

    @property
    def loader(self):
        """
        Return loader object able to interpret plugin's folder content
        """
        if not self._loader:
            # iterate over known loaders, the first one which can interpret plugin content is kept
            for klass in self.loaders.values():
                # propagate managers
                klass.dumper_manager = self.dumper_manager
                klass.uploader_manager = self.uploader_manager
                klass.data_plugin_manager = self.data_plugin_manager
                klass.keylookup = self.keylookup
                loader = klass(self.plugin_name)
                if loader.can_load_plugin():
                    self._loader = loader
                    self.logger.debug(
                        'For plugin "%s", selecting loader class "%s"',
                        self.plugin_name,
                        self._loader.__class__.__name__,
                    )
                    self.register_loader()
                    break
                else:
                    self.logger.debug('Loader %s cannot load plugin "%s"', loader, self.plugin_name)
                    continue
        return self._loader

    def handle(self):
        """
        Access self.url and do whatever is necessary to bring code to life within the hub...
        (hint: that may involve creating a dumper on-the-fly and register that dumper to
        a manager...)
        """
        assert self.__class__.data_plugin_manager, "Please set data_plugin_manager attribute"
        klass = self.get_classdef()
        self.__class__.data_plugin_manager.register_classes([klass])

    @property
    @abc.abstractmethod
    def plugin_name(self):
        """
        Return plugin name, parsed from self.url and set self._src_folder as
        path to folder containing dataplugin source code
        """

    @abc.abstractmethod
    def can_handle(self) -> bool:
        """
        Return true if assistant can handle the code
        """


class GithubAssistant(BaseAssistant):
    plugin_type = "github"

    @property
    def plugin_name(self):
        folder_name = parse_folder_name_from_url(self.url)
        if not self._plugin_name:
            self._src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, folder_name)
            # Try to load plugin name from the local first, if exist that mean we are working with a cloned and updated plugin
            # If plugin name is empty that mean this plugin has not cloned to local then we try to fetch its name from the Github
            # Otherwise we use the path_name as the fallback.
            plugin_name = get_plugin_name_from_local_manifest(os.path.join(btconfig.DATA_PLUGIN_FOLDER, folder_name))
            if not plugin_name:
                plugin_name = get_plugin_name_from_remote_manifest(self.url)
            if not plugin_name:
                plugin_name = folder_name
            self._plugin_name = plugin_name
        return self._plugin_name

    def can_handle(self) -> bool:
        # analyze headers to guess type of required assitant
        try:
            headers = requests.head(self.url).headers
            return headers.get("server").lower() == "github.com"
        except Exception as gen_exc:
            self.logger.exception(gen_exc)
            self.logger.error("%s plugin can't handle URL '%s': %s" % (self.plugin_type, self.url, e))
            return False

    def get_classdef(self):
        # generate class dynamically and register
        confdict = {
            "SRC_NAME": self.plugin_name,
            "GIT_REPO_URL": self.url,
            "SRC_ROOT_FOLDER": self._src_folder,
        }
        # TODO: store confdict in hubconf collection
        k = type("AssistedGitDataPlugin_%s" % self.plugin_name, (GitDataPlugin,), confdict)
        return k


class LocalAssistant(BaseAssistant):
    plugin_type = "local"

    @property
    def plugin_name(self):
        """
        We attempt to derive the plugin name from the url as we expect the URL
        (for local plugins) to follow the structure local://<pluginname>

        Formats local://pluginname so it's in hostname.
        (we leverage urlsplit over urlparse due to lack of need for parameter parsing)
        https://docs.python.org/3/library/urllib.parse.html#structured-parse-results

        If we discover a subdirectory we raise an error for moment due to lack of subdirectory
        support at the moment for our pathing

        This can be verified by checking the `path` value from the SplitResult

        url -> local://plugin-name
        Supported:
        > ParseResult(scheme='local', netloc='plugin-name', path='', params='', query='', fragment='')

        url -> local://sub-directory/plugin-name
        Unsupported:
        > ParseResult(scheme='local', netloc='plugin-name', path='sub-directory', params='', query='', fragment='')
        """
        if not self._plugin_name:
            split = urllib.parse.urlsplit(self.url)
            # format local://pluginname so it's in hostname.
            # if path is set, it means format is  local://subdir/pluginname
            # and we don't support that for import reason (we would need to
            # add .../plugins/subdir to sys.path, not impossible but might have side effects
            # so for now we stay on the safe (and also let's remember 1st version of
            # MS DOS didn't support subdirs, so I guess we're on the right path :))
            assert not split.path, "It seems URL '%s' references a sub-directory (%s)," % (
                self.url,
                split.hostname,
            ) + " with plugin name '%s', sub-directories are not supported (yet)" % split.path.strip("/")
            # don't use hostname here because it's lowercased, netloc isn't
            # (and we're matching directory names on the filesystem, it's case-sensitive)
            src_folder_name = os.path.basename(split.netloc)
            try:
                self._plugin_name = (
                    get_plugin_name_from_local_manifest(os.path.join(btconfig.DATA_PLUGIN_FOLDER, src_folder_name))
                    or src_folder_name
                )
            except Exception as ex:
                self.logger.exception(ex)
                self._plugin_name = src_folder_name
            self._src_folder = os.path.join(btconfig.DATA_PLUGIN_FOLDER, src_folder_name)
        return self._plugin_name

    def can_handle(self) -> bool:
        return self.url.startswith(self.__class__.plugin_type + "://")

    def get_classdef(self):
        # generate class dynamically and register
        confdict = {"SRC_NAME": self.plugin_name, "SRC_ROOT_FOLDER": self._src_folder}
        k = type("AssistedManualDataPlugin_%s" % self.plugin_name, (ManualDataPlugin,), confdict)
        return k
