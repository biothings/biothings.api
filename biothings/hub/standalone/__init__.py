"""
    This standalone module is originally located at "biothings/standalone" repo.
    It's used for Standalone/Autohub instance.
"""
import os, logging, sys
from functools import partial
from collections import OrderedDict
import asyncio

from biothings import config as btconfig

from biothings.hub.autoupdate import BiothingsDumper, BiothingsUploader
from biothings.utils.es import ESIndexer
from biothings.utils.backend import DocESBackend
from biothings.utils.hub import CommandDefinition
from biothings.hub import HubServer

class AutoHubServer(HubServer):

    DEFAULT_FEATURES = ["job","autohub","terminal","config","ws"]

    DEFAULT_DUMPER_CLASS = BiothingsDumper
    DEFAULT_UPLOADER_CLASS = BiothingsUploader

    def __init__(self, version_urls, indexer_factory=None, *args, **kwargs):
        """
        version_urls is a list of URLs pointing to versions.json file. The name
        of the data release is taken from the URL (http://...s3.amazon.com/<the_name>/versions.json)
        unless specified as a dict: {"name" : "custom_name", "url" : "http://..."}

        If indexer_factory is passed, it'll be used to create indexer used to dump/check versions
        currently installed on ES, restore snapshot, index, etc... A indexer_factory is typically
        used to generate indexer dynamically (ES host, index name, etc...) according to URLs for
        instance. See standalone.hub.DynamicIndexerFactory class for an example. It is typically
        used when lots of data releases are being managed by the Hub (so no need to manually update
        STANDALONE_CONFIG parameter.

        If indexer_factory is None, a config param named STANDALONE_CONFIG is used,
        format is the following:

            {"_default" : {"es_host": "...", "index": "...", "doc_type" : "..."},
             "the_name" : {"es_host": "...", "index": "...", "doc_type" : "..."}}

        When a data release named (from URL) matches an entry, it's used to configured
        which ES backend to target, otherwise the default one is used.
        """
        super().__init__(*args, **kwargs)
        self.version_urls = self.extract(version_urls)
        self.indexer_factory = indexer_factory

    def extract(self,urls):
        vurls = []
        for url in urls:
            if isinstance(url,dict):
                assert "name" in url and "url" in url
                vurls.append(url)
            else:
                vurls.append({"name" : self.get_folder_name(url), "url" : url})

        return vurls

    def install(self, src_name, version="latest", dry=False, force=False):
        """
        Update hub's data up to the given version (default is latest available),
        using full and incremental updates to get up to that given version (if possible).
        """
        @asyncio.coroutine
        def do(version):
            try:
                dklass = self.managers["dump_manager"][src_name][0] # only one dumper allowed / source
                dobj = self.managers["dump_manager"].create_instance(dklass)
                update_path = dobj.find_update_path(version,backend_version=dobj.target_backend.version)
                version_path = [v["build_version"] for v in update_path]
                if not version_path:
                    logging.info("No update path found")
                    return

                logging.info("Found path for updating from version '%s' to version '%s': %s" % (dobj.target_backend.version,version,version_path))
                if dry:
                    return version_path

                for step_version in version_path:
                    logging.info("Downloading data for version '%s'" % step_version)
                    jobs = self.managers["dump_manager"].dump_src(src_name,version=step_version,force=force)
                    download = asyncio.gather(*jobs)
                    res = yield from download
                    assert len(res) == 1
                    if res[0] == None:
                        # download ready, now install
                        logging.info("Updating backend to version '%s'" % step_version)
                        jobs = self.managers["upload_manager"].upload_src(src_name)
                        upload = asyncio.gather(*jobs)
                        res = yield from upload

            except Exception as e:
                self.logger.exception(e)
                raise
        return asyncio.ensure_future(do(version))

    def get_folder_name(self,url):
        return os.path.basename(os.path.dirname(url))

    def get_class_name(self, folder):
        """Return class-compliant name from a folder name"""
        return folder.replace(".","_").replace("-","_")

    def list_biothings(self):
        return self.version_urls

    def configure_autohub_feature(self):
        # "autohub" feature is based on "dump","upload" and "sync" features.
        # we don't list them in DEFAULT_FEATURES as we don't want them to produce
        # commands such as dump() or upload() as these are renamed for clarity
        # that said, those managers could still exist *if* autohub is mixed
        # with "standard" hub, so we don't want to override them if already configured
        if not self.managers.get("dump_manager"): self.configure_dump_manager()
        if not self.managers.get("upload_manager"): self.configure_upload_manager()
        if not self.managers.get("sync_manager"): self.configure_sync_manager()
        default_standalone_conf = getattr(btconfig,"STANDALONE_CONFIG",{}).get("_default")
        for info in self.version_urls:
            version_url = info["url"]
            self.__class__.DEFAULT_DUMPER_CLASS.VERSION_URL = version_url
            if self.indexer_factory:
                pidxr, actual_conf = self.indexer_factory.create(info["name"])
            else:
                actual_conf = btconfig.STANDALONE_CONFIG.get(info["name"],default_standalone_conf)
                assert actual_conf, "No standalone config could be found for data release '%s'" % info["name"]
                pidxr = partial(ESIndexer,index=actual_conf["index"],
                                doc_type=None,
                                es_host=actual_conf["es_host"])
            partial_backend = partial(DocESBackend,pidxr)

            SRC_NAME = info["name"]
            dump_class_name = "%sDumper" % self.get_class_name(SRC_NAME)
            # dumper
            dumper_klass = type(dump_class_name,(self.__class__.DEFAULT_DUMPER_CLASS,),
                    {"TARGET_BACKEND" : partial_backend,
                     "SRC_NAME" : SRC_NAME,
                     "SRC_ROOT_FOLDER" : os.path.join(btconfig.DATA_ARCHIVE_ROOT, SRC_NAME),
                     "VERSION_URL" : version_url,
                     "AWS_ACCESS_KEY_ID" : btconfig.STANDALONE_AWS_CREDENTIALS.get("AWS_ACCESS_KEY_ID"),
                     "AWS_SECRET_ACCESS_KEY" : btconfig.STANDALONE_AWS_CREDENTIALS.get("AWS_SECRET_ACCESS_KEY")
                     })
            sys.modules["standalone.hub"].__dict__[dump_class_name] = dumper_klass
            self.managers["dump_manager"].register_classes([dumper_klass])
            # uploader
            # syncer will work on index used in web part
            esb = (actual_conf["es_host"], actual_conf["index"], None)
            partial_syncer = partial(self.managers["sync_manager"].sync,"es",target_backend=esb)
            # manually register biothings source uploader
            # this uploader will use dumped data to update an ES index
            uploader_class_name = "%sUploader" % self.get_class_name(SRC_NAME)
            uploader_klass = type(uploader_class_name,(self.__class__.DEFAULT_UPLOADER_CLASS,),
                    {"TARGET_BACKEND" : partial_backend,
                     "SYNCER_FUNC" : partial_syncer,
                     "AUTO_PURGE_INDEX" : True, # because we believe
                     "name" : SRC_NAME
                    })
            sys.modules["standalone.hub"].__dict__[uploader_class_name] = uploader_klass
            self.managers["upload_manager"].register_classes([uploader_klass])

    def configure_commands(self):
        super().configure_commands()
        self.commands["list"] = CommandDefinition(command=self.list_biothings,tracked=False)
        # dump commands
        self.commands["versions"] = partial(self.managers["dump_manager"].call,method_name="versions")
        self.commands["check"] = partial(self.managers["dump_manager"].dump_src,check_only=True)
        self.commands["info"] = partial(self.managers["dump_manager"].call,method_name="info")
        self.commands["download"] = partial(self.managers["dump_manager"].dump_src)
        # upload commands
        self.commands["apply"] = partial(self.managers["upload_manager"].upload_src)
        self.commands["install"] = partial(self.install)
        self.commands["backend"] = partial(self.managers["dump_manager"].call,method_name="get_target_backend")
        self.commands["reset_backend"] = partial(self.managers["dump_manager"].call,method_name="reset_target_backend")

    def configure_api_endpoints(self):
        super().configure_api_endpoints()
        from biothings.hub.api import EndpointDefinition
        cmdnames = list(self.commands.keys())
        self.api_endpoints["standalone"] = []
        self.api_endpoints["standalone"].append(EndpointDefinition(name="list",method="get",suffix="list"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="versions",method="get",suffix="versions"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="check",method="get",suffix="check"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="info",method="get",suffix="info"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="backend",method="get",suffix="backend"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="reset_backend",method="delete",suffix="backend"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="download",method="post",suffix="download"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="apply",method="post",suffix="apply"))
        self.api_endpoints["standalone"].append(EndpointDefinition(name="install",method="post",suffix="install"))


class DynamicIndexerFactory(object):
    """
    Create indexer with parameters taken from versions.json URL.
    A list of  URLs is provided so the factory knows how to create these
    indexers for each URLs. There's no way to "guess" an ES host from a URL,
    so this parameter must be specified as well, common to all URLs
    "suffix" param is added at the end of index names.
    """

    def __init__(self, urls, es_host, suffix="_current"):
        self.urls = urls
        self.es_host = es_host
        self.bynames = {}
        for url in urls:
            if isinstance(url, dict):
                name = url["name"]
                # actual_url = url["url"]
            else:
                name = os.path.basename(os.path.dirname(url))
                # actual_url = url
            self.bynames[name] = {
                "es_host": self.es_host,
                "index": name + suffix
            }

    def create(self, name):
        conf = self.bynames[name]
        pidxr = partial(ESIndexer, index=conf["index"],
                        doc_type=None,
                        es_host=conf["es_host"])
        conf = {"es_host": conf["es_host"], "index": conf["index"]}
        return pidxr, conf
