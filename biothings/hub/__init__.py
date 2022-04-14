import asyncio
import concurrent.futures
import copy
import glob
import logging
import os
import sys
import time
import types
from collections import OrderedDict
from functools import partial
from pprint import pformat
from types import SimpleNamespace

import aiocron
import asyncssh
from biothings.utils.configuration import *
from biothings.utils.document_generator import generate_command_documentations
from . import default_config


def _config_for_app(config_mod=None):

    if not config_mod:
        config_name = os.environ.get("HUB_CONFIG", "config")
        config_mod = import_module(config_name)

    if not isinstance(config_mod, (types.ModuleType, SimpleNamespace)):
        raise TypeError(type(config_mod))

    for attr in dir(config_mod):
        value = getattr(config_mod, attr)
        if isinstance(value, ConfigurationError):
            raise ConfigurationError("%s: %s" % (attr, str(value)))

    try:
        app_path = os.path.split(config_mod.__file__)[0]
        sys.path.insert(0, app_path)
    except Exception:
        logging.exception(config_mod)
        app_path = ""  # TODO

    wrapper = ConfigurationWrapper(default_config, config_mod)
    wrapper.APP_PATH = app_path

    if not hasattr(config_mod, "HUB_DB_BACKEND"):
        raise AttributeError("HUB_DB_BACKEND Not Found.")

    # this will create a "biothings.config" module
    # so "from biothings from config" will get app config at lib level
    biothings = import_module("biothings")
    biothings.config = wrapper
    globals()["config"] = wrapper

    import biothings.utils.hub_db  # the order of the following commands matter
    wrapper.hub_db = import_module(config_mod.HUB_DB_BACKEND["module"])
    biothings.utils.hub_db.setup(wrapper)
    wrapper._db = biothings.utils.hub_db.get_hub_config()

    # setup logging
    from biothings.utils.loggers import EventRecorder
    logger = logging.getLogger()
    fmt = logging.Formatter(
        '%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s',
        datefmt="%H:%M:%S")
    erh = EventRecorder()
    erh.name = "event_recorder"
    erh.setFormatter(fmt)
    if erh.name not in [h.name for h in logger.handlers]:
        logger.addHandler(erh)


_config_for_app()

# FOR DEVELOPMENT USAGE
# --------------------------
# try:
#     _config_for_app()
# except Exception:
#     logging.exception("Fallback to local DB.")
#     _config = SimpleNamespace()
#     _config.HUB_DB_BACKEND = {
#         "module": "biothings.utils.sqlite3",
#         "sqlite_db_folder": "."}
#     _config.DATA_HUB_DB_DATABASE = ".hubdb"
#     _config_for_app(_config)


from biothings.utils.common import get_class_from_classpath
from biothings.utils.hub import (AlreadyRunningException, CommandDefinition,
                                 CommandError, HubShell, get_hub_reloader,
                                 pending)
from biothings.utils.jsondiff import make as jsondiff
from biothings.utils.loggers import (ShellLogger, WSLogHandler, WSShellHandler,
                                     get_logger)
from biothings.utils.version import check_new_version, get_version

# adjust some loggers...
if os.environ.get("HUB_VERBOSE", "0") != "1":
    logging.getLogger("elasticsearch").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.ERROR)
    logging.getLogger("boto3").setLevel(logging.ERROR)
    logging.getLogger("git").setLevel(logging.ERROR)

def get_loop(max_workers=None):
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    loop.set_default_executor(executor)
    return loop


# Keys used as category in pinfo (description of jobs submitted to JobManager)
# Those are used in different places
DUMPER_CATEGORY = "dumper"
UPLOADER_CATEGORY = "uploader"
BUILDER_CATEGORY = "builder"
INDEXER_CATEGORY = "indexer"
INDEXMANAGER_CATEGORY = "indexmanager"
RELEASEMANAGER_CATEGORY = "releasemanager"
RELEASER_CATEGORY = "releaser"
SNAPSHOTMANAGER_CATEGORY = "snapshotmanager"
SNAPSHOOTER_CATEGORY = "snapshooter"
DIFFER_CATEGORY = "differ"
DIFFMANAGER_CATEGORY = "diffmanager"
SYNCER_CATEGORY = "syncer"
INSPECTOR_CATEGORY = "inspector"

# HUB_REFRESH_COMMANDS = hasattr(
#     config, "HUB_REFRESH_COMMANDS"
# ) and config.HUB_REFRESH_COMMANDS or "* * * * * *"  # every sec
HUB_REFRESH_COMMANDS = getattr(
    config, "HUB_REFRESH_COMMANDS", "* * * * * *"  # every sec
)

# Check for new code update from app and biothings Git repo
HUB_CHECK_UPGRADE = getattr(
    config, "HUB_CHECK_UPGRADE", "0 * * * *"  # every hour
)


class JobRenderer(object):
    def __init__(self):
        self.rendered = {
            types.FunctionType: self.render_func,
            types.MethodType: self.render_method,
            partial: self.render_partial,
            types.LambdaType: self.render_lambda,
        }

    def render(self, job):
        r = self.rendered.get(type(job._callback))
        rstr = r(job._callback)
        delta = job._when - job._loop.time()
        days = None
        if delta > 86400:
            days = int(delta / 86400)
            delta = delta - 86400
        strdelta = time.strftime("%Hh:%Mm:%Ss", time.gmtime(int(delta)))
        if days:
            strdelta = "%d day(s) %s" % (days, strdelta)
        return "%s {run in %s}" % (rstr, strdelta)

    def render_partial(self, p):
        # class.method(args)
        return self.rendered[type(p.func)](p.func) + "%s" % str(p.args)

    def render_cron(self, c):
        # func type associated to cron can vary
        return self.rendered[type(c.func)](c.func) + " [%s]" % c.spec

    def render_func(self, f):
        return f.__name__

    def render_method(self, m):
        # what is self ? cron ?
        # if type(m.__self__) == aiocron.Cron:   # TODO: delete if confirmed
        if isinstance(m.__self__, aiocron.Cron):
            return self.render_cron(m.__self__)
        else:
            return "%s.%s" % (m.__self__.__class__.__name__, m.__name__)

    def render_lambda(self, l):
        return l.__name__


renderer = JobRenderer()


def status(managers):
    """
    Return a global hub status (number or sources, documents, etc...)
    according to available managers
    """
    total_srcs = None
    total_docs = None
    total_confs = None
    total_builds = None
    total_apis = None
    total_running_apis = None
    if managers.get("source_manager"):
        try:
            srcm = managers["source_manager"]
            srcs = srcm.get_sources()
            total_srcs = len(srcs)
            total_docs = sum([s["upload"]["sources"][subs].get("count", 0) or 0
                              for s in srcs
                              for subs in s.get("upload", {}).get("sources", {})
                              if s.get("upload")])
        except Exception:
            logging.exception("Can't get stats for sources:")

    try:
        bm = managers["build_manager"]
        total_confs = len(bm.build_config_info())
    except Exception:
        logging.exception("Can't get total number of build configurations:")
    try:
        total_builds = len(bm.build_info())
    except Exception:
        logging.exception("Can't get total number of builds:")

    try:
        am = managers["api_manager"]
        apis = am.get_apis()
        total_apis = len(apis)
        total_running_apis = len(
            [a for a in apis if a.get("status") == "running"])
    except Exception:
        logging.exception("Can't get stats for APIs:")

    return {
        "source": {
            "total": total_srcs,
            "documents": total_docs
        },
        "build": {
            "total": total_builds
        },
        "build_conf": {
            "total": total_confs
        },
        "api": {
            "total": total_apis,
            "running": total_running_apis
        },
    }


def get_schedule(loop):
    """try to render job in a human-readable way..."""
    out = []
    for sch in loop._scheduled:
        if type(sch) != asyncio.events.TimerHandle:
            continue
        if sch._cancelled:
            continue
        try:
            info = renderer.render(sch)
            out.append(info)
        except Exception:
            import traceback
            traceback.print_exc()
            out.append(sch)

    return "\n".join(out)


async def start_ssh_server(loop,
                           name,
                           passwords,
                           keys=['bin/ssh_host_key'],
                           shell=None,
                           host='',
                           port=8022):
    for key in keys:
        assert os.path.exists(
            key
        ), "Missing key '%s' (use: 'ssh-keygen -f %s' to generate it" % (key,
                                                                         key)
    HubSSHServer.PASSWORDS = passwords
    HubSSHServer.NAME = name
    HubSSHServer.SHELL = shell
    aiocron.crontab(HUB_REFRESH_COMMANDS,
                    func=shell.__class__.refresh_commands,
                    start=True,
                    loop=loop)
    # yield from asyncssh.create_server(HubSSHServer,
    #                                   host,
    #                                   port,
    #                                   loop=loop,
    #                                   server_host_keys=keys)
    await asyncssh.create_server(HubSSHServer,
                                 host,
                                 port,
                                 #loop=loop,
                                 server_host_keys=keys)


class HubCommands(OrderedDict):
    def __setitem__(self, k, v):
        if k in self:
            raise ValueError("Command '%s' already defined" % k)
        super().__setitem__(k, v)


class HubServer(object):

    DEFAULT_FEATURES = [
        "config", "job", "dump", "upload", "dataplugin", "source", "build",
        "diff", "index", "snapshot", "release", "inspect", "sync", "api",
        "terminal", "reloader", "dataupload", "ws", "readonly", "upgrade",
        "autohub", "hooks",
    ]
    DEFAULT_MANAGERS_ARGS = {"upload": {"poll_schedule": "* * * * * */10"}}
    DEFAULT_RELOADER_CONFIG = {
        "folders": None,  # will use default one
        "managers": ["source_manager", "assistant_manager"],
        "reload_func": None
    }  # will use default one
    DEFAULT_DATAUPLOAD_CONFIG = {
        "upload_root": getattr(config, "DATA_UPLOAD_FOLDER", None)
    }
    DEFAULT_WEBSOCKET_CONFIG = {}
    DEFAULT_API_CONFIG = {}
    DEFAULT_AUTOHUB_CONFIG = {
        "version_urls": getattr(config, "VERSION_URLS", []),
        "indexer_factory": getattr(config, "AUTOHUB_INDEXER_FACTORY", None),
        "es_host": getattr(config, "AUTOHUB_ES_HOST", None),
    }

    def __init__(self,
                 source_list,
                 features=None,
                 name="BioThings Hub",
                 managers_custom_args={},
                 api_config=None,
                 reloader_config=None,
                 dataupload_config=None,
                 websocket_config=None,
                 autohub_config=None):
        """
        Helper to setup and instantiate common managers usually used in a hub
        (eg. dumper manager, uploader manager, etc...)
        "source_list" is either:
            - a list of string corresponding to paths to datasources modules
            - a package containing sub-folders with datasources modules
        Specific managers can be retrieved adjusting "features" parameter, where
        each feature corresponds to one or more managers. Parameter defaults to
        all possible available. Managers are configured/init in the same order as the list,
        so if a manager (eg. job_manager) is required by all others, it must be the first
        in the list.
        "managers_custom_args" is an optional dict used to pass specific arguments while
        init managers:
            managers_custom_args={"upload" : {"poll_schedule" : "*/5 * * * *"}}
        will set poll schedule to check upload every 5min (instead of default 10s)
        "reloader_config", "dataupload_config", "autohub_config" and "websocket_config"
        can be used to customize reloader, dataupload and websocket. If None, default config
        is used. If explicitely False, feature is deactivated.
        """
        self.name = name
        self.source_list = source_list
        self.logger, self.logfile = get_logger("hub")
        self._passed_features = features
        self._passed_managers_custom_args = managers_custom_args
        self.features = self.clean_features(features or self.DEFAULT_FEATURES)
        self.managers_custom_args = managers_custom_args
        self.reloader_config = reloader_config or self.DEFAULT_RELOADER_CONFIG
        self.dataupload_config = dataupload_config or self.DEFAULT_DATAUPLOAD_CONFIG
        self.websocket_config = websocket_config or self.DEFAULT_WEBSOCKET_CONFIG
        self.autohub_config = autohub_config or self.DEFAULT_AUTOHUB_CONFIG
        self.ws_listeners = [
        ]  # collect listeners that should be connected (push data through) to websocket
        self.api_config = api_config or self.DEFAULT_API_CONFIG
        # set during configure()
        self.managers = None
        self.api_endpoints = {}
        self.readonly_api_endpoints = None
        self.shell = None
        self.commands = None  # default "public" commands
        self.extra_commands = None  # "hidden" commands, but still useful for advanced usage
        self.hook_files = None  # user-defined commands as hook files
        self.routes = []
        self.readonly_routes = []
        self.ws_urls = []  # only one set, shared between r/w and r/o hub api server
        # flag "do we need to configure?"
        self.configured = False

    def clean_features(self, features):
        """
        Sanitize (ie. remove duplicates) features
        """
        # we can't just use "set()" because we need to preserve order
        ordered = OrderedDict()
        for feat in features:
            if feat not in ordered:
                ordered[feat] = None
        return list(ordered.keys())

    def before_configure(self):
        """
        Hook triggered before configure(),
        used eg. to adjust features list
        """
        pass

    def configure_readonly_api_endpoints(self):
        """
        Assuming read-write API endpoints have previously been defined (self.api_endpoints set)
        extract commands and their endpoint definitions only when method is GET. That is, for any
        given API definition honoring REST principle for HTTP verbs, generate endpoints only for
        which actions are read-only actions.
        """
        assert self.api_endpoints, "Can't derive a read-only API is no read-write endpoints are defined"
        self.readonly_api_endpoints = {}
        for cmd, api_endpoints in self.api_endpoints.items():
            if not isinstance(api_endpoints, list):
                api_endpoints = [api_endpoints]
            for endpoint in api_endpoints:
                if endpoint["method"].lower() != "get":
                    self.logger.debug("Skipping %s: %s for read-only API" % (cmd, endpoint))
                    continue
                else:
                    self.readonly_api_endpoints.setdefault(cmd, []).append(endpoint)

    def configure(self):
        self.before_configure()
        self.remaining_features = copy.deepcopy(
            self.features)  # keep track of what's been configured
        self.configure_ioloop()
        self.configure_managers()
        # setup the shell
        self.shell = HubShell(self.managers["job_manager"])
        self.shell.register_managers(self.managers)
        self.shell.server = self  # propagate server instance in shell
        # so it's accessible from the console if needed
        self.configure_remaining_features()
        self.configure_commands()
        self.configure_extra_commands()
        self.shell.set_commands(self.commands, self.extra_commands)
        self.ingest_hooks()
        # setapi
        if self.api_config is not False:
            self.configure_api_endpoints(
            )  # after shell setup as it adds some default commands
            # we want to expose throught the api
            from biothings.hub.api import generate_api_routes
            from biothings.hub.api.handlers.base import RootHandler

            # First deal with read-only API
            if "readonly" in self.features:
                self.configure_readonly_api_endpoints()
                self.readonly_routes.extend(
                    generate_api_routes(self.shell, self.readonly_api_endpoints))
                # we don't want to expose feature read-only for the API that is *not*
                # read-only. "readonly" feature means we're running another webapp for
                # a specific readonly API. UI can then query the root handler and see
                # if the API is readonly or not, and adjust the components & actions
                ro_features = copy.deepcopy(self.features)
                # terminal feature certainly not allowed in read-only server...
                if "terminal" in self.features:
                    ro_features.remove("terminal")
                # if we have readonly feature, it means another non-readonly server is running
                self.features.remove("readonly")
                hub_name = getattr(config, "HUB_NAME", "Hub") + " (read-only)"
                self.readonly_routes.append(("/", RootHandler, {
                    "features": ro_features, "hub_name": hub_name
                }))

            # Then deal with read-write API
            self.routes.extend(
                generate_api_routes(self.shell, self.api_endpoints))
            from biothings.hub.api.handlers.log import (HubLogDirHandler,
                                                        HubLogFileHandler)
            self.routes.append(("/logs/(.*)", HubLogDirHandler, {"path": config.LOG_FOLDER}))
            self.routes.append(("/log/(.+)", HubLogFileHandler, {"path": config.LOG_FOLDER}))
            self.routes.append(("/", RootHandler, {
                "features": self.features,
            }))

        # done
        self.configured = True

    def configure_ioloop(self):
        import tornado.platform.asyncio
        tornado.platform.asyncio.AsyncIOMainLoop().install()

    def before_start(self):
        pass

    def start(self):
        if not self.configured:
            self.configure()
        self.logger.info("Starting '%s'", self.name)
        # can't use asyncio.get_event_loop() if python < 3.5.3 as it would return
        # another instance of aio loop, take it from job_manager to make sure
        # we share the same one
        loop = self.managers["job_manager"].loop
        if self.routes:
            self.logger.info("Starting Hub API server on port %s" % config.HUB_API_PORT)
            #self.logger.info(self.routes)
            import tornado.web

            # register app into current event loop
            api = tornado.web.Application(self.routes)
            self.extra_commands["api"] = api
            from biothings.hub.api import start_api
            start_api(api,
                      config.HUB_API_PORT,
                      settings=getattr(config, "TORNADO_SETTINGS", {})
                      )
            if self.readonly_routes:
                if not getattr(config, "READONLY_HUB_API_PORT", None):
                    self.logger.warning("Read-only Hub API feature is set but READONLY_HUB_API_PORT "
                                        + "isn't set in configuration")
                else:
                    self.logger.info("Starting read-only Hub API server on port %s" % config.READONLY_HUB_API_PORT)
                    #self.logger.info(self.readonly_routes)
                    ro_api = tornado.web.Application(self.readonly_routes)
                    start_api(ro_api,
                              config.READONLY_HUB_API_PORT,
                              settings=getattr(config, "TORNADO_SETTINGS", {}))
        else:
            self.logger.info("No route defined, API server won't start")
        # at this point, everything is ready/set, last call for customizations
        self.before_start()
        self.logger.info("Starting Hub SSH server on port %s" % config.HUB_SSH_PORT)
        self.ssh_server = start_ssh_server(loop,
                                           self.name,
                                           passwords=config.HUB_PASSWD,
                                           port=config.HUB_SSH_PORT,
                                           shell=self.shell)
        try:
            loop.run_until_complete(self.ssh_server)
        except (OSError, asyncssh.Error) as exc:
            sys.exit('Error starting server: ' + str(exc))
        loop.run_forever()

    def mixargs(self, feat, params=None):
        params = params or {}
        args = {}
        for p in params:
            args[p] = self.managers_custom_args.get(feat, {}).pop(
                p, None) or params[p]
        # mix remaining
        args.update(self.managers_custom_args.get(feat, {}))
        return args

    def configure_job_manager(self):
        import asyncio
        loop = asyncio.get_event_loop()
        from biothings.utils.manager import JobManager
        args = self.mixargs(
            "job", {
                "num_workers": config.HUB_MAX_WORKERS,
                "max_memory_usage": config.HUB_MAX_MEM_USAGE
            })
        job_manager = JobManager(loop, **args)
        self.managers["job_manager"] = job_manager

    def configure_dump_manager(self):
        from biothings.hub.dataload.dumper import DumperManager
        args = self.mixargs("dump")
        dmanager = DumperManager(job_manager=self.managers["job_manager"],
                                 **args)
        self.managers["dump_manager"] = dmanager

    def configure_upload_manager(self):
        from biothings.hub.dataload.uploader import UploaderManager
        args = self.mixargs("upload", {"poll_schedule": "* * * * * */10"})
        upload_manager = UploaderManager(
            job_manager=self.managers["job_manager"], **args)
        self.managers["upload_manager"] = upload_manager

    def configure_dataplugin_manager(self):
        from biothings.hub.dataplugin.manager import DataPluginManager
        dp_manager = DataPluginManager(
            job_manager=self.managers["job_manager"])
        self.managers["dataplugin_manager"] = dp_manager
        from biothings.hub.dataplugin.assistant import AssistantManager
        args = self.mixargs("dataplugin")
        assistant_manager = AssistantManager(
            data_plugin_manager=dp_manager,
            dumper_manager=self.managers["dump_manager"],
            uploader_manager=self.managers["upload_manager"],
            job_manager=self.managers["job_manager"],
            **args)
        self.managers["assistant_manager"] = assistant_manager

    def configure_build_manager(self):
        from biothings.hub.databuild.builder import BuilderManager
        args = self.mixargs("build")
        build_manager = BuilderManager(
            job_manager=self.managers["job_manager"], **args)
        build_manager.configure()
        self.managers["build_manager"] = build_manager
        build_manager.poll()

    def configure_diff_manager(self):
        from biothings.hub.databuild.differ import (DifferManager,
                                                    SelfContainedJsonDiffer)
        args = self.mixargs("diff")
        diff_manager = DifferManager(job_manager=self.managers["job_manager"],
                                     poll_schedule="* * * * * */10",
                                     **args)
        diff_manager.configure([
            SelfContainedJsonDiffer,
        ])
        diff_manager.poll(
            "diff", lambda doc: diff_manager.diff(
                "jsondiff-selfcontained", old=None, new=doc["_id"]))
        self.managers["diff_manager"] = diff_manager

    def configure_index_manager(self):
        from biothings.hub.dataindex.indexer import IndexManager
        args = self.mixargs("index")
        index_manager = IndexManager(job_manager=self.managers["job_manager"], **args)
        index_manager.configure(config.INDEX_CONFIG)
        self.managers["index_manager"] = index_manager

    def configure_snapshot_manager(self):
        assert "index" in self.features, "'snapshot' feature requires 'index'"
        from biothings.hub.dataindex.snapshooter import SnapshotManager
        args = self.mixargs("snapshot")
        snapshot_manager = SnapshotManager(
            index_manager=self.managers["index_manager"],
            job_manager=self.managers["job_manager"],
            poll_schedule="* * * * * */10", **args)
        snapshot_manager.configure(config.SNAPSHOT_CONFIG)
        snapshot_manager.poll("snapshot", snapshot_manager.snapshot_a_build)
        self.managers["snapshot_manager"] = snapshot_manager

    def configure_release_manager(self):
        assert "diff" in self.features, "'release' feature requires 'diff'"
        assert "snapshot" in self.features, "'release' feature requires 'snapshot'"
        from biothings.hub.datarelease.publisher import ReleaseManager
        args = self.mixargs("release")
        release_manager = ReleaseManager(
            diff_manager=self.managers["diff_manager"],
            snapshot_manager=self.managers["snapshot_manager"],
            job_manager=self.managers["job_manager"],
            poll_schedule="* * * * * */10",
            **args)
        release_manager.configure(config.RELEASE_CONFIG)
        release_manager.poll("release_note", release_manager.create_release_note_from_build)
        release_manager.poll("publish", release_manager.publish_build)

        self.managers["release_manager"] = release_manager

    def configure_sync_manager(self):
        from biothings.hub.databuild.syncer import SyncerManager
        args = self.mixargs("sync")
        sync_manager = SyncerManager(job_manager=self.managers["job_manager"],
                                     **args)
        sync_manager.configure()
        self.managers["sync_manager"] = sync_manager

    def configure_inspect_manager(self):
        assert "upload" in self.features, "'inspect' feature requires 'upload'"
        assert "build" in self.features, "'inspect' feature requires 'build'"
        from biothings.hub.datainspect.inspector import InspectorManager
        args = self.mixargs("inspect")
        inspect_manager = InspectorManager(
            upload_manager=self.managers["upload_manager"],
            build_manager=self.managers["build_manager"],
            job_manager=self.managers["job_manager"],
            **args)
        self.managers["inspect_manager"] = inspect_manager

    def configure_api_manager(self):
        assert "index" in self.features, "'api' feature requires 'index'"
        from biothings.hub.api.manager import APIManager
        args = self.mixargs("api")
        api_manager = APIManager(**args)
        self.managers["api_manager"] = api_manager

    def configure_source_manager(self):
        if "dump" in self.features or "upload" in self.features:
            self.mixargs("source")
            from biothings.hub.dataload.source import SourceManager
            source_manager = SourceManager(
                source_list=self.source_list,
                dump_manager=self.managers["dump_manager"],
                upload_manager=self.managers["upload_manager"],
                data_plugin_manager=self.managers.get("dataplugin_manager"),
            )
            self.managers["source_manager"] = source_manager
        # init data plugin once source_manager has been set (it inits dumper and uploader
        # managers, if assistant_manager is configured/loaded before, datasources won't appear
        # in dumper/uploader managers as they were not ready yet)
        if "dataplugin" in self.features:
            self.managers["assistant_manager"].configure()
            self.managers["assistant_manager"].load()

        # now that we have the source manager setup, we can schedule and poll
        if "dump" in self.features and not getattr(
                config, "SKIP_DUMPER_SCHEDULE", False):
            self.managers["dump_manager"].schedule_all()
        if "upload" in self.features and not getattr(
                config, "SKIP_UPLOADER_POLL", False):
            self.managers["upload_manager"].poll(
                'upload', lambda doc: self.shell.launch(
                    partial(self.managers["upload_manager"].upload_src, doc[
                        "_id"])))

    def configure_autohub_feature(self):
        """
        See bt.hub.standalone.AutoHubFeature
        """
        # "autohub" feature is based on "dump","upload" and "sync" features.
        # If autohub is running on its own (standalone instance only for instance)
        # we don't list them in DEFAULT_FEATURES as we don't want them to produce
        # commands such as dump() or upload() as these are renamed for clarity
        # that said, those managers could still exist *if* autohub is mixed
        # with "standard" hub, so we don't want to override them if already configured
        if not self.managers.get("dump_manager"):
            self.configure_dump_manager()
        if not self.managers.get("upload_manager"):
            self.configure_upload_manager()
        if not self.managers.get("sync_manager"):
            self.configure_sync_manager()

        # Originally, autohub was a hub server on its own, it's now
        # converted a feature;to avoid mixins and bringing complexity in this HubServer
        # definition, we use composition pointing to an instance of that feature which
        # encapsulates that complexity
        from biothings.hub.standalone import AutoHubFeature

        # only pass required manage rs
        autohub_managers = {
            "dump_manager": self.managers["dump_manager"],
            "upload_manager": self.managers["upload_manager"],
            "sync_manager": self.managers["sync_manager"],
            "job_manager": self.managers["job_manager"]
        }
        version_urls = self.autohub_config["version_urls"]
        indexer_factory = self.autohub_config["indexer_factory"]
        es_host = self.autohub_config["es_host"]
        factory = None
        if indexer_factory:
            assert es_host, "indexer_factory set but es_host not set (AUTOHUB_ES_HOST), can't know which ES server to use"
            try:
                factory_class = get_class_from_classpath(indexer_factory)
                factory = factory_class(version_urls, es_host)
            except (ImportError, ModuleNotFoundError) as e:
                self.logger.error("Couldn't find indexer factory class from '%s': %s" % (indexer_factory, e))
        self.autohub_feature = AutoHubFeature(autohub_managers, version_urls, factory)
        try:
            self.autohub_feature.configure()
            self.autohub_feature.configure_auto_release(config)
        except Exception as e:
            self.logger.error("Could't configure feature 'autohub', will be deactivated: %s" % e)
            self.features.remove("autohub")

    def configure_hooks_feature(self):
        """
        Ingest user-defined commands into hub namespace, giving access
        to all pre-defined commands (commands, extra_commands).
        This method prepare the hooks but the ingestion is done later
        when all commands are defined
        """
        hooks_folder = getattr(config, "HOOKS_FOLDER", "./hooks")
        if not os.path.exists(hooks_folder):
            self.logger.info("Hooks folder '%s' doesn't exist, creating it" % hooks_folder)
            os.makedirs(hooks_folder)
        self.hook_files = glob.glob(os.path.join(hooks_folder, "*.py"))

    def ingest_hooks(self):
        if not self.hook_files:
            return
        for pyfile in self.hook_files:
            try:
                self.logger.info("Processing hook file '%s'" % pyfile)
                self.process_hook_file(pyfile)
            except Exception as e:
                self.logger.exception("Can't process hook file: %s" % e)

    def process_hook_file(self, hook_file):
        strcode = open(hook_file).read()
        code = compile(strcode, "<string>", "exec")
        eval(code, self.shell.extra_ns, self.shell.extra_ns)

    def configure_managers(self):
        if self.managers is not None:
            raise Exception("Managers have already been configured")
        self.managers = {}

        self.logger.info("Setting up managers for following features: %s",
                         self.features)
        assert "job" in self.features, "'job' feature is mandatory"
        if "source" in self.features:
            assert "dump" in self.features and "upload" in self.features, "'source' feature requires both 'dump' and 'upload' features"
        if "dataplugin" in self.features:
            assert "source" in self.features, "'dataplugin' feature requires 'source' feature"

        # specific order, eg. job_manager is used by all managers
        for feat in self.features:
            if hasattr(self, "configure_%s_manager" % feat):
                self.logger.info("Configuring feature '%s'", feat)
                getattr(self, "configure_%s_manager" % feat)()
                self.remaining_features.remove(feat)
            elif hasattr(self, "configure_%s_feature" % feat):
                # see configure_remaining_features()
                pass  # this is configured after managers but should not produce an error
            else:
                raise AttributeError(
                    "Feature '%s' listed but no 'configure_%s_{manager|feature}' method found"
                    % (feat, feat))

        self.logger.info("Active manager(s): %s" % pformat(self.managers))

    def configure_config_feature(self):
        # just a placeholder
        pass

    def configure_upgrade_feature(self):
        """
        Allows a Hub to check for new versions (new commits to apply on running branch)
        and apply them on current code base
        """

        if not getattr(config, "app_folder", None) or not getattr(config, "biothings_folder", None):
            self.logger.warning("Can't schedule check for new code updates, "
                                + "app folder and/or biothings folder not defined")
            return

        from biothings.hub.upgrade import (ApplicationSystemUpgrade,
                                           BioThingsSystemUpgrade)

        def get_upgrader(klass, folder):
            version = get_version(folder)
            if version:
                klass.SRC_ROOT_FOLDER = folder
                klass.GIT_REPO_URL = version["giturl"]
                klass.DEFAULT_BRANCH = version["branch"]
                return klass
            else:
                # set a flag to skip version checks, folder is likely not a git folder
                _skip_list = getattr(self, 'upgrader_skip_folders', [])
                if folder not in _skip_list:
                    _skip_list.append(folder)
                    setattr(self, 'upgrader_skip_folders', _skip_list)

        bt_upgrader_class = get_upgrader(BioThingsSystemUpgrade, config.biothings_folder)
        app_upgrader_class = get_upgrader(ApplicationSystemUpgrade, config.app_folder)
        self.managers["dump_manager"].register_classes(
            [cls for cls in [bt_upgrader_class, app_upgrader_class] if cls]
        )

        loop = self.managers.get("job_manager") and self.managers[
            "job_manager"].loop or asyncio.get_event_loop()

        @aiocron.crontab(HUB_CHECK_UPGRADE, start=True, loop=loop)
        async def check_code_upgrade():
            _skip_list = getattr(self, 'upgrader_skip_folders', [])
            if _skip_list and config.biothings_folder in _skip_list and config.app_folder in _skip_list:
                # both folders cannot be checked for versions, exit now
                return

            self.logger.info("Checking for new code updates")
            if config.biothings_folder in _skip_list:
                bt_new = None
            else:
                bt_new = check_new_version(config.biothings_folder)
            if config.app_folder in _skip_list:
                app_new = None
            else:
                try:
                    app_new = check_new_version(config.app_folder)
                except Exception as e:
                    self.logger.warning("Can't check for new version: %s" % e)
                    return
            # enrich existing version information with an "upgrade" field.
            # note: we do that on config._module, the actual config.py module,
            # *not* directly on config as it's a wrapper over config._module
            for (name, new, param) in (("app", app_new, "APP_VERSION"), ("biothings", bt_new, "BIOTHINGS_VERSION")):
                if new:
                    self.logger.info("Found updates for %s:\n%s" % (name, pformat(new)))
                    getattr(config._module, param)["upgrade"] = new
                else:
                    # just in case, we pop out the key
                    val = getattr(config._module, param)
                    if val:
                        val.pop("upgrade", None)

        asyncio.ensure_future(check_code_upgrade.func())

    def get_websocket_urls(self):

        if self.ws_urls:
            return self.ws_urls

        import biothings.hub.api.handlers.ws as ws
        import sockjs.tornado
        from biothings.utils.hub_db import ChangeWatcher

        # monitor change in database to report activity in webapp
        self.db_listener = ws.HubDBListener()
        ChangeWatcher.add(self.db_listener)
        ChangeWatcher.publish()
        self.log_listener = ws.LogListener()
        # push log statements to the webapp
        root_logger = logging.getLogger(
        )  # careful, asyncio logger will trigger log statement while in the handler
        # (ie. infinite loop), root logger not recommended)
        root_logger.addHandler(WSLogHandler(self.log_listener))
        self.ws_listeners.extend([self.db_listener, self.log_listener])
        ws_router = sockjs.tornado.SockJSRouter(
            partial(ws.WebSocketConnection, listeners=self.ws_listeners),
            '/ws')
        self.ws_urls = ws_router.urls
        return self.ws_urls

    def configure_ws_feature(self):
        # add websocket endpoint
        ws_urls = self.get_websocket_urls()
        self.routes.extend(ws_urls)

    def configure_terminal_feature(self):
        assert "ws" in self.features, "'terminal' feature requires 'ws'"
        assert "ws" in self.remaining_features, "'terminal' feature should configured before 'ws'"
        # shell logger/listener to communicate between webapp and hub ssh console
        import biothings.hub.api.handlers.ws as ws
        shell_listener = ws.LogListener()
        shell_logger = logging.getLogger("shell")
        assert isinstance(shell_logger,
                          ShellLogger), "shell_logger isn't properly set"
        shell_logger.addHandler(WSShellHandler(shell_listener))
        self.ws_listeners.append(shell_listener)
        # webapp terminal to hub shell connection through /shell endpoint
        from biothings.hub.api.handlers.shell import ShellHandler
        shell_endpoint = ("/shell", ShellHandler, {
            "shell": self.shell,
            "shellog": shell_logger
        })
        self.routes.append(shell_endpoint)

    def configure_dataupload_feature(self):
        assert "ws" in self.features, "'dataupload' feature requires 'ws'"
        assert "ws" in self.remaining_features, "'dataupload' feature should configured before 'ws'"
        # this one is not bound to a specific command
        from biothings.hub.api.handlers.upload import UploadHandler

        # tuple type = interpreted as a route handler
        self.routes.append(
            (r"/dataupload/([\w\.-]+)?", UploadHandler, self.dataupload_config))

    def configure_reloader_feature(self):
        monitored_folders = self.reloader_config["folders"] or [
            "hub/dataload/sources",
            getattr(config, "DATA_PLUGIN_FOLDER", None),
            getattr(config, "HOOKS_FOLDER", "./hooks"),
        ]
        reload_func = self.reloader_config["reload_func"] or partial(
            self.shell.restart, force=True)
        reloader = get_hub_reloader(monitored_folders,
                                    reload_func=reload_func)
        reloader and reloader.monitor()

    def configure_readonly_feature(self):
        """
        Define then expose read-only Hub API endpoints
        so Hub can be accessed without any risk of modifying data
        """
        assert self.api_config is not False, "api_config (read/write API) is required " \
                                             + "to defined a read-only API (it's derived from)"
        # first websockets URLs (we only fetch data from websocket, so no
        # risk of write operations there
        ws_urls = self.get_websocket_urls()
        self.readonly_routes.extend(ws_urls)
        # the rest of the readonly feature setup is done as the end, when starting the server

    def configure_remaining_features(self):
        self.logger.info("Setting up remaining features: %s",
                         self.remaining_features)
        # specific order, eg. job_manager is used by all managers
        for feat in copy.deepcopy(self.remaining_features):
            if hasattr(self, "configure_%s_feature" % feat):
                getattr(self, "configure_%s_feature" % feat)()
                self.remaining_features.remove(feat)
                pass  # this is configured after managers but should not produce an error
            else:
                raise AttributeError(
                    "Feature '%s' listed but no 'configure_%s_feature' method found"
                    % (feat, feat))

    def configure_commands(self):
        """
        Configure hub commands according to available managers
        """
        assert self.managers, "No managers configured"
        self.commands = HubCommands()
        self.commands["status"] = CommandDefinition(command=partial(status, self.managers),
                                                    tracked=False)
        self.commands["export_command_documents"] = CommandDefinition(
            command=self.export_command_documents,
            tracked=False
        )

        if "config" in self.features:
            self.commands["config"] = CommandDefinition(command=config.show,
                                                        tracked=False)
            self.commands["setconf"] = config.store_value_to_db
            self.commands["resetconf"] = config.reset
        # getting info
        if self.managers.get("source_manager"):
            self.commands["source_info"] = CommandDefinition(
                command=self.managers["source_manager"].get_source,
                tracked=False)
            self.commands["source_reset"] = CommandDefinition(
                command=self.managers["source_manager"].reset, tracked=True)
        # dump commands
        if self.managers.get("dump_manager"):
            self.commands["dump"] = self.managers["dump_manager"].dump_src
            self.commands["dump_all"] = self.managers["dump_manager"].dump_all
        # upload commands
        if self.managers.get("upload_manager"):
            self.commands["upload"] = self.managers[
                "upload_manager"].upload_src
            self.commands["upload_all"] = self.managers[
                "upload_manager"].upload_all
        # building/merging
        if self.managers.get("build_manager"):
            self.commands["whatsnew"] = CommandDefinition(
                command=self.managers["build_manager"].whatsnew, tracked=False)
            self.commands["lsmerge"] = self.managers[
                "build_manager"].list_merge
            self.commands["rmmerge"] = self.managers[
                "build_manager"].delete_merge
            self.commands["merge"] = self.managers["build_manager"].merge
            self.commands["archive"] = self.managers[
                "build_manager"].archive_merge
        if hasattr(config, "INDEX_CONFIG"):
            self.commands["index_config"] = config.INDEX_CONFIG
        if hasattr(config, "SNAPSHOT_CONFIG"):
            self.commands["snapshot_config"] = config.SNAPSHOT_CONFIG
        if hasattr(config, "PUBLISH_CONFIG"):
            self.commands["publish_config"] = config.PUBLISH_CONFIG
        # diff
        if self.managers.get("diff_manager"):
            self.commands["diff"] = self.managers["diff_manager"].diff
            self.commands["report"] = self.managers["diff_manager"].diff_report
        # indexing commands
        if self.managers.get("index_manager"):
            self.commands["index"] = self.managers["index_manager"].index
            self.commands["index_cleanup"] = self.managers["index_manager"].cleanup
        if self.managers.get("snapshot_manager"):
            self.commands["snapshot"] = self.managers["snapshot_manager"].snapshot
            self.commands["snapshot_cleanup"] = self.managers["snapshot_manager"].cleanup
        # data release commands
        if self.managers.get("release_manager"):
            self.commands["create_release_note"] = self.managers[
                "release_manager"].create_release_note
            self.commands["get_release_note"] = CommandDefinition(
                command=self.managers["release_manager"].get_release_note,
                tracked=False)
            self.commands["publish"] = self.managers["release_manager"].publish
            self.commands["publish_diff"] = self.managers[
                "release_manager"].publish_diff
            self.commands["publish_snapshot"] = self.managers[
                "release_manager"].publish_snapshot
        if self.managers.get("sync_manager"):
            self.commands["sync"] = CommandDefinition(
                command=self.managers["sync_manager"].sync)
        # inspector
        if self.managers.get("inspect_manager"):
            self.commands["inspect"] = self.managers["inspect_manager"].inspect
        # data plugins
        if self.managers.get("assistant_manager"):
            self.commands["register_url"] = partial(
                self.managers["assistant_manager"].register_url)
            self.commands["unregister_url"] = partial(
                self.managers["assistant_manager"].unregister_url)
            self.commands["export_plugin"] = partial(
                self.managers["assistant_manager"].export)
        if self.managers.get("dataplugin_manager"):
            self.commands["dump_plugin"] = self.managers[
                "dataplugin_manager"].dump_src
        if "autohub" in self.DEFAULT_FEATURES:
            self.commands["list"] = CommandDefinition(command=self.autohub_feature.list_biothings, tracked=False)
            # dump commands
            self.commands["versions"] = partial(self.managers["dump_manager"].call, method_name="versions")
            self.commands["check"] = partial(self.managers["dump_manager"].dump_src, check_only=True)
            self.commands["info"] = partial(self.managers["dump_manager"].call, method_name="info")
            self.commands["download"] = partial(self.managers["dump_manager"].dump_src)
            # upload commands
            self.commands["apply"] = partial(self.managers["upload_manager"].upload_src)
            self.commands["install"] = partial(self.autohub_feature.install)
            self.commands["backend"] = partial(self.managers["dump_manager"].call, method_name="get_target_backend")
            self.commands["reset_backend"] = partial(self.managers["dump_manager"].call, method_name="reset_target_backend")

        logging.info("Registered commands: %s", list(self.commands.keys()))

    def configure_extra_commands(self):
        """
        Same as configure_commands() but commands are not exposed publicly in the shell
        (they are shortcuts or commands for API endpoints, supporting commands, etc...)
        """
        assert self.managers, "No managers configured"
        self.extra_commands = {}  # unordered since not exposed, we don't care
        loop = self.managers.get("job_manager") and self.managers[
            "job_manager"].loop or asyncio.get_event_loop()
        self.extra_commands["g"] = CommandDefinition(command=globals(),
                                                     tracked=False)
        self.extra_commands["sch"] = CommandDefinition(command=partial(get_schedule, loop),
                                                       tracked=False)
        # expose contant so no need to put quotes (eg. top(pending) instead of top("pending")
        self.extra_commands["pending"] = CommandDefinition(command=pending,
                                                           tracked=False)
        self.extra_commands["loop"] = CommandDefinition(command=loop,
                                                        tracked=False)

        if self.managers.get("job_manager"):
            self.extra_commands["pqueue"] = CommandDefinition(
                command=self.managers["job_manager"].process_queue,
                tracked=False)
            self.extra_commands["tqueue"] = CommandDefinition(
                command=self.managers["job_manager"].thread_queue,
                tracked=False)
            self.extra_commands["jm"] = CommandDefinition(
                command=self.managers["job_manager"], tracked=False)
            self.extra_commands["top"] = CommandDefinition(
                command=self.managers["job_manager"].top, tracked=False)
            self.extra_commands["job_info"] = CommandDefinition(
                command=self.managers["job_manager"].job_info, tracked=False)
            self.extra_commands["schedule"] = CommandDefinition(
                command=self.managers["job_manager"].schedule, tracked=False)
        if self.managers.get("source_manager"):
            self.extra_commands["sm"] = CommandDefinition(
                command=self.managers["source_manager"], tracked=False)
            self.extra_commands["sources"] = CommandDefinition(
                command=self.managers["source_manager"].get_sources,
                tracked=False)
            self.extra_commands["source_save_mapping"] = CommandDefinition(
                command=self.managers["source_manager"].save_mapping)
        if self.managers.get("dump_manager"):
            self.extra_commands["dm"] = CommandDefinition(
                command=self.managers["dump_manager"], tracked=False)
            self.extra_commands["dump_info"] = CommandDefinition(
                command=self.managers["dump_manager"].dump_info, tracked=False)
        if self.managers.get("dataplugin_manager"):
            self.extra_commands["dpm"] = CommandDefinition(
                command=self.managers["dataplugin_manager"], tracked=False)
        if self.managers.get("assistant_manager"):
            self.extra_commands["am"] = CommandDefinition(
                command=self.managers["assistant_manager"], tracked=False)
        if self.managers.get("upload_manager"):
            self.extra_commands["um"] = CommandDefinition(
                command=self.managers["upload_manager"], tracked=False)
            self.extra_commands["upload_info"] = CommandDefinition(
                command=self.managers["upload_manager"].upload_info,
                tracked=False)
        if self.managers.get("build_manager"):
            self.extra_commands["bm"] = CommandDefinition(
                command=self.managers["build_manager"], tracked=False)
            self.extra_commands["builds"] = CommandDefinition(
                command=self.managers["build_manager"].build_info,
                tracked=False)
            self.extra_commands["build"] = CommandDefinition(
                command=lambda id: self.managers["build_manager"].build_info(
                    id=id),
                tracked=False)
            self.extra_commands["build_config_info"] = CommandDefinition(
                command=self.managers["build_manager"].build_config_info,
                tracked=False)
            self.extra_commands["build_save_mapping"] = CommandDefinition(
                command=self.managers["build_manager"].save_mapping)
            self.extra_commands["create_build_conf"] = CommandDefinition(
                command=self.managers["build_manager"].
                create_build_configuration)
            self.extra_commands["update_build_conf"] = CommandDefinition(
                command=self.managers["build_manager"].
                update_build_configuration)
            self.extra_commands["delete_build_conf"] = CommandDefinition(
                command=self.managers["build_manager"].
                delete_build_configuration)
        if self.managers.get("diff_manager"):
            self.extra_commands["dim"] = CommandDefinition(
                command=self.managers["diff_manager"], tracked=False)
            self.extra_commands["diff_info"] = CommandDefinition(
                command=self.managers["diff_manager"].diff_info, tracked=False)
            self.extra_commands["jsondiff"] = CommandDefinition(
                command=jsondiff, tracked=False)
        if self.managers.get("sync_manager"):
            self.extra_commands["sym"] = CommandDefinition(
                command=self.managers["sync_manager"], tracked=False)
        if self.managers.get("index_manager"):
            self.extra_commands["im"] = CommandDefinition(
                command=self.managers["index_manager"], tracked=False)
            self.extra_commands["index_info"] = CommandDefinition(
                command=self.managers["index_manager"].index_info,
                tracked=False)
            self.extra_commands["validate_mapping"] = CommandDefinition(
                command=self.managers["index_manager"].validate_mapping)
            self.extra_commands["update_metadata"] = CommandDefinition(
                command=self.managers["index_manager"].update_metadata)
        if self.managers.get("snapshot_manager"):
            self.extra_commands["ssm"] = CommandDefinition(
                command=self.managers["snapshot_manager"], tracked=False)
            self.extra_commands["snapshot_info"] = CommandDefinition(
                command=self.managers["snapshot_manager"].snapshot_info,
                tracked=False)
        if self.managers.get("release_manager"):
            self.extra_commands["rm"] = CommandDefinition(
                command=self.managers["release_manager"], tracked=False)
            self.extra_commands["release_info"] = CommandDefinition(
                command=self.managers["release_manager"].release_info,
                tracked=False)
            self.extra_commands["reset_synced"] = CommandDefinition(
                command=self.managers["release_manager"].reset_synced,
                tracked=True)
        if self.managers.get("inspect_manager"):
            self.extra_commands["ism"] = CommandDefinition(
                command=self.managers["inspect_manager"], tracked=False)
        if self.managers.get("api_manager"):
            self.extra_commands["api"] = CommandDefinition(
                command=self.managers["api_manager"], tracked=False)
            self.extra_commands["get_apis"] = CommandDefinition(
                command=self.managers["api_manager"].get_apis, tracked=False)
            self.extra_commands["delete_api"] = CommandDefinition(
                command=self.managers["api_manager"].delete_api)
            self.extra_commands["create_api"] = CommandDefinition(
                command=self.managers["api_manager"].create_api)
            self.extra_commands["start_api"] = CommandDefinition(
                command=self.managers["api_manager"].start_api)
            self.extra_commands["stop_api"] = self.managers[
                "api_manager"].stop_api
        if "upgrade" in self.DEFAULT_FEATURES:
            def upgrade(code_base):  # just a wrapper over dumper
                """Upgrade (git pull) repository for given code base name ("biothings_sdk" or "application")"""
                assert code_base in ("application", "biothings_sdk"), "Unknown code base '%s'" % code_base
                return self.managers["dump_manager"].dump_src("__" + code_base)
            self.commands["upgrade"] = CommandDefinition(command=upgrade)

        self.extra_commands["expose"] = self.add_api_endpoint

        logging.debug("Registered extra (private) commands: %s",
                      list(self.extra_commands.keys()))

    def add_api_endpoint(self, endpoint_name, command_name, method, **kwargs):
        """
        Add an API endpoint to expose command named "command_name"
        using HTTP method "method". **kwargs are used to specify
        more arguments for EndpointDefinition
        """
        if self.configured:
            raise Exception("API endpoint creation must be done before Hub is configured")
        from biothings.hub.api import EndpointDefinition
        endpoint = EndpointDefinition(name=command_name, method=method, **kwargs)
        self.api_endpoints[endpoint_name] = endpoint

    def configure_api_endpoints(self):
        cmdnames = list(self.commands.keys())
        if self.extra_commands:
            cmdnames.extend(list(self.extra_commands.keys()))
        from biothings.hub.api import EndpointDefinition
        self.api_endpoints["config"] = []
        if "config" in cmdnames:
            self.api_endpoints["config"].append(
                EndpointDefinition(name="config", method="get"))
            self.api_endpoints["config"].append(
                EndpointDefinition(name="setconf",
                                   method="put",
                                   force_bodyargs=True))
            self.api_endpoints["config"].append(
                EndpointDefinition(name="resetconf",
                                   method="delete",
                                   force_bodyargs=True))
        if not self.api_endpoints["config"]:
            self.api_endpoints.pop("config")
        if "builds" in cmdnames:
            self.api_endpoints["builds"] = EndpointDefinition(name="builds",
                                                              method="get")
        self.api_endpoints["build"] = []
        if "build" in cmdnames:
            self.api_endpoints["build"].append(
                EndpointDefinition(method="get", name="build"))
        if "archive" in cmdnames:
            self.api_endpoints["build"].append(
                EndpointDefinition(method="post",
                                   name="archive",
                                   suffix="archive"))
        if "rmmerge" in cmdnames:
            self.api_endpoints["build"].append(
                EndpointDefinition(method="delete", name="rmmerge"))
        if "merge" in cmdnames:
            self.api_endpoints["build"].append(
                EndpointDefinition(name="merge", method="put", suffix="new"))
        if "build_save_mapping" in cmdnames:
            self.api_endpoints["build"].append(
                EndpointDefinition(name="build_save_mapping",
                                   method="put",
                                   suffix="mapping"))
        if not self.api_endpoints["build"]:
            self.api_endpoints.pop("build")
        self.api_endpoints["publish"] = []
        if "publish_diff" in cmdnames:
            self.api_endpoints["publish"].append(
                EndpointDefinition(name="publish_diff",
                                   method="post",
                                   suffix="incremental",
                                   force_bodyargs=True))
        if "publish_snapshot" in cmdnames:
            self.api_endpoints["publish"].append(
                EndpointDefinition(name="publish_snapshot",
                                   method="post",
                                   suffix="full",
                                   force_bodyargs=True))
        if not self.api_endpoints["publish"]:
            self.api_endpoints.pop("publish")
        if "diff" in cmdnames:
            self.api_endpoints["diff"] = EndpointDefinition(
                name="diff", method="put", force_bodyargs=True)
        if "job_info" in cmdnames:
            self.api_endpoints["job_manager"] = EndpointDefinition(
                name="job_info", method="get")
        if "dump_info" in cmdnames:
            self.api_endpoints["dump_manager"] = EndpointDefinition(
                name="dump_info", method="get")
        if "upload_info" in cmdnames:
            self.api_endpoints["upload_manager"] = EndpointDefinition(
                name="upload_info", method="get")
        if "build_config_info" in cmdnames:
            self.api_endpoints["build_manager"] = EndpointDefinition(
                name="build_config_info", method="get")
        if "index_info" in cmdnames:
            self.api_endpoints["index_manager"] = EndpointDefinition(
                name="index_info", method="get")
        if "snapshot_info" in cmdnames:
            self.api_endpoints["snapshot_manager"] = EndpointDefinition(
                name="snapshot_info", method="get")
        if "release_info" in cmdnames:
            self.api_endpoints["release_manager"] = EndpointDefinition(
                name="release_info", method="get")
        if "reset_synced" in cmdnames:
            self.api_endpoints[
                "release_manager/reset_synced"] = EndpointDefinition(
                    name="reset_synced", method="put")
        if "diff_info" in cmdnames:
            self.api_endpoints["diff_manager"] = EndpointDefinition(
                name="diff_info", method="get")
        if "commands" in cmdnames:
            self.api_endpoints["commands"] = EndpointDefinition(
                name="commands", method="get")
        if "command" in cmdnames:
            self.api_endpoints["command"] = EndpointDefinition(name="command",
                                                               method="get")
        if "sources" in cmdnames:
            self.api_endpoints["sources"] = EndpointDefinition(name="sources",
                                                               method="get")
        self.api_endpoints["source"] = []
        if "source_info" in cmdnames:
            self.api_endpoints["source"].append(
                EndpointDefinition(name="source_info", method="get"))
        if "source_reset" in cmdnames:
            self.api_endpoints["source"].append(
                EndpointDefinition(name="source_reset",
                                   method="post",
                                   suffix="reset"))
        if "dump" in cmdnames:
            self.api_endpoints["source"].append(
                EndpointDefinition(name="dump", method="put", suffix="dump"))
        if "upload" in cmdnames:
            self.api_endpoints["source"].append(
                EndpointDefinition(name="upload",
                                   method="put",
                                   suffix="upload"))
        if "source_save_mapping" in cmdnames:
            self.api_endpoints["source"].append(
                EndpointDefinition(name="source_save_mapping",
                                   method="put",
                                   suffix="mapping"))
        if not self.api_endpoints["source"]:
            self.api_endpoints.pop("source")
        if "inspect" in cmdnames:
            self.api_endpoints["inspect"] = EndpointDefinition(
                name="inspect", method="put", force_bodyargs=True)
        if "register_url" in cmdnames:
            self.api_endpoints["dataplugin/register_url"] = EndpointDefinition(
                name="register_url", method="post", force_bodyargs=True)
        if "unregister_url" in cmdnames:
            self.api_endpoints[
                "dataplugin/unregister_url"] = EndpointDefinition(
                    name="unregister_url",
                    method="delete",
                    force_bodyargs=True)
        self.api_endpoints["dataplugin"] = []
        if "dump_plugin" in cmdnames:
            self.api_endpoints["dataplugin"].append(
                EndpointDefinition(name="dump_plugin",
                                   method="put",
                                   suffix="dump"))
        if "export_plugin" in cmdnames:
            self.api_endpoints["dataplugin"].append(
                EndpointDefinition(name="export_plugin",
                                   method="put",
                                   suffix="export"))
        if not self.api_endpoints["dataplugin"]:
            self.api_endpoints.pop("dataplugin")
        if "jsondiff" in cmdnames:
            self.api_endpoints["jsondiff"] = EndpointDefinition(
                name="jsondiff", method="post", force_bodyargs=True)
        if "validate_mapping" in cmdnames:
            self.api_endpoints["mapping/validate"] = EndpointDefinition(
                name="validate_mapping", method="post", force_bodyargs=True)
        self.api_endpoints["buildconf"] = []
        if "create_build_conf" in cmdnames:
            self.api_endpoints["buildconf"].append(
                EndpointDefinition(name="create_build_conf",
                                   method="post",
                                   force_bodyargs=True))
            self.api_endpoints["buildconf"].append(
                EndpointDefinition(name="update_build_conf",
                                   method="put",
                                   force_bodyargs=True))
        if "delete_build_conf" in cmdnames:
            self.api_endpoints["buildconf"].append(
                EndpointDefinition(name="delete_build_conf",
                                   method="delete",
                                   force_bodyargs=True))
        if not self.api_endpoints["buildconf"]:
            self.api_endpoints.pop("buildconf")
        if "index" in cmdnames:
            self.api_endpoints["index"] = EndpointDefinition(
                name="index", method="put", force_bodyargs=True)
        if "snapshot" in cmdnames:
            self.api_endpoints["snapshot"] = EndpointDefinition(
                name="snapshot", method="put", force_bodyargs=True)
        if "sync" in cmdnames:
            self.api_endpoints["sync"] = EndpointDefinition(
                name="sync", method="post", force_bodyargs=True)
        if "whatsnew" in cmdnames:
            self.api_endpoints["whatsnew"] = EndpointDefinition(
                name="whatsnew", method="get")
        if "status" in cmdnames:
            self.api_endpoints["status"] = EndpointDefinition(name="status",
                                                              method="get")
        self.api_endpoints["release_note"] = []
        if "create_release_note" in cmdnames:
            self.api_endpoints["release_note"].append(
                EndpointDefinition(name="create_release_note",
                                   method="put",
                                   suffix="create",
                                   force_bodyargs=True))
        if "get_release_note" in cmdnames:
            self.api_endpoints["release_note"].append(
                EndpointDefinition(name="get_release_note",
                                   method="get",
                                   force_bodyargs=True))
        if not self.api_endpoints["release_note"]:
            self.api_endpoints.pop("release_note")
        self.api_endpoints["api"] = []
        if "start_api" in cmdnames:
            self.api_endpoints["api"].append(
                EndpointDefinition(name="start_api",
                                   method="put",
                                   suffix="start"))
        if "stop_api" in cmdnames:
            self.api_endpoints["api"].append(
                EndpointDefinition(name="stop_api",
                                   method="put",
                                   suffix="stop"))
        if "delete_api" in cmdnames:
            self.api_endpoints["api"].append(
                EndpointDefinition(name="delete_api",
                                   method="delete",
                                   force_bodyargs=True))
        if "create_api" in cmdnames:
            self.api_endpoints["api"].append(
                EndpointDefinition(name="create_api",
                                   method="post",
                                   force_bodyargs=True))
        if not self.api_endpoints["api"]:
            self.api_endpoints.pop("api")
        if "get_apis" in cmdnames:
            self.api_endpoints["api/list"] = EndpointDefinition(
                name="get_apis", method="get")
        if "stop" in cmdnames:
            self.api_endpoints["stop"] = EndpointDefinition(name="stop",
                                                            method="put")
        if "restart" in cmdnames:
            self.api_endpoints["restart"] = EndpointDefinition(name="restart",
                                                               method="put")
        self.api_endpoints["standalone"] = []
        if "list" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="list", method="get", suffix="list"))
        if "versions" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="versions", method="get", suffix="versions"))
        if "check" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="check", method="get", suffix="check"))
        if "info" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="info", method="get", suffix="info"))
        if "download" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="download", method="post", suffix="download"))
        if "apply" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="apply", method="post", suffix="apply"))
        if "install" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="install", method="post", suffix="install"))
        if "backend" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="backend", method="get", suffix="backend"))
        if "reset_backend" in cmdnames:
            self.api_endpoints["standalone"].append(EndpointDefinition(name="reset_backend", method="delete", suffix="backend"))
        if not self.api_endpoints["standalone"]:
            self.api_endpoints.pop("standalone")
        if "upgrade" in self.commands:
            self.api_endpoints["code/upgrade"] = EndpointDefinition(name="upgrade", method="put")

    def export_command_documents(self, filepath):
        generate_command_documentations(filepath, self.commands)


class HubSSHServer(asyncssh.SSHServer):

    PASSWORDS = {}
    SHELL = None

    def session_requested(self):
        return HubSSHServerSession(self.__class__.NAME, self.__class__.SHELL)

    def connection_made(self, connection):
        self._conn = connection
        print('SSH connection received from %s.' %
              connection.get_extra_info('peername')[0])

    def connection_lost(self, exc):
        if exc:
            print('SSH connection error: ' + str(exc), file=sys.stderr)
        else:
            print('SSH connection closed.')

    def begin_auth(self, username):
        try:
            self._conn.set_authorized_keys('bin/authorized_keys/%s.pub' %
                                           username)
        except IOError:
            pass
        return True

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        import crypt  # not available on windows
        if self.password_auth_supported():
            pw = self.__class__.PASSWORDS.get(username, '*')
            return crypt.crypt(password, pw) == pw
        else:
            return False


class HubSSHServerSession(asyncssh.SSHServerSession):
    def __init__(self, name, shell):
        self.name = name
        self.shell = shell
        self._input = ''

    def connection_made(self, chan):
        self._chan = chan

    def shell_requested(self):
        return True

    def exec_requested(self, command):
        self.eval_lines(["%s" % command, "\n"])
        return True

    def session_started(self):
        welcome = ('\nWelcome to %s, %s!\n' %
                   (self.name, self._chan.get_extra_info('username')))
        self.shell.shellog.output(welcome)
        self._chan.write(welcome)
        prompt = 'hub> '
        self.shell.shellog.output(prompt)
        self._chan.write(prompt)

    def data_received(self, data, datatype):
        self._input += data
        return self.eval_lines(self._input.split('\n'))

    def eval_lines(self, lines):
        for line in lines[:-1]:
            try:
                outs = [out for out in self.shell.eval(line) if out]

                # Prepend the standout out/err 
                last_std_contents = self.shell.last_std_contents or {}
                if "stdout" in last_std_contents:
                    outs.append(last_std_contents["stdout"])
                if "stderr" in last_std_contents:
                    outs.append(last_std_contents["stderr"])

                # trailing \n if not already there
                if outs:
                    strout = "\n".join(outs).strip("\n") + "\n"
                    self._chan.write(strout)
                    self.shell.shellog.output(strout)
            except AlreadyRunningException as e:
                self._chan.write("AlreadyRunningException: %s" % e)
            except CommandError as e:
                self._chan.write("CommandError: %s" % e)
        self._chan.write('hub> ')
        # consume passed commands
        self._input = lines[-1]

    def eof_received(self):
        self._chan.write('Have a good one...\n')
        self._chan.exit(0)

    def break_received(self, msec):
        # simulate CR
        self._chan.write('\n')
        self.data_received("\n", None)
