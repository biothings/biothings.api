from pprint import pprint, pformat
import asyncio, asyncssh, crypt, aiocron, os, sys, types, time
from collections import OrderedDict
from functools import partial
import logging

from biothings import config
from biothings.utils.loggers import get_logger
from biothings.utils.hub import HubShell, HubReloader, CommandDefinition, pending, \
                                AlreadyRunningException, CommandError
from biothings.utils.jsondiff import make as jsondiff

# Keys used as category in pinfo (description of jobs submitted to JobManager)
# Those are used in different places
DUMPER_CATEGORY = "dumper"
UPLOADER_CATEGORY = "uploader"
BUILDER_CATEGORY = "builder"
INDEXER_CATEGORY = "indexer"
INDEXMANAGER_CATEGORY = "indexmanager"
DIFFER_CATEGORY = "differ"
DIFFMANAGER_CATEGORY = "diffmanager"
SYNCER_CATEGORY = "syncer"
INSPECTOR_CATEGORY = "inspector"

HUB_REFRESH_COMMANDS = hasattr(config,"HUB_REFRESH_COMMANDS") and config.HUB_REFRESH_COMMANDS or "* * * * * *" # every sec

__version__ = "0.2"


class JobRenderer(object):

    def __init__(self):
        self.rendered = {
                types.FunctionType : self.render_func,
                types.MethodType : self.render_method,
                partial : self.render_partial,
                types.LambdaType: self.render_lambda,
        }

    def render(self,job):
        r = self.rendered.get(type(job._callback))
        rstr = r(job._callback)
        delta = job._when - job._loop.time()
        days = None
        if delta > 86400:
            days = int(delta/86400)
            delta = delta - 86400
        strdelta = time.strftime("%Hh:%Mm:%Ss", time.gmtime(int(delta)))
        if days:
            strdelta = "%d day(s) %s" % (days,strdelta)
        return "%s {run in %s}" % (rstr,strdelta)

    def render_partial(self,p):
        # class.method(args)
        return self.rendered[type(p.func)](p.func) + "%s" % str(p.args)

    def render_cron(self,c):
        # func type associated to cron can vary
        return self.rendered[type(c.func)](c.func) + " [%s]" % c.spec

    def render_func(self,f):
        return f.__name__

    def render_method(self,m):
        # what is self ? cron ?
        if type(m.__self__) == aiocron.Cron:
            return self.render_cron(m.__self__)
        else:
            return "%s.%s" % (m.__self__.__class__.__name__,
                              m.__name__)

    def render_lambda(self,l):
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
            total_docs = sum([s["upload"]["sources"][subs].get("count",0) or 0 \
                            for s in srcs
                            for subs in s.get("upload",{}).get("sources",{}) \
                            if s.get("upload")])
        except Exception as e:
            logging.error("Can't get stats for sources: %s" % e)

    try:
        bm = managers["build_manager"]
        total_confs = len(bm.build_config_info())
    except Exception as e:
        logging.error("Can't get total number of build configurations: %s" % e)
    try:
        total_builds = len(bm.build_info())
    except Exception as e:
        logging.error("Can't get total number of builds: %s" % e)

    try:
        am = managers["api_manager"]
        apis = am.get_apis()
        total_apis = len(apis)
        total_running_apis = len([a for a in apis if a.get("status") == "running"])
    except Exception as e:
        logging.error("Can't get stats for APIs: %s" % e)

    return {
            "source" : {"total" : total_srcs,
                        "documents" : total_docs},
            "build" : {"total" : total_builds},
            "build_conf" : {"total" : total_confs},
            "api" : {"total" : total_apis,
                     "running" : total_running_apis},
            }


def schedule(loop):
    jobs = {}
    # try to render job in a human-readable way...
    out = []
    for sch in loop._scheduled:
        if type(sch) != asyncio.events.TimerHandle:
            continue
        if sch._cancelled:
            continue
        try:
            info = renderer.render(sch)
            out.append(info)
        except Exception as e:
            import traceback
            traceback.print_exc()
            out.append(sch)

    return "\n".join(out)


@asyncio.coroutine
def start_ssh_server(loop,name,passwords,keys=['bin/ssh_host_key'],shell=None,
                 host='',port=8022):
    for key in keys:
        assert os.path.exists(key),"Missing key '%s' (use: 'ssh-keygen -f %s' to generate it" % (key,key)
    HubSSHServer.PASSWORDS = passwords
    HubSSHServer.NAME = name
    HubSSHServer.SHELL = shell
    cron = aiocron.crontab(HUB_REFRESH_COMMANDS,func=shell.__class__.refresh_commands,
                           start=True, loop=loop)
    yield from asyncssh.create_server(HubSSHServer, host, port, loop=loop,
                                 server_host_keys=keys)


class HubServer(object):

    DEFAULT_FEATURES = ["job","dump","upload","dataplugin","source",
                        "build","diff","index","inspect","sync","api"]
    DEFAULT_MANAGERS_ARGS = {"upload" : {"poll_schedule" : "* * * * * */10"}}
    DEFAULT_RELOADER_CONFIG = {"folders": None, # will use default one
                               "managers" : ["source_manager","assistant_manager"],
                               "reload_func" : None} # will use default one
    DEFAULT_DATAUPLOAD_CONFIG = {"upload_root" : getattr(config,"DATA_UPLOAD_FOLDER",None)}
    DEFAULT_WEBSOCKET_CONFIG = {}
    DEFAULT_API_CONFIG = {}

    def __init__(self, source_list, features=None, name="BioThings Hub",
                 managers_custom_args={}, api_config=None, reloader_config=None,
                 dataupload_config=None, websocket_config=None):
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
        "reloader_config", "dataupload_config" and "websocket_config" can be used to
        customize reloader, dataupload and websocket. If None, default config is used.
        If explicitely False, feature is deactivated.
        """
        self.name = name
        self.source_list = source_list
        self.logger, self.logfile = get_logger("hub")
        self._passed_features = features
        self._passed_managers_custom_args = managers_custom_args
        self.features = features or self.DEFAULT_FEATURES
        self.managers_custom_args = managers_custom_args
        if reloader_config == False:
            self.logger.debug("Reloader deactivated")
            self.reloader_config = False
        else:
            self.reloader_config = reloader_config or self.DEFAULT_RELOADER_CONFIG
        if dataupload_config == False:
            self.logger.debug("Data upload deactivated")
            self.dataupload_config = False
        else:
            self.dataupload_config = dataupload_config or self.DEFAULT_DATAUPLOAD_CONFIG
        if websocket_config == False:
            self.logger.debug("Websocket deactivated")
            self.websocket_config = False
        else:
            self.websocket_config = websocket_config or self.DEFAULT_WEBSOCKET_CONFIG
        if api_config == False:
            self.logger.debug("API deactivated")
            self.api_config = False
        else:
            self.api_config = api_config or self.DEFAULT_API_CONFIG
        # set during configure()
        self.managers = None
        self.api_endpoints = None
        self.shell = None
        self.commands = None
        self.extra_commands = None
        self.routes = []
        # flag "do we need to configure?"
        self.configured = False

    def configure_ioloop(self):
        import tornado.platform.asyncio
        tornado.platform.asyncio.AsyncIOMainLoop().install()

    def configure(self):
        self.configure_ioloop()
        self.configure_managers()
        self.configure_commands()
        self.configure_extra_commands()
        # setup the shell
        self.shell = HubShell(self.managers["job_manager"])
        self.shell.register_managers(self.managers)
        self.shell.set_commands(self.commands,self.extra_commands)
        self.shell.server = self # propagate server instance in shell
                                 # so it's accessible from the console if needed
        # set api
        if self.api_config != False:
            self.configure_api_endpoints() # after shell setup as it adds some default commands
                                           # we want to expose throught the api
            from biothings.hub.api import generate_api_routes
            self.routes = generate_api_routes(self.shell, self.api_endpoints)

        if self.dataupload_config != False:
            # this one is not bound to a specific command
            from biothings.hub.api.handlers.upload import UploadHandler
            # tuple type = interpreted as a route handler
            self.routes.append(("/dataupload/([\w\.-]+)?",UploadHandler,self.dataupload_config))

        if self.websocket_config != False:
            # add websocket endpoint
            import biothings.hub.api.handlers.ws as ws
            import sockjs.tornado
            from biothings.utils.hub_db import ChangeWatcher
            listener = ws.HubDBListener()
            ChangeWatcher.add(listener)
            ChangeWatcher.publish()
            self.logger.info("Starting SockJS router")
            ws_router = sockjs.tornado.SockJSRouter(partial(ws.WebSocketConnection,listener=listener), '/ws')
            self.routes.extend(ws_router.urls)

        if self.reloader_config != False:
            monitored_folders = self.reloader_config["folders"] or ["hub/dataload/sources",getattr(config,"DATA_PLUGIN_FOLDER",None)]
            reload_managers = [self.managers[m] for m in self.reloader_config["managers"] if m in self.managers]
            reload_func = self.reloader_config["reload_func"] or partial(self.shell.restart,force=True)
            reloader = HubReloader(monitored_folders, reload_managers, reload_func=reload_func)
            reloader.monitor()

        # done
        self.configured = True

    def before_start(self):
        pass

    def start(self):
        if not self.configured:
            self.configure()
        self.logger.info("Starting server '%s'" % self.name)
        # can't use asyncio.get_event_loop() if python < 3.5.3 as it would return
        # another instance of aio loop, take it from job_manager to make sure
        # we share the same one
        loop = self.managers["job_manager"].loop

        if self.routes:
            self.logger.info("Starting Hub API server")
            import tornado.web
            # register app into current event loop
            api = tornado.web.Application(self.routes)
            self.extra_commands["api"] = api
            from biothings.hub.api import start_api
            api_server = start_api(api,config.HUB_API_PORT,settings=getattr(config,"TORNADO_SETTINGS",{}))
        else:
            self.logger.info("No route defined, API server won't start")

        # at this point, everything is ready/set, last call for customizations
        self.before_start()

        self.ssh_server = start_ssh_server(loop,self.name,passwords=config.HUB_PASSWD,
                              port=config.HUB_SSH_PORT,shell=self.shell)
        try:
            loop.run_until_complete(self.ssh_server)
        except (OSError, asyncssh.Error) as exc:
            sys.exit('Error starting server: ' + str(exc))
        loop.run_forever()

    def mixargs(self, feat, params={}):
        args = {}
        for p in params:
            args[p] = self.managers_custom_args.get(feat,{}).pop(p,None) or params[p]
        # mix remaining
        args.update(self.managers_custom_args.get(feat,{}))
        return args

    def configure_job_manager(self):
        import asyncio
        loop = asyncio.get_event_loop()
        from biothings.utils.manager import JobManager
        args = self.mixargs("job",{"num_workers":config.HUB_MAX_WORKERS,"max_memory_usage":config.HUB_MAX_MEM_USAGE})
        job_manager = JobManager(loop,**args)
        self.managers["job_manager"] = job_manager

    def configure_dump_manager(self):
        from biothings.hub.dataload.dumper import DumperManager
        args = self.mixargs("dump")
        dmanager = DumperManager(job_manager=self.managers["job_manager"],**args)
        self.managers["dump_manager"] = dmanager

    def configure_upload_manager(self):
        from biothings.hub.dataload.uploader import UploaderManager
        args = self.mixargs("upload",{"poll_schedule":"* * * * * */10"})
        upload_manager = UploaderManager(job_manager=self.managers["job_manager"],**args)
        self.managers["upload_manager"] = upload_manager

    def configure_dataplugin_manager(self):
        from biothings.hub.dataplugin.manager import DataPluginManager
        dp_manager = DataPluginManager(job_manager=self.managers["job_manager"])
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
        build_manager = BuilderManager(job_manager=self.managers["job_manager"],**args)
        build_manager.configure()
        self.managers["build_manager"] = build_manager

    def configure_diff_manager(self):
        from biothings.hub.databuild.differ import DifferManager, SelfContainedJsonDiffer
        args = self.mixargs("diff")
        diff_manager = DifferManager(job_manager=self.managers["job_manager"],
                                     poll_schedule="* * * * * */10",**args)
        diff_manager.configure([SelfContainedJsonDiffer,])
        diff_manager.poll("diff",lambda doc: diff_manager.diff("jsondiff-selfcontained",old=None,new=doc["_id"]))
        diff_manager.poll("release_note",lambda doc: diff_manager.release_note(old=None,new=doc["_id"]))
        self.managers["diff_manager"] = diff_manager

    def configure_index_manager(self):
        from biothings.hub.dataindex.indexer import IndexerManager
        args = self.mixargs("index")
        index_manager = IndexerManager(job_manager=self.managers["job_manager"],**args)
        index_manager.configure(config.ES_CONFIG)
        self.managers["index_manager"] = index_manager

    def configure_sync_manager(self):
        from biothings.hub.databuild.syncer import SyncerManager
        args = self.mixargs("sync")
        sync_manager = SyncerManager(job_manager=self.managers["job_manager"],**args)
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
                job_manager=self.managers["job_manager"],**args)
        self.managers["inspect_manager"] = inspect_manager

    def configure_api_manager(self):
        assert "index" in self.features, "'api' feature requires 'index'"
        from biothings.hub.api.manager import APIManager
        args = self.mixargs("api")
        api_manager = APIManager(**args)
        self.managers["api_manager"] = api_manager

    def configure_source_manager(self):
        if "dump" in self.features or "upload" in self.features:
            args = self.mixargs("source")
            from biothings.hub.dataload.source import SourceManager
            source_manager = SourceManager(
                    source_list=self.source_list,
                    dump_manager=self.managers["dump_manager"],
                    upload_manager=self.managers["upload_manager"],
                    data_plugin_manager=self.managers.get("dataplugin_manager"),
                    )
            self.managers["source_manager"] = source_manager
            # now that we have the source manager setup, we can schedule and poll
            if "dump" in self.features and not getattr(config,"SKIP_DUMPER_SCHEDULE",False):
                self.managers["dump_manager"].schedule_all()
            if "upload" in self.features and not getattr(config,"SKIP_UPLOADER_POLL",False):
                self.managers["upload_manager"].poll('upload',lambda doc:
                        self.shell.launch(partial(self.managers["upload_manager"].upload_src,doc["_id"])))
        # init data plugin once source_manager has been set (it inits dumper and uploader
        # managers, if assistant_manager is configured/loaded before, datasources won't appear
        # in dumper/uploader managers as they were not ready yet)
        if "dataplugin" in self.features:
            self.managers["assistant_manager"].configure()
            self.managers["assistant_manager"].load()

    def configure_managers(self):

        if not self.managers is None:
            raise Exception("Managers have already been configured")
        self.managers = {}

        self.logger.info("Setting up managers for following features: %s" % self.features)
        assert "job" in self.features, "'job' feature is mandatory"
        if "source" in self.features:
            assert "dump" in self.features and "upload" in self.features, \
                    "'source' feature requires both 'dump' and 'upload' features"
        if "dataplugin" in self.features:
            assert "source" in self.features, "'dataplugin' feature requires 'source' feature"

        # specific order, eg. job_manager is used by all managers
        for feat in self.features:
            if hasattr(self,"configure_%s_manager" % feat):
                getattr(self,"configure_%s_manager" % feat)()
            else:
                raise AttributeError("Feature '%s' listed but no 'configure_%s_manager' method found" % (feat,feat))

        self.logger.info("Active manager(s): %s" % pformat(self.managers))

    def configure_commands(self):
        """
        Configure hub commands according to available managers
        """
        assert self.managers, "No managers configured"
        self.commands = OrderedDict()
        self.commands["status"] = CommandDefinition(command=partial(status,self.managers),tracked=False)
        # getting info
        if self.managers.get("source_manager"):
            self.commands["source_info"] = CommandDefinition(command=self.managers["source_manager"].get_source,tracked=False)
        # dump commands
        if self.managers.get("dump_manager"):
            self.commands["dump"] = self.managers["dump_manager"].dump_src
            self.commands["dump_all"] = self.managers["dump_manager"].dump_all
        # upload commands
        if self.managers.get("upload_manager"):
            self.commands["upload"] = self.managers["upload_manager"].upload_src
            self.commands["upload_all"] = self.managers["upload_manager"].upload_all
        # building/merging
        if self.managers.get("build_manager"):
            self.commands["whatsnew"] = CommandDefinition(command=self.managers["build_manager"].whatsnew,tracked=False)
            self.commands["lsmerge"] = self.managers["build_manager"].list_merge
            self.commands["rmmerge"] = self.managers["build_manager"].delete_merge
            self.commands["merge"] = self.managers["build_manager"].merge
            self.commands["archive"] = self.managers["build_manager"].archive_merge
        if hasattr(config,"ES_CONFIG"):
            self.commands["es_config"] = config.ES_CONFIG
        # diff
        if self.managers.get("diff_manager"):
            self.commands["diff"] = self.managers["diff_manager"].diff
            self.commands["report"] = self.managers["diff_manager"].diff_report
            self.commands["release_note"] = self.managers["diff_manager"].release_note
            self.commands["publish_diff"] = self.managers["diff_manager"].publish_diff
        # indexing commands
        if self.managers.get("index_manager"):
            self.commands["index"] = self.managers["index_manager"].index
            self.commands["snapshot"] = self.managers["index_manager"].snapshot
            self.commands["publish_snapshot"] = self.managers["index_manager"].publish_snapshot
        if self.managers.get("sync_manager"):
            self.commands["sync"] = CommandDefinition(command=self.managers["sync_manager"].sync)
        # inspector
        if self.managers.get("inspect_manager"):
            self.commands["inspect"] = self.managers["inspect_manager"].inspect
        # data plugins
        if self.managers.get("assistant_manager"):
            self.commands["register_url"] = partial(self.managers["assistant_manager"].register_url)
            self.commands["unregister_url"] = partial(self.managers["assistant_manager"].unregister_url)
        if self.managers.get("dataplugin_manager"):
            self.commands["dump_plugin"] = self.managers["dataplugin_manager"].dump_src

        logging.info("Registered commands: %s" % list(self.commands.keys()))

    def configure_extra_commands(self):
        """
        Same as configure_commands() but commands are not exposed publicly in the shell
        (they are shortcuts or commands for API endpoints, supporting commands, etc...)
        """
        assert self.managers, "No managers configured"
        self.extra_commands = {} # unordered since not exposed, we don't care
        loop = self.managers.get("job_manager") and self.managers["job_manager"].loop or asyncio.get_event_loop()
        self.extra_commands["g"] = CommandDefinition(command=globals(),tracked=False)
        self.extra_commands["sch"] = CommandDefinition(command=partial(schedule,loop),tracked=False)
        # expose contant so no need to put quotes (eg. top(pending) instead of top("pending")
        self.extra_commands["pending"] = CommandDefinition(command=pending,tracked=False)
        self.extra_commands["loop"] = CommandDefinition(command=loop,tracked=False)

        if self.managers.get("source_manager"):
            self.extra_commands["sources"] = CommandDefinition(command=self.managers["source_manager"].get_sources,tracked=False)
            self.extra_commands["source_save_mapping"] = CommandDefinition(command=self.managers["source_manager"].save_mapping)
        if self.managers.get("dump_manager"):
            self.extra_commands["dm"] = CommandDefinition(command=self.managers["dump_manager"],tracked=False)
            self.extra_commands["dump_info"] = CommandDefinition(command=self.managers["dump_manager"].dump_info,tracked=False)
        if self.managers.get("dataplugin_manager"):
            self.extra_commands["dpm"] = CommandDefinition(command=self.managers["dataplugin_manager"],tracked=False)
        if self.managers.get("assistant_manager"):
            self.extra_commands["am"] = CommandDefinition(command=self.managers["assistant_manager"],tracked=False)
        if self.managers.get("upload_manager"):
            self.extra_commands["um"] = CommandDefinition(command=self.managers["upload_manager"],tracked=False)
            self.extra_commands["upload_info"] = CommandDefinition(command=self.managers["upload_manager"].upload_info,tracked=False)
        if self.managers.get("build_manager"):
            self.extra_commands["bm"] = CommandDefinition(command=self.managers["build_manager"],tracked=False)
            self.extra_commands["builds"] = CommandDefinition(command=self.managers["build_manager"].build_info,tracked=False)
            self.extra_commands["build"] = CommandDefinition(command=lambda id: self.managers["build_manager"].build_info(id=id),tracked=False)
            self.extra_commands["build_config_info"] = CommandDefinition(command=self.managers["build_manager"].build_config_info,tracked=False)
            self.extra_commands["build_save_mapping"] = CommandDefinition(command=self.managers["build_manager"].save_mapping)
            self.extra_commands["create_build_conf"] = CommandDefinition(command=self.managers["build_manager"].create_build_configuration)
            self.extra_commands["update_build_conf"] = CommandDefinition(command=self.managers["build_manager"].update_build_configuration)
            self.extra_commands["delete_build_conf"] = CommandDefinition(command=self.managers["build_manager"].delete_build_configuration)
        if self.managers.get("diff_manager"):
            self.extra_commands["dim"] = CommandDefinition(command=self.managers["diff_manager"],tracked=False)
            self.extra_commands["diff_info"] = CommandDefinition(command=self.managers["diff_manager"].diff_info,tracked=False)
            self.extra_commands["jsondiff"] = CommandDefinition(command=jsondiff,tracked=False)
        if self.managers.get("sync_manager"):
            self.extra_commands["sm"] = CommandDefinition(command=self.managers["sync_manager"],tracked=False)
        if self.managers.get("index_manager"):
            self.extra_commands["im"] = CommandDefinition(command=self.managers["index_manager"],tracked=False)
            self.extra_commands["index_info"] = CommandDefinition(command=self.managers["index_manager"].index_info,tracked=False)
            self.extra_commands["validate_mapping"] = CommandDefinition(command=self.managers["index_manager"].validate_mapping)
            self.extra_commands["pqueue"] = CommandDefinition(command=self.managers["job_manager"].process_queue,tracked=False)
            self.extra_commands["tqueue"] = CommandDefinition(command=self.managers["job_manager"].thread_queue,tracked=False)
            self.extra_commands["jm"] = CommandDefinition(command=self.managers["job_manager"],tracked=False)
            self.extra_commands["top"] = CommandDefinition(command=self.managers["job_manager"].top,tracked=False)
            self.extra_commands["job_info"] = CommandDefinition(command=self.managers["job_manager"].job_info,tracked=False)
        if self.managers.get("inspect_manager"):
            self.extra_commands["ism"] = CommandDefinition(command=self.managers["inspect_manager"],tracked=False)
        if self.managers.get("api_manager"):
            self.extra_commands["api"] = CommandDefinition(command=self.managers["api_manager"],tracked=False)
            self.extra_commands["get_apis"] = CommandDefinition(command=self.managers["api_manager"].get_apis,tracked=False)
            self.extra_commands["delete_api"] = CommandDefinition(command=self.managers["api_manager"].delete_api)
            self.extra_commands["create_api"] = CommandDefinition(command=self.managers["api_manager"].create_api)
            self.extra_commands["start_api"] = CommandDefinition(command=self.managers["api_manager"].start_api)
            self.extra_commands["stop_api"] = self.managers["api_manager"].stop_api

        logging.debug("Registered extra (private) commands: %s" % list(self.extra_commands.keys()))

    def configure_api_endpoints(self):
        cmdnames = list(self.commands.keys())
        if self.extra_commands:
            cmdnames.extend(list(self.extra_commands.keys()))
        from biothings.hub.api import EndpointDefinition
        self.api_endpoints = {}
        if "builds" in cmdnames: self.api_endpoints["builds"] = EndpointDefinition(name="builds",method="get")
        self.api_endpoints["build"] = []
        if "build" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(method="get",name="build"))
        if "archive" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(method="post",name="archive",suffix="archive"))
        if "rmmerge" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(method="delete",name="rmmerge"))
        if "merge" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(name="merge",method="put",suffix="new"))
        if "build_save_mapping" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(name="build_save_mapping",method="put",suffix="mapping"))
        if not self.api_endpoints["build"]:
            self.api_endpoints.pop("build")
        if "diff" in cmdnames: self.api_endpoints["diff"] = EndpointDefinition(name="diff",method="put",force_bodyargs=True)
        if "job_info" in cmdnames: self.api_endpoints["job_manager"] = EndpointDefinition(name="job_info",method="get")
        if "dump_info" in cmdnames: self.api_endpoints["dump_manager"] = EndpointDefinition(name="dump_info", method="get")
        if "upload_info" in cmdnames: self.api_endpoints["upload_manager"] = EndpointDefinition(name="upload_info",method="get")
        if "build_config_info" in cmdnames: self.api_endpoints["build_manager"] = EndpointDefinition(name="build_config_info",method="get")
        if "index_info" in cmdnames: self.api_endpoints["index_manager"] = EndpointDefinition(name="index_info",method="get")
        if "diff_info" in cmdnames: self.api_endpoints["diff_manager"] = EndpointDefinition(name="diff_info",method="get")
        if "commands" in cmdnames: self.api_endpoints["commands"] = EndpointDefinition(name="commands",method="get")
        if "command" in cmdnames: self.api_endpoints["command"] = EndpointDefinition(name="command",method="get")
        if "sources" in cmdnames: self.api_endpoints["sources"] = EndpointDefinition(name="sources",method="get")
        self.api_endpoints["source"] = []
        if "source_info" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="source_info",method="get"))
        if "dump" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="dump",method="put",suffix="dump"))
        if "upload" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="upload",method="put",suffix="upload"))
        if "source_save_mapping" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="source_save_mapping",method="put",suffix="mapping"))
        if not self.api_endpoints["source"]:
            self.api_endpoints.pop("source")
        if "inspect" in cmdnames: self.api_endpoints["inspect"] = EndpointDefinition(name="inspect",method="put",force_bodyargs=True)
        if "register_url" in cmdnames: self.api_endpoints["dataplugin/register_url"] = EndpointDefinition(name="register_url",method="post",force_bodyargs=True)
        if "unregister_url" in cmdnames: self.api_endpoints["dataplugin/unregister_url"] = EndpointDefinition(name="unregister_url",method="delete",force_bodyargs=True)
        if "dump_plugin" in cmdnames: self.api_endpoints["dataplugin"] = [EndpointDefinition(name="dump_plugin",method="put",suffix="dump")]
        if "jsondiff" in cmdnames: self.api_endpoints["jsondiff"] = EndpointDefinition(name="jsondiff",method="post",force_bodyargs=True)
        if "validate_mapping" in cmdnames: self.api_endpoints["mapping/validate"] = EndpointDefinition(name="validate_mapping",method="post",force_bodyargs=True)
        self.api_endpoints["buildconf"] = []
        if "create_build_conf" in cmdnames:
            self.api_endpoints["buildconf"].append(EndpointDefinition(name="create_build_conf",method="post",force_bodyargs=True))
            self.api_endpoints["buildconf"].append(EndpointDefinition(name="update_build_conf",method="put",force_bodyargs=True))
        if "delete_build_conf" in cmdnames: self.api_endpoints["buildconf"].append(EndpointDefinition(name="delete_build_conf",method="delete",force_bodyargs=True))
        if not self.api_endpoints["buildconf"]:
            self.api_endpoints.pop("buildconf")
        if "index" in cmdnames: self.api_endpoints["index"] = EndpointDefinition(name="index",method="put",force_bodyargs=True)
        if "sync" in cmdnames: self.api_endpoints["sync"] = EndpointDefinition(name="sync",method="post",force_bodyargs=True)
        if "whatsnew" in cmdnames: self.api_endpoints["whatsnew"] = EndpointDefinition(name="whatsnew",method="get")
        if "status" in cmdnames: self.api_endpoints["status"] = EndpointDefinition(name="status",method="get")
        self.api_endpoints["api"] = []
        if "start_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="start_api",method="put",suffix="start"))
        if "stop_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="stop_api",method="put",suffix="stop"))
        if "delete_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="delete_api",method="delete",force_bodyargs=True))
        if "create_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="create_api",method="post",force_bodyargs=True))
        if not self.api_endpoints["api"]:
            self.api_endpoints.pop("api")
        if "get_apis" in cmdnames: self.api_endpoints["api/list"] = EndpointDefinition(name="get_apis",method="get")
        if "stop" in cmdnames: self.api_endpoints["stop"] = EndpointDefinition(name="stop",method="put")
        if "restart" in cmdnames: self.api_endpoints["restart"] = EndpointDefinition(name="restart",method="put")


class HubSSHServer(asyncssh.SSHServer):

    COMMANDS = OrderedDict() # public hub commands
    EXTRA_NS = {} # extra commands, kind-of of hidden/private
    PASSWORDS = {}
    SHELL = None

    def session_requested(self):
        return HubSSHServerSession(self.__class__.NAME,self.__class__.SHELL)

    def connection_made(self, conn):
         self._conn = conn
         print('SSH connection received from %s.' %
         conn.get_extra_info('peername')[0])

    def connection_lost(self, exc):
        if exc:
            print('SSH connection error: ' + str(exc), file=sys.stderr)
        else:
            print('SSH connection closed.')

    def begin_auth(self, username):
        try:
            self._conn.set_authorized_keys('bin/authorized_keys/%s.pub' % username)
        except IOError:
            pass
        return True

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
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

    def exec_requested(self,command):
        self.eval_lines(["%s" % command,"\n"])
        return True

    def session_started(self):
        self._chan.write('\nWelcome to %s, %s!\n' % (self.name,self._chan.get_extra_info('username')))
        self._chan.write('hub> ')

    def data_received(self, data, datatype):
        self._input += data
        return self.eval_lines(self._input.split('\n'))

    def eval_lines(self, lines):
        for line in lines[:-1]:
            try:
                outs = [out for out in self.shell.eval(line) if out]
                # trailing \n if not already there
                if outs:
                    self._chan.write("\n".join(outs).strip("\n") + "\n")
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
        self.data_received("\n",None)

