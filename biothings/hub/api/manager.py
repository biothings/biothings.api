import os
import socket
import types
from datetime import datetime

from biothings import config as btconfig
from biothings.utils.hub_db import get_api
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.web.launcher import BiothingsAPILauncher


class APIManagerException(Exception):
    pass


class APIManager(BaseManager):
    def __init__(self, log_folder=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = get_api()
        self.register = {}
        self.timestamp = datetime.now()
        self.log_folder = log_folder or btconfig.LOG_FOLDER
        self.setup()
        self.restore_running_apis()

    def setup(self):
        self.setup_log()

    def setup_log(self):
        self.logger, _ = get_logger("apimanager")

    def restore_running_apis(self):
        """
        If some APIs were running but the hub stopped, re-start APIs
        as hub restarts
        """
        apis = self.get_apis()
        # these were running but had to stop when hub stopped
        running_apis = [api for api in apis if api.get("status") == "running"]
        for api in running_apis:
            self.logger.info("Restart API '%s'" % api["_id"])
            self.start_api(api["_id"])

    def register_status(self, api_id, status, **extra):
        apidoc = self.api.find_one({"_id": api_id})
        apidoc.update(extra)
        # clean according to status
        if status == "running":
            apidoc.pop("err", None)
        else:
            apidoc.pop("url", None)
        apidoc["status"] = status
        self.api.save(apidoc)

    def get_apis(self):
        return [d for d in self.api.find()]

    def test_variables(self):
        has_pytests = False
        try:
            import config_web_local as config_mod
            has_pytests = True
            self.logger.info(f"**** THIS IS A TESTING IF CONFIG_WEB_LOCAL IS IMPORTED {config_mod.ES_HOST}")
            self.logger.info(f"**** THIS IS THE CURRENT WORKING DIR {os.getcwd()}")
        except ImportError:
            self.logger.info("**** CANNOT IMPORT CONFIG_WEB")
            config_mod = types.ModuleType("config_mod")
        finally:
            return config_mod, has_pytests


    async def test_api(self, api_id):
        assert self.job_manager
        apidoc = self.api.find_one({"_id": api_id})
        config_mod, has_pytests = self.test_variables()
        self.logger.info(f"THESE ARE ALL THE VARIABLES {dir(config_mod)}")
        # config_mod = types.ModuleType("config_mod")
        config_mod.ES_HOST = apidoc["config"]["es_host"]
        config_mod.ES_INDEX = apidoc["config"]["index"]
        config_mod.ES_DOC_TYPE = apidoc["config"]["doc_type"]
        self.logger.info(f"CHECK IF ESHOST IS CHANGED {config_mod.ES_HOST}")
        port = int(apidoc["config"]["port"])

        # import pytests if has_pytest is true then run the pytests
        if has_pytests:
            self.logger.info("**** RUNNING PYTESTS ****")
            import pytest
            PYTEST_PATH = os.path.join(os.getcwd(), config_mod.PYTEST_PATH)
            job = job_manager.defer_to_process(pinfo, partial(pytest.main, [PYTEST_PATH, "-m", "not userquery", "--host", "mygene.info"]))
            # pytest.main([PYTEST_PATH, "-m", "not userquery", "--host", str(port)])
            self.logger.info("**** PYTESTS FINISHED ****")
            await job

    def start_api(self, api_id):
        apidoc = self.api.find_one({"_id": api_id})
        if not apidoc:
            raise APIManagerException("No such API with ID '%s'" % api_id)
        if "entry_point" in apidoc:
            raise NotImplementedError(
                "Custom entry point not implemented yet, " + "only basic generated APIs are currently supported"
            )

        config_mod, has_pytests = self.test_variables()
        self.logger.info(f"THESE ARE ALL THE VARIABLES {dir(config_mod)}")
        # config_mod = types.ModuleType("config_mod")
        config_mod.ES_HOST = apidoc["config"]["es_host"]
        config_mod.ES_INDEX = apidoc["config"]["index"]
        config_mod.ES_DOC_TYPE = apidoc["config"]["doc_type"]
        self.logger.info(f"CHECK IF ESHOST IS CHANGED {config_mod.ES_HOST}")

        launcher = BiothingsAPILauncher(config_mod)
        port = int(apidoc["config"]["port"])
        try:
            server = launcher.get_server()
            server.listen(port)
            self.register[api_id] = server
            self.logger.info("Running API '%s' on port %s" % (api_id, port))
            url = "http://%s:%s" % (socket.gethostname(), port)
            self.register_status(api_id, "running", url=url)
        except Exception as e:
            self.logger.exception("Failed to start API '%s'" % api_id)
            self.register_status(api_id, "failed", err=str(e))
            raise
        # self.test_api(api_id)


    def stop_api(self, api_id):
        try:
            assert api_id in self.register, "API '%s' is not running" % api_id
            server = self.register.pop(api_id)
            server.stop()
            if server._stopped:
                self.register_status(api_id, "stopped")
        except Exception as e:
            self.logger.exception("Failed to stop API '%s'" % api_id)
            self.register_status(api_id, "failed", err=str(e))
            raise

    def delete_api(self, api_id):
        try:
            self.stop_api(api_id)
        except Exception as e:
            self.logger.warning("While trying to stop API '%s': %s" % (api_id, e))
        finally:
            self.api.remove({"_id": api_id})

    def create_api(
        self,
        api_id,
        es_host,
        index,
        doc_type,
        port,
        description=None,
        **kwargs,
    ):
        apidoc = {
            "_id": api_id,
            "config": {
                "port": port,
                "es_host": es_host,
                "index": index,
                "doc_type": doc_type,
            },
            "description": description,
        }
        apidoc.update(kwargs)
        self.api.save(apidoc)
        return apidoc

