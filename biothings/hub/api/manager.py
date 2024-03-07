import contextlib
import importlib
import logging
import os
import socket
import types
from datetime import datetime
from functools import partial

import pytest

from biothings import config as btconfig
from biothings.hub import APITESTER_CATEGORY
from biothings.utils.hub_db import get_api
from biothings.utils.loggers import get_logger
from biothings.utils.manager import BaseManager
from biothings.web.launcher import BiothingsAPILauncher


class LoggerFile:
    """
    File-like object that writes to a logger at a specific level.
    This object is used to redirect stdout/stderr to a logger.
    """
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.rstrip() != "":
            self.logger.log(self.level, message)

    def flush(self):
        pass

    def isatty(self):
        return False

class APITester:
    def __init__(self):
        self.setup()

    def setup(self):
        self.setup_log()

    def setup_log(self):
        self.logger, _ = get_logger("apimanager")

    def run_pytests(self, pytest_path, host):
        """
        Run the pytests for the given pytest path and host. We create a LoggerFile object to redirect stdout and stderr to the logger.
        """

        stdout = LoggerFile(self.logger, logging.INFO)
        stderr = LoggerFile(self.logger, logging.ERROR)

        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            pytest.main(["-vv", pytest_path, "-m", "not production", "--host", host, "--scheme", "http"])


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

    def get_predicates(self):
        return []

    def get_pinfo(self):
        """
        Return dict containing information about the current process
        (used to report in the hub)
        """
        pinfo = {
            "category": APITESTER_CATEGORY,
            "source": "",
            "description": "",
            "step": "test_api",
        }
        preds = self.get_predicates()
        if preds:
            pinfo["__predicates__"] = preds
        return pinfo


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

    def import_config_web(self):
        try:
            config_mod = importlib.import_module(btconfig.APITEST_CONFIG)
            self.logger.info("Imported %s as config_mod.", btconfig.APITEST_CONFIG)
        except ImportError:
            self.logger.info("Cannot import APITEST_CONFIG variable from btconfig, creating a new module")
            config_mod = types.ModuleType("config_mod")
        finally:
            return config_mod

    async def test_api(self, api_id):
        """
        Run pytests for the given api id. If no pytest path is found in the config_web_local.py then log an error.
        """
        assert self.job_manager

        has_pytests = False
        try:
            if btconfig.APITEST_PATH:
                has_pytests = True
        except AttributeError:
            self.logger.error("No pytest path found in config. Skipping pytests for '%s'", api_id)

        #if has_pytest is true then run the pytests
        if has_pytests:
            self.logger.info("Pytest path found in config. Running pytests from %s.", btconfig.APITEST_PATH)
            apidoc = self.api.find_one({"_id": api_id})
            port = int(apidoc["config"]["port"])
            APITEST_PATH = os.path.join(btconfig.APITEST_PATH)
            pinfo = self.get_pinfo()
            pinfo["description"] = "Running API tests"
            job = await self.job_manager.defer_to_process(pinfo, partial(APITester().run_pytests, APITEST_PATH, "localhost:" + str(port)))

            got_error = False
            def updated(f):
                try:
                    _ = f.result()
                    self.logger.info("Finished running pytests for '%s'" % api_id)
                    self.register_status(api_id, "running", job={"step": "test_api"})
                except Exception as e:
                    nonlocal got_error
                    self.logger.error("Failed to run pytests for '%s': %s" % (api_id, e))
                    self.register_status(api_id, "running", job={"err": repr(e)})
                    got_error = e

            job.add_done_callback(updated)
            await job

    def start_api(self, api_id):
        apidoc = self.api.find_one({"_id": api_id})
        if not apidoc:
            raise APIManagerException("No such API with ID '%s'" % api_id)
        if "entry_point" in apidoc:
            raise NotImplementedError(
                "Custom entry point not implemented yet, " + "only basic generated APIs are currently supported"
            )

        config_mod = self.import_config_web()
        config_mod.ES_HOST = apidoc["config"]["es_host"]
        config_mod.ES_INDEX = apidoc["config"]["index"]
        config_mod.ES_DOC_TYPE = apidoc["config"]["doc_type"]

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

