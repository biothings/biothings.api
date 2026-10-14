import datetime
import json
import logging

# non-json compliant values (NaN, Inf) are handled by utils.serializer,
# which encodes them as null
from tornado.web import HTTPError, RequestHandler

from biothings import config
from biothings.hub.api.handlers.auth import get_users, login, require_login
from biothings.utils import serializer


class DefaultHandler(RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Content-Type", "application/json")
        # part of pre-flight requests
        self.set_header("Access-Control-Allow-Methods", "PUT, DELETE, POST, GET, OPTIONS")
        self.set_header("Access-Control-Allow-Headers", "Content-Type,X-BioThings-API,X-Biothings-Access-Token")

    def prepare(self):
        # when users are defined (HUB_API_USERS), running commands or changing data requires a login
        require_login(self)

    def write(self, result):
        super(DefaultHandler, self).write(
            # pdjson.dumps({
            #     "result": result,
            #     "status": "ok"
            # }, iso_dates=True)
            serializer.to_json({
                "result": result, "status": "ok"
            })
        )

    def write_error(self, status_code, **kwargs):
        self.set_status(status_code)
        super(DefaultHandler, self).write(
            {
                "error": str(kwargs.get("exc_info", [None, None, None])[1]),
                "status": "error",
                "code": status_code,
            }
        )

    # defined by default so we accept OPTIONS pre-flight requests
    def options(self, *args, **kwargs):
        logging.debug("OPTIONS args: %s, kwargs: %s" % (args, kwargs))


class BaseHandler(DefaultHandler):
    def initialize(self, managers, **kwargs):
        self.managers = managers


class GenericHandler(DefaultHandler):
    def initialize(self, shell, **kwargs):
        self.shell = shell

    def get(self, *args, **kwargs):
        logging.debug("GET args: %s, kwargs: %s" % (args, kwargs))
        self.write_error(405, exc_info=(None, "Method GET not allowed", None))

    def post(self, *args, **kwargs):
        logging.debug("POST args: %s, kwargs: %s" % (args, kwargs))
        self.write_error(405, exc_info=(None, "Method POST not allowed", None))

    def put(self, *args, **kwargs):
        logging.debug("PUT args: %s, kwargs: %s" % (args, kwargs))
        self.write_error(405, exc_info=(None, "Method PUT not allowed", None))

    def delete(self, *args, **kwargs):
        logging.debug("DELETE args: %s, kwargs: %s" % (args, kwargs))
        self.write_error(405, exc_info=(None, "Method DELETE not allowed", None))

    def head(self, *args, **kwargs):
        logging.debug("HEAD args: %s, kwargs: %s" % (args, kwargs))
        self.write_error(405, exc_info=(None, "Method HEAD not allowed", None))


class RootHandler(DefaultHandler):
    def initialize(self, features, hub_name=None, **kwargs):
        self.features = features
        self.hub_name = hub_name

    async def get(self):
        self.write(
            {
                "name": self.hub_name or getattr(config, "HUB_NAME", None),
                "biothings_version": getattr(config, "BIOTHINGS_VERSION", None),
                "app_version": getattr(config, "APP_VERSION", None),
                "icon": getattr(config, "HUB_ICON", None),
                "now": datetime.datetime.now().astimezone(),
                "features": self.features,
                # the read-only API can't run commands, it never requires a login
                "login_required": bool(get_users()) and "readonly" not in self.features,
            }
        )


class LoginHandler(DefaultHandler):
    """
    POST /login with {"username": ..., "password": ...}: returns a login token, to send in the
    X-Biothings-Access-Token header of the requests requiring a login, and its expiry date
    """

    def prepare(self):
        pass  # logging in doesn't require a login

    async def post(self):
        if not get_users():
            raise HTTPError(404, reason="Login isn't enabled on this hub (no HUB_API_USERS)")
        try:
            body = json.loads(self.request.body or b"{}")
            username, password = body["username"], body["password"]
        except (ValueError, KeyError, TypeError):
            username = password = None
        if not isinstance(username, str) or not isinstance(password, str):
            raise HTTPError(400, reason="Expecting a JSON body with a 'username' and a 'password'")
        result = await login(username, password, self.request.remote_ip)
        # not through self.write(), so the token can't be altered like other results (eg. hidden as a secret)
        super(DefaultHandler, self).write(serializer.to_json({"result": result, "status": "ok"}))
