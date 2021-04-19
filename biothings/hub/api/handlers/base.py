import asyncio
from tornado.web import RequestHandler
import logging
import datetime

# pandas.io.json encoder to deal with non-json compliant values NaN, Inf
# (based on ujson, but pandas has its own way to deal with these values)
# see https://github.com/biothings/biothings.api/commit/59c0d78f758018b0d87836657a2b5d1a700503a1
import pandas.io.json as pdjson

from biothings import config


class DefaultHandler(RequestHandler):
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Content-Type', 'application/json')
        # part of pre-flight requests
        self.set_header('Access-Control-Allow-Methods',
                        'PUT, DELETE, POST, GET, OPTIONS')
        self.set_header(
            'Access-Control-Allow-Headers',
            'Content-Type,X-BioThings-API,X-Biothings-Access-Token')

    def write(self, result):
        super(DefaultHandler, self).write(
            pdjson.dumps({
                "result": result,
                "status": "ok"
            }, iso_dates=True))

    def write_error(self, status_code, **kwargs):
        self.set_status(status_code)
        super(DefaultHandler, self).write({
            "error":
            str(kwargs.get("exc_info", [None, None, None])[1]),
            "status":
            "error",
            "code":
            status_code
        })

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
        self.write_error(405,
                         exc_info=(None, "Method DELETE not allowed", None))

    def head(self, *args, **kwargs):
        logging.debug("HEAD args: %s, kwargs: %s" % (args, kwargs))
        self.write_error(405, exc_info=(None, "Method HEAD not allowed", None))


class RootHandler(DefaultHandler):
    def initialize(self, features, hub_name=None, **kwargs):
        self.features = features
        self.hub_name = hub_name

    @asyncio.coroutine
    def get(self):
        self.write({
            "name": self.hub_name or \
            getattr(config, "HUB_NAME", None),
            "biothings_version":
            getattr(config, "BIOTHINGS_VERSION", None),
            "app_version":
            getattr(config, "APP_VERSION", None),
            "icon":
            getattr(config, "HUB_ICON", None),
            "now":
            datetime.datetime.now().astimezone(),
            "features":
            self.features,
        })
