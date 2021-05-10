import asyncio
from tornado.web import RequestHandler
import logging
import datetime

# pandas.io.json encoder to deal with non-json compliant values NaN, Inf
# (based on ujson, but pandas has its own way to deal with these values)
# see https://github.com/biothings/biothings.api/commit/59c0d78f758018b0d87836657a2b5d1a700503a1
# import pandas.io.json as pdjson
# replace pandas json encoder with orjson:
import orjson

from biothings import config
from biothings import ConfigurationDefault, ConfigurationValue


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
        def configuration_default_handler(obj: ConfigurationDefault):
            return f"ConfigurationDefault: default: {obj.default}, desc: {obj.desc}"

        def configuration_value_handler(obj: ConfigurationValue):
            return f"ConfigurationValue: FIXME!!"

        handlers = {
            'biothings.ConfigurationDefault': configuration_default_handler,
            'biothings.ConfigurationValue': configuration_value_handler,
        }

        def serialization_default_handler(obj):
            # use names like biothings.ConfigurationDefault, builtins.set, etc. note "builtins"
            cls = getattr(obj, '__class__', None)
            cls_name = getattr(cls, '__qualname__', None)
            module_name = getattr(cls, '__module__', None)
            if cls_name and module_name:
                handler_name = f'{module_name}.{cls_name}'
                if handler_name in handlers:
                    return handlers[handler_name](obj)
            raise TypeError("Cannot serialize %s: %s" % (type(obj), obj))

        super(DefaultHandler, self).write(
            # pdjson.dumps({
            #     "result": result,
            #     "status": "ok"
            # }, iso_dates=True)
            orjson.dumps({
                "result": result,
                "status": "ok"
            }, option=orjson.OPT_NON_STR_KEYS, default=serialization_default_handler).decode()
        )

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
            "name": self.hub_name or getattr(config, "HUB_NAME", None),
            "biothings_version": getattr(config, "BIOTHINGS_VERSION", None),
            "app_version": getattr(config, "APP_VERSION", None),
            "icon": getattr(config, "HUB_ICON", None),
            "now": datetime.datetime.now().astimezone(),
            "features": self.features
        })
