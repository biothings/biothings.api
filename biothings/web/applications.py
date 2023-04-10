"""
    Biothings Web Applications -

    define the routes and handlers a supported web framework would consume
    basing on a config file, typically named `config.py`, enhanced by
    :py:mod:`biothings.web.settings.configs`.

    The currently supported web frameworks are
    `Tornado <https://www.tornadoweb.org/en/stable/index.html>`_,
    `Flask <https://flask.palletsprojects.com/en/2.0.x/>`_, and
    `FastAPI <https://fastapi.tiangolo.com/>`_.

    The :py:mod:`biothings.web.launcher` can start the compatible HTTP servers
    basing on their interface. And the web applications delegate routes defined
    in the config file to handlers typically in :py:mod:`biothings.web.handlers`.

    +----------------+------------+--------------------------------+
    | Web Framework  | Interface  | Handlers                       |
    +================+============+================================+
    | Tornado        | Tornado    | biothings.web.handlers.*       |
    +----------------+------------+--------------------------------+
    | Flask          | WSGI       | biothings.web.handlers._flask  |
    +----------------+------------+--------------------------------+
    | FastAPI        | ASGI       | biothings.web.handlers._fastapi|
    +----------------+------------+--------------------------------+

"""

import inspect
import logging
from pprint import pformat
from pydoc import locate
from types import SimpleNamespace

import tornado.httpserver
import tornado.ioloop
import tornado.log
import tornado.web

from biothings.web.handlers import BaseAPIHandler, BaseQueryHandler
from biothings.web.services.namespace import BiothingsNamespace
from biothings.web.settings import configs

try:
    import sentry_sdk
    from sentry_sdk.integrations.tornado import TornadoIntegration
except ImportError:
    __SENTRY_INSTALLED__ = False
else:
    __SENTRY_INSTALLED__ = True

logger = logging.getLogger(__name__)


def load_class(kls):
    if inspect.isclass(kls):
        return kls
    if isinstance(kls, str):
        return locate(kls)
    raise ValueError()


class TornadoBiothingsAPI(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.biothings = SimpleNamespace()

    @staticmethod
    def _get_settings(biothings, override=None):
        """
        Generates settings for tornado.web.Application. This result and the
        method below can define a tornado application to start a web server.
        """
        settings = {
            "biothings": biothings.config,
            "autoreload": False,
            "debug": False,
        }
        settings.update(override or {})
        supported_keywords = (
            "default_handler_class",
            "default_handler_args",
            "template_path",
            "log_function",
            "compress_response",
            "cookie_secret",
            "login_url",
            "static_path",
            "static_url_prefix",
        )

        for setting in supported_keywords:
            if hasattr(biothings.config, setting.upper()):
                if setting in settings:
                    logging.warning("Override config setting %s to %s.", setting, settings[setting])
                    continue
                settings[setting] = getattr(biothings.config, setting.upper())

        if __SENTRY_INSTALLED__ and biothings.config.SENTRY_CLIENT_KEY:
            sentry_sdk.init(
                dsn=biothings.config.SENTRY_CLIENT_KEY,
                # adjust this value to allow sentry to trace transactions:
                #   https://docs.sentry.io/platforms/python/guides/starlette/configuration/options/#traces-sample-rate
                traces_sample_rate=0.2,
                integrations=[TornadoIntegration()],
            )

        return settings

    @staticmethod
    def _get_handlers(biothings, addons=None):
        """
        Generates the tornado.web.Application `(regex, handler_class, options) tuples
        <http://www.tornadoweb.org/en/stable/web.html#application-configuration>`_.
        """
        handlers = {}
        addons = addons or []
        for rule in biothings.config.APP_LIST + addons:
            pattern = rule[0]
            handler = load_class(rule[1])
            setting = rule[2] if len(rule) == 3 else {}
            assert handler, rule[1]

            if "{typ}" in pattern or "{tps}" in pattern:
                if not issubclass(handler, BaseQueryHandler):
                    raise TypeError("Not a biothing_type-aware handler.")
                if "{tps}" in pattern and len(biothings.metadata.types) <= 1:
                    continue  # '{tps}' routes only valid for multi-type apps
                for biothing_type in biothings.metadata.types:
                    _pattern = pattern.format(
                        pre=biothings.config.APP_PREFIX,
                        ver=biothings.config.APP_VERSION,
                        typ=biothing_type,
                        tps=biothing_type,
                    ).replace("//", "/")
                    _setting = dict(setting)
                    _setting["biothing_type"] = biothing_type
                    handlers[_pattern] = (_pattern, handler, _setting)
            elif "{pre}" in pattern or "{ver}" in pattern:
                pattern = pattern.format(pre=biothings.config.APP_PREFIX, ver=biothings.config.APP_VERSION).replace(
                    "//", "/"
                )
                if "()" not in pattern:
                    handlers[pattern] = (pattern, handler, setting)
            else:  # no pattern translation
                handlers[pattern] = (pattern, handler, setting)

        handlers = list(handlers.values())
        logger.info("API Handlers:\n%s", pformat(handlers, width=200))
        return handlers

    @classmethod
    def get_app(cls, config, settings=None, handlers=None):
        """
        Return the tornado.web.Application defined by this config.
        **Additional** settings and handlers are accepted as parameters.
        """
        if isinstance(config, configs.ConfigModule):
            biothings = BiothingsNamespace(config)
            _handlers = BiothingsAPI._get_handlers(biothings, handlers)
            _settings = BiothingsAPI._get_settings(biothings, settings)
            app = cls(_handlers, **_settings)
            app.biothings = biothings
            app._populate_optionsets(config, _handlers)
            app._populate_handlers(_handlers)
            return app
        if isinstance(config, configs.ConfigPackage):
            biothings = BiothingsNamespace(config.root)
            _handlers = [(f"/{c.APP_PREFIX}/.*", cls.get_app(c, settings)) for c in config.modules]
            _settings = BiothingsAPI._get_settings(biothings, settings)
            app = cls(_handlers + handlers or [], **_settings)
            app.biothings = biothings
            app._populate_optionsets(config, handlers or [])
            app._populate_handlers(handlers or [])
            return app
        raise TypeError()

    def _populate_optionsets(self, config, handlers):
        for handler in handlers:
            handler = handler[1]  # handler[0] is a matching pattern
            if issubclass(handler, BaseAPIHandler) and handler.name:
                handler_name = handler.name
                handler_options = handler.kwargs
                setting_attr = "_".join((handler_name, "kwargs")).upper()
                setting_options = getattr(config, setting_attr, {})
                self.biothings.optionsets.add(handler_name, setting_options)
                self.biothings.optionsets.add(handler_name, handler_options)

    def _populate_handlers(self, handlers):
        for handler in handlers:
            self.biothings.handlers[handler[0]] = handler[1]


try:
    from flask import Flask

    class FlaskBiothingsAPI(Flask):
        @classmethod
        def get_app(cls, config):
            app = cls(__name__)
            app.config["JSON_SORT_KEYS"] = False
            app.url_map.strict_slashes = False
            app.biothings = BiothingsNamespace(config)
            from biothings.web.handlers._flask import routes

            for route in routes:
                setting_attr = "_".join((route.name, "kwargs")).upper()
                setting_options = getattr(config, setting_attr, {})
                app.biothings.optionsets.add(route.name, setting_options)
                if isinstance(route.pattern, str):
                    route.pattern = [route.pattern]
                for pattern in route.pattern:
                    if "{typ}" in pattern:
                        assert len(app.biothings.metadata.types) == 1, (
                            "Currently Biothings API on Flask only " "supports single biothings_type configuration."
                        )
                    pattern = pattern.replace("{typ}", app.biothings.metadata.types[0])
                    pattern = pattern.replace("{ver}", app.biothings.config.APP_VERSION)
                    app.add_url_rule(pattern, route.name, route, methods=route.methods)
                    app.biothings.handlers[pattern] = route
            return app

except Exception as exc:  # noqa F841

    class FlaskBiothingsAPI:
        @classmethod
        def get_app(cls, config):
            raise exc


try:
    from fastapi import FastAPI
    from fastapi.middleware.wsgi import WSGIMiddleware

    class FastAPIBiothingsAPI(FastAPI):
        @classmethod
        def get_app(cls, config):
            app = cls()
            app.mount("/", WSGIMiddleware(FlaskBiothingsAPI.get_app(config)))
            return app

    # Native Implementation
    # -------------------------------------------------------------------------
    # class FastAPIBiothingsAPI(FastAPI):
    #     @classmethod
    #     def get_app(cls, config):
    #         from biothings.web.handlers import _fastapi
    #         _fastapi.biothings = BiothingsNamespace(config)
    #         app = cls()
    #         for route in _fastapi.routes:
    #             setting_attr = '_'.join((route.name, 'kwargs')).upper()
    #             setting_options = getattr(config, setting_attr, {})
    #             _fastapi.biothings.optionsets.add(route.name, setting_options)
    #             app.get(*route.args, **route.kwargs)(route)
    #         return app

except Exception as exc:  # noqa F841

    class FastAPIBiothingsAPI:
        @classmethod
        def get_app(cls, config):
            raise exc


BiothingsAPI = TornadoBiothingsAPI  # default
