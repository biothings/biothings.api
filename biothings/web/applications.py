import inspect
import logging
from pprint import pformat
from pydoc import locate
from types import SimpleNamespace

import tornado.httpserver
import tornado.ioloop
import tornado.log
import tornado.web
from biothings.web.handlers import BaseAPIHandler, BaseESRequestHandler
from biothings.web.settings import configs
from biothings.web.services.namespace import BiothingsNamespace

try:
    from raven.contrib.tornado import AsyncSentryClient
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

class AsyncTornadoBiothingsAPI(tornado.web.Application):

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
            'biothings': biothings.config,
            'autoreload': False,
            'debug': False,
        }
        settings.update(override or {})
        supported_keywords = (
            'default_handler_class', 'default_handler_args', 'template_path',
            'log_function', 'compress_response', 'cookie_secret',
            'login_url', 'static_path', 'static_url_prefix')

        for setting in supported_keywords:
            if hasattr(biothings.config, setting.upper()):
                if setting in settings:
                    logging.warning(
                        "Override config setting %s to %s.",
                        setting, settings[setting])
                    continue
                settings[setting] = getattr(biothings.config, setting.upper())

        if __SENTRY_INSTALLED__ and biothings.config.SENTRY_CLIENT_KEY:
            # Setup error monitoring with Sentry. More on:
            # https://docs.sentry.io/clients/python/integrations/#tornado
            settings['sentry_client'] = AsyncSentryClient(biothings.config.SENTRY_CLIENT_KEY)

        return settings

    @staticmethod
    def _get_handlers(biothings, addons=None):
        '''
        Generates the tornado.web.Application `(regex, handler_class, options) tuples
        <http://www.tornadoweb.org/en/stable/web.html#application-configuration>`_.
        '''
        handlers = {}
        addons = addons or []
        for rule in biothings.config.APP_LIST + addons:
            pattern = rule[0]
            handler = load_class(rule[1])
            setting = rule[2] if len(rule) == 3 else {}
            assert handler, rule[1]

            if '{typ}' in pattern:
                if not issubclass(handler, BaseESRequestHandler):
                    raise TypeError()
                for biothing_type in biothings.metadata.types:
                    _pattern = pattern.format(
                        pre=biothings.config.API_PREFIX,
                        ver=biothings.config.API_VERSION,
                        typ=biothing_type).replace('//', '/')
                    _setting = dict(setting)
                    _setting['biothing_type'] = biothing_type
                    handlers[_pattern] = (_pattern, handler, _setting)
            elif '{pre}' in pattern or '{ver}' in pattern:
                pattern = pattern.format(
                    pre=biothings.config.API_PREFIX,
                    ver=biothings.config.API_VERSION).replace('//', '/')
                if '()' not in pattern:
                    handlers[pattern] = (pattern, handler, setting)
            else:  # no pattern translation
                handlers[pattern] = (pattern, handler, setting)

        handlers = list(handlers.values())
        logger.info('API Handlers:\n%s', pformat(handlers, width=200))
        return handlers  # TODO

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
            _handlers = [(f'/{c.API_PREFIX}/.*', cls.get_app(c, settings)) for c in config.modules]
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
                setting_attr = '_'.join((handler_name, 'kwargs')).upper()
                setting_options = getattr(config, setting_attr, {})
                self.biothings.optionsets.add(handler_name, setting_options)
                self.biothings.optionsets.add(handler_name, handler_options)

    def _populate_handlers(self, handlers):
        for handler in handlers:
            self.biothings.handlers[handler[0]] = handler[1]

class WSGITornadoBiothingsAPI():
    pass

class FlaskBiothingsAPI():
    pass

class FastAPIBiothingsAPI():
    pass


BiothingsAPI = AsyncTornadoBiothingsAPI
