
import importlib.util
import inspect
import logging
import os
import types
from importlib import import_module
from pprint import pformat
from pydoc import locate

import tornado.log
from tornado.web import Application

from biothings.web.settings import default as web_default
from biothings.web.handlers import BaseAPIHandler, BaseESRequestHandler
from biothings.web.options import OptionSets

try:
    from raven.contrib.tornado import AsyncSentryClient
except ImportError:
    __SENTRY_INSTALLED__ = False
else:
    __SENTRY_INSTALLED__ = True

class BiothingConfigError(Exception):
    pass

class BiothingWebSettings():
    '''
    A container for the settings that configure the web API.

    * Environment variables can override settings of the same names.
    * Default values are defined in biothings.web.settings.default.

    '''
    _default = object()
    _parent = object()
    _user = object()

    def __init__(self, config=None, parent=None, **kwargs):
        '''
        :param config: a module that configures this biothing
            or its fully qualified name,
            or its module file path.
        '''
        self._default = web_default
        self._parent = parent  # config package
        self._user = self.load_module(config, object())
        self.logger.info("%s", self._user)  # log file location

        # process keyword setting override
        for key, value in kwargs.items():
            setattr(self._user, key, value)

        # process environment variable override of named settings
        for name in os.environ:
            if hasattr(self, name):
                new_value = None
                if isinstance(getattr(self, name), str):
                    new_value = os.environ[name]
                elif isinstance(getattr(self, name), bool):
                    new_value = os.environ[name].lower() in ('true', '1')
                if new_value is not None:
                    self.logger.info("$ %s = %s", name, os.environ[name])
                    setattr(self._user, name, new_value)
                else:  # cannot override dict, array, object type...
                    self.logger.error("Env %s is not suppored.", name)

        # for metadata dev details
        if os.path.isdir(os.path.join(self.APP_GIT_REPOSITORY, '.git')):
            self._git_repo_path = self.APP_GIT_REPOSITORY
        else:
            self._git_repo_path = None

        self.optionsets = OptionSets()
        self.handlers = {}
        self.validate()

    @staticmethod
    def load_module(config, default=None):
        """
        Ensure config is a module.
        If config does not evaluate,
        Return default if it's provided.
        """
        if isinstance(config, types.ModuleType):
            return config
        elif isinstance(config, str) and config.endswith('.py'):
            spec = importlib.util.spec_from_file_location("config", config)
            config = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config)
            return config
        elif isinstance(config, str) and config:
            return import_module(config)
        elif not config and default:
            return default
        raise BiothingConfigError()

    def __getattr__(self, name):
        if hasattr(self._user, name):
            return getattr(self._user, name)
        elif hasattr(self._parent, name):
            return getattr(self._parent, name)
        elif hasattr(self._default, name):
            return getattr(self._default, name)
        else:  # not provided and no default
            raise AttributeError()

    @property
    def logger(self):
        return logging.getLogger('biothings.web.settings')

    @staticmethod
    def load_class(kls):
        """
        Ensure config is a module.
        If config does not evaluate,
        Return default if it's provided.
        """
        if inspect.isclass(kls):
            return kls
        if isinstance(kls, str):
            return locate(kls)
        raise BiothingConfigError()

    def _generate_app_settings(self, override=None):
        """
        Generates settings for tornado.web.Application. This result and the
        method below can define a tornado application to start a web server.
        """
        settings = {
            'biothings': self,
            'autoreload': False,
            'debug': False,
        }
        settings.update(override or {})
        supported_keywords = (
            'default_handler_class', 'default_handler_args', 'template_path',
            'log_function', 'compress_response', 'cookie_secret',
            'login_url', 'static_path', 'static_url_prefix')

        for setting in supported_keywords:
            if hasattr(self, setting.upper()):
                settings[setting] = getattr(self, setting.upper())

        if __SENTRY_INSTALLED__ and self.SENTRY_CLIENT_KEY:
            # Setup error monitoring with Sentry. More on:
            # https://docs.sentry.io/clients/python/integrations/#tornado
            settings['sentry_client'] = AsyncSentryClient(self.SENTRY_CLIENT_KEY)

        return settings

    def _generate_app_handlers(self, addons=None):
        '''
        Generates the tornado.web.Application `(regex, handler_class, options) tuples
        <http://www.tornadoweb.org/en/stable/web.html#application-configuration>`_.
        '''
        handlers = {}
        addons = addons or []
        for rule in self.APP_LIST + addons:
            pattern = rule[0]
            handler = self.load_class(rule[1])
            setting = rule[2] if len(rule) == 3 else {}
            assert handler, rule[1]
            if issubclass(handler, BaseAPIHandler) and handler.name:
                handler_name = handler.name
                handler_options = handler.kwargs
                setting_attr = '_'.join((handler_name, 'kwargs')).upper()
                setting_options = getattr(self, setting_attr, {})
                self.optionsets.add(handler_name, handler_options)
                self.optionsets.add(handler_name, setting_options)
                self.optionsets.groups[handler_name] = handler.kwarg_groups
                self.optionsets.methods[handler_name] = handler.kwarg_methods
            if '{typ}' in pattern:
                if not issubclass(handler, BaseESRequestHandler):
                    raise BiothingConfigError()
                for biothing_type in self.BIOTHING_TYPES:
                    _pattern = pattern.format(
                        pre=self.API_PREFIX,
                        ver=self.API_VERSION,
                        typ=biothing_type).replace('//', '/')
                    _setting = dict(setting)
                    _setting['biothing_type'] = biothing_type
                    handlers[_pattern] = (_pattern, handler, _setting)
            elif '{pre}' in pattern or '{ver}' in pattern:
                pattern = pattern.format(
                    pre=self.API_PREFIX,
                    ver=self.API_VERSION).replace('//', '/')
                handlers[pattern] = (pattern, handler, setting)
            else:  # no pattern translation
                handlers[pattern] = (pattern, handler, setting)

        self.handlers = handlers
        handlers = list(handlers.values())
        self.logger.info('API Handlers:\n%s', pformat(handlers, width=200))
        return handlers

    def get_app(self, settings=False, handlers=None):
        """
        Return the tornado.web.Application defined by this settings.
        This is primarily how an HTTP server interacts with this class.
        Additional settings and handlers accepted as parameters.
        """
        # config package
        if self._user.__name__ == self._user.__package__:
            attrs = [getattr(self._user, attr) for attr in dir(self._user)]
            confs = [attr for attr in attrs if isinstance(attr, types.ModuleType)]
            _settings = [self.__class__(_attr, self._user) for _attr in confs]
            _handlers = [(f'/{c.API_PREFIX}/.*', c.get_app(settings)) for c in _settings]
            _handlers += handlers or []  # second level front pages won't be exposed
        else:  # config module
            _handlers = self._generate_app_handlers(handlers)
        _settings = self._generate_app_settings(settings)
        return Application(_handlers, **_settings)

    def get_git_repo_path(self):
        '''
        Return the path of the codebase if the specified folder in settings exists or `None`.
        '''
        return self._git_repo_path

    def configure_logger(self, logger):
        '''
        Configure a logger's formatter to use the format defined in this web setting.
        '''
        try:
            if self.LOGGING_FORMAT and logger.hasHandlers():
                for handler in logger.handlers:
                    if isinstance(handler.formatter, tornado.log.LogFormatter):
                        handler.formatter._fmt = self.LOGGING_FORMAT
        except Exception:
            self.logger.exception('Error configuring logger %s.', logger)

    def validate(self):
        '''
        Validate the settings defined for this web server.
        '''
        assert self.API_VERSION or self.API_PREFIX
        assert isinstance(self.LIST_SIZE_CAP, int)
        assert isinstance(self.ACCESS_CONTROL_ALLOW_METHODS, str)
        assert isinstance(self.ACCESS_CONTROL_ALLOW_HEADERS, str)
