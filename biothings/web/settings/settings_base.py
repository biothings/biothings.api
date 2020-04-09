
import importlib.util
import logging
import os
import types
from importlib import import_module
from pprint import pformat
from pydoc import locate


import biothings.web.settings.default
from biothings.web.api.handler import BaseAPIHandler

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

    def __init__(self, config=None, **kwargs):
        '''
        :param config: a module that configures this biothing
            or its fully qualified name,
            or its module file path.
        '''
        self._default = biothings.web.settings.default
        self._user = self.load_module(config, self._default)
        logging.info("Loaded: %s", self._user)

        # process keyword setting override
        for key, value in kwargs.items():
            setattr(self._user, key, value)

        # process environment variable override of named settings
        for name in os.environ:
            if hasattr(self, name) and isinstance(getattr(self, name), str):
                logging.info("Env %s = %s", name, os.environ[name])
                setattr(self._user, name, os.environ[name])

        # for metadata dev details
        if os.path.isdir(os.path.join(self.APP_GIT_REPOSITORY, '.git')):
            self._git_repo_path = self.APP_GIT_REPOSITORY
        else:
            self._git_repo_path = None

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
        elif hasattr(self._default, name):
            return getattr(self._default, name)
        else:  # not provided and no default
            raise AttributeError()

    def generate_app_settings(self, debug=False):
        """
        Generates settings for tornado.web.Application. This result and the
        method below can define a tornado application to start a web server.
        """
        settings = {
            'biothings': self,
            'debug': bool(debug)
        }
        supported_keywords = (
            'default_handler_class', 'default_handler_args',
            'log_function', 'compress_response', 'cookie_secret',
            'login_url', 'static_path', 'static_url_prefix')

        for setting in supported_keywords:
            if hasattr(self, setting.upper()):
                settings[setting] = getattr(self, setting.upper())

        if debug:
            self.GA_RUN_IN_PROD = False

        elif __SENTRY_INSTALLED__ and self.SENTRY_CLIENT_KEY:
            # Setup error monitoring with Sentry. More on:
            # https://docs.sentry.io/clients/python/integrations/#tornado
            settings['sentry_client'] = AsyncSentryClient(self.SENTRY_CLIENT_KEY)

        return settings

    def generate_app_handlers(self, addons=None):
        '''
        Generates the tornado.web.Application `(regex, handler_class, options) tuples
        <http://www.tornadoweb.org/en/stable/web.html#application-configuration>`_.
        '''
        handlers = []
        addons = addons or []
        for rule in self.APP_LIST + addons:
            handler = locate(rule[1])
            if issubclass(handler, BaseAPIHandler):
                pattern = rule[0]
                setting = rule[2] if len(rule) == 3 else {}
                if '{typ}' in pattern:
                    for biothing_type in self.BIOTHING_TYPES:
                        pattern = pattern.format(
                            pre=self.API_PREFIX,
                            ver=self.API_VERSION,
                            typ=biothing_type).replace('//', '/')
                        setting['biothing_type'] = biothing_type
                    handlers.append((pattern, handler, setting))
                else:
                    pattern = pattern.format(
                        pre=self.API_PREFIX,
                        ver=self.API_VERSION).replace('//', '/')
                    handlers.append((pattern, handler, setting))
            else:
                handlers.append(rule)

        logging.info('API Handlers:\n%s', pformat(handlers, width=200))
        return handlers

    def get_git_repo_path(self):
        '''
        Return the path of the codebase if the specified folder in settings exists or `None`.
        '''
        return self._git_repo_path

    def validate(self):
        '''
        Validate the settings defined for this web server.
        '''
        assert self.API_VERSION or self.API_PREFIX
        assert isinstance(self.LIST_SIZE_CAP, int)
        assert isinstance(self.ACCESS_CONTROL_ALLOW_METHODS, str)
        assert isinstance(self.ACCESS_CONTROL_ALLOW_HEADERS, str)

    #### COMPATIBILITY METHODS ####

    def set_debug_level(self, debug=False):
        pass

    @property
    def git_repo_path(self):
        return self._git_repo_path

    @property
    def _app_git_repo(self):
        return self._git_repo_path
