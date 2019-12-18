import os
from importlib import import_module

# Error class
class BiothingTestConfigError(Exception):
    pass

class BiothingTestSettings(object):
    def __init__(self, config_module='biothings.tests.settings.default'):
        self.config_mod = import_module(config_module)

    def __getattr__(self, name):
        try:
            return getattr(self.config_mod, name)
        except AttributeError:
            raise AttributeError("No test setting named '{}' was found, check configuration module.".format(name))

