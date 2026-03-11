"""
Dataplugin exception classes
"""


class LoaderException(Exception):
    """Exceptions specific to our plugin loading capabilities."""


class PluginTransportException(Exception):
    """Exceptions specific to our dumper transport protocol selection."""


class AssistantException(Exception):
    """Exceptions specific to our dataplugin assistant instances."""
