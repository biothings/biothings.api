"""
    Web Handler Class

    * Can be used for both API handlers and web rendering
    * Report errors to sentry when its API key is provided
"""

from tornado.web import RequestHandler


try:
    from raven.contrib.tornado import SentryMixin
except ImportError:
    class SentryMixin(object):
        """dummy class mixin"""

class BaseHandler(SentryMixin, RequestHandler):
    """
        Parent class of all handlers, only direct descendant of `tornado.web.RequestHandler
        <http://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler>`_,
    """

    def get_sentry_client(self):
        """
        Override default and retrieve from tornado setting instead.
        """
        client = self.settings.get('sentry_client')
        if not client:
            self.require_setting('sentry_client')
        return client

    def log_exception(self, *args, **kwargs):
        """
        Only attempt to report to Sentry when the client is setup.
        Discard when API key is not set or raven is not installed.
        """
        if hasattr(self.application, 'sentry_client'):
            return SentryMixin.log_exception(self, *args, **kwargs)
        return RequestHandler.log_exception(self, *args, **kwargs)

    def data_received(self, chunk):
        """
        Implement this method to handle streamed request data.
        """
        # this dummy implementation silences the pylint abstract-class error
