"""
    Web Handler Class

    * Can be used for both API handlers and web rendering
    * Report errors to sentry when its API key is provided
"""

import logging

from elasticsearch.exceptions import ElasticsearchException
from tornado.web import RequestHandler

from .templates import FRONT_PAGE_TEMPLATE

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

    @property
    def web_settings(self):
        try:
            setting = self.settings['biothings']
        except KeyError:
            # need to pass it to tornado application settings
            self.require_setting('biothings')
        else:
            return setting

    def get_sentry_client(self):
        """
        Override default and retrieve from tornado setting instead.
        """
        client = self.settings.get('sentry_client')
        if not client:
            self.require_setting('sentry_client')
        return client

    @property
    def logger(self):
        return logging.getLogger(__name__)

    def log_exception(self, *args, **kwargs):
        """
        Only attempt to report to Sentry when the client is setup.
        Discard when API key is not set or raven is not installed.
        """
        if 'sentry_client' in self.settings:
            return SentryMixin.log_exception(self, *args, **kwargs)
        return RequestHandler.log_exception(self, *args, **kwargs)

    def data_received(self, chunk):
        """
        Implement this method to handle streamed request data.
        """
        # this dummy implementation silences the pylint abstract-class error


class StatusHandler(BaseHandler):
    '''
    Handles requests to check the status of the server.
    Use set_status instead of raising exception so that
    no error will be propogated to sentry monitoring.
    '''

    def head(self):
        return self._check(False)

    def get(self):
        return self._check()

    async def _check(self, body=True):

        client = self.web_settings.async_es_client
        payload = self.web_settings.STATUS_CHECK

        status = None  # green, red, yellow
        res = None  # additional doc check

        try:
            health = await client.cluster.health()
            status = health['status']
            if payload:
                res = await client.get(**payload)

        except ElasticsearchException:
            self.set_status(503)

        else:
            if status == 'red':
                self.set_status(503)
            if payload and not res:
                self.set_status(503)
        finally:
            if body:
                self.write({
                    "code": self.get_status(),
                    "status": status,
                    "payload": payload,
                    "response": res
                })

class FrontPageHandler(BaseHandler):

    def get(self):

        self.write(FRONT_PAGE_TEMPLATE.format(
            alert='Front Page Not Configured.',
            title='Biothings API',
            text='<br />'.join(self.web_settings.handlers.keys()),
            footnote='Supported types: '
            + ', '.join(self.web_settings.ES_INDICES.keys()),
            url='http://biothings.io/'
        ))
