"""
Biothings Web Handlers

biothings.web.handlers.BaseHandler

    Supports:
    - access to biothings.web.settings instance
    - access to biothings.web.handlers logging stream
    - access to default web templates
    - Sentry error monitoring

    Subclasses:
    - biothings.web.handlers.StatusHandler
    - biothings.web.handlers.FrontPageHandler
    - discovery.web.handlers.BaseHandler
    ...

Also available:

biothings.web.handlers.BaseAPIHandler
biothings.web.handlers.BaseESRequestHandler
biothings.web.handlers.ESRequestHandler

"""
import copy
import logging

from elasticsearch.exceptions import ElasticsearchException
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

    @property
    def web_settings(self):
        try:
            setting = self.settings['biothings']
        except KeyError:
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

    def get_template_path(self):
        if "template_path" in self.settings:
            return super().get_template_path()
        import biothings.web.templates
        return next(iter(biothings.web.templates.__path__))

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
        return self._check()

    async def get(self):
        res = await self._check()
        self.finish(res)

    async def _check(self):

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

        return {
            "code": self.get_status(),
            "status": status,
            "payload": payload,
            "response": res
        }


class FrontPageHandler(BaseHandler):

    def get(self):

        self.render(
            template_name="home.html",
            alert='Front Page Not Configured.',
            title='Biothings API',
            contents=self.web_settings.handlers.keys(),
            support=self.web_settings.BIOTHING_TYPES,
            url='http://biothings.io/'
        )


from .api import *
from .es import *
