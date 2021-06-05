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
    def biothings(self):
        return self.application.biothings

    # ------------
    #  legacy API
    # ------------
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
        return logging.getLogger("biothings.web.handlers")

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


class StatusHandler(BaseHandler):
    '''
    Handles requests to check the status of the server.
    Use set_status instead of raising exception so that
    no error will be propogated to sentry monitoring. # TODO IS IT A GOOD DESIGN?
    '''

    def head(self):
        return self._check()

    async def get(self):

        dev = self.get_argument('dev', None)
        res = await self._check(dev is not None)
        self.finish(res)

    async def _check(self, dev=False):

        try:  # some db connections support async operations
            response = await self.biothings.health.async_check(info=dev)
        except (AttributeError, NotImplementedError):
            response = self.biothings.health.check()

        if not dev:
            return {
                # this endpoint might be accessed frequently,
                # keep the default response minimal. This is
                # especially useful when the document payload
                # is very large. Also useful when the automated
                # healch check only support GET requests.
                "success": True,
                "status": response.get("status")
            }

        return dict(response)

class FrontPageHandler(BaseHandler):

    def get(self):

        self.render(
            template_name="home.html",
            alert='Front Page Not Configured.',
            title='Biothings API',
            contents=self.application.biothings.handlers.keys(),
            support=self.biothings.config.BIOTHING_TYPES,
            url='http://biothings.io/'
        )
