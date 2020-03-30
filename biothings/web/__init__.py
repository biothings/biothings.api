"""
    Biothings Web API

    - Support running behind a load balancer. Understand x-headers.
    TODO
"""
import logging

import tornado.httpserver
import tornado.ioloop
import tornado.web

from biothings.web.settings import BiothingESWebSettings

try:
    from raven.contrib.tornado import AsyncSentryClient
except ImportError:
    __SENTRY_INSTALLED__ = False
else:
    __SENTRY_INSTALLED__ = True

class BiothingsAPI():
    """
    Configure a Biothings Web API.

    There are three parts to it:
    * A biothings config module that defines the API.
    * Tornado settings that control web behaviors.
    * An asyncio event loop to run tornado.

    The API can be started with:
    * An external event loop by calling get_server()
    * A default tornado event loop by calling start()
    """

    def __init__(self, config=None, **settings):
        self.config = config
        self.settings = dict(autoreload=False)
        self.settings.update(settings)
        self.host = '127.0.0.1'

    @staticmethod
    def use_curl():
        """
        Use curl implementation for tornado http clients.
        More on https://www.tornadoweb.org/en/stable/httpclient.html
        """
        tornado.httpclient.AsyncHTTPClient.configure(
            "tornado.curl_httpclient.CurlAsyncHTTPClient")

    def use_sentry(self, sentry_client_key):
        """
        Setup error monitoring with Sentry. More on:
        https://docs.sentry.io/clients/python/integrations/#tornado
        """
        if __SENTRY_INSTALLED__:
            self.settings['sentry_client'] = \
                AsyncSentryClient(sentry_client_key)

    def update(self, **settings):
        """
        Update Tornado application settings. More on:
        https://www.tornadoweb.org/en/stable/web.html \
        #tornado.web.Application.settings
        """
        self.settings.update(settings)

    def debug(self, debug=True):
        """
        Configure API to run in debug mode.
        * listen on all interfaces.
        * log debug level message.
        * enable autoreload etc.
        """
        # About debug mode in tornado:
        # https://www.tornadoweb.org/en/stable/guide/running.html \
        # #debug-mode-and-automatic-reloading
        if debug:
            self.host = '0.0.0.0'
            self.settings.update({"debug": True})
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            self.host = '127.0.0.1'
            self.settings.update({"debug": False})
            logging.getLogger().setLevel(logging.WARNING)

    def get_server(self, config=None, **settings):
        """
        Run API in an external event loop.
        """
        api = BiothingESWebSettings(config or self.config)
        app = tornado.web.Application(api.generate_app_list(), **settings)
        server = tornado.httpserver.HTTPServer(app, xheaders=True)
        return server

    def start(self, port=8000):
        """
        Run API in the default event loop.
        """
        http_server = self.get_server(**self.settings)
        http_server.listen(port, self.host)

        logging.debug('Server is running on "%s:%s"...',
                      self.host, port)

        loop = tornado.ioloop.IOLoop.instance()
        loop.start()


BiothingsAPIApp = BiothingsAPI
