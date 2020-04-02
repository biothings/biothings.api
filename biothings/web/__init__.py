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

    def __init__(self, config_module=None, **settings):
        self.config = BiothingESWebSettings(config_module)
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
        api = BiothingESWebSettings(config) if config else self.config
        settings_ = dict(self.settings)
        settings_.update(settings)
        app = api.generate_app(settings_)
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
