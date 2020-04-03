"""
    Biothings Web API
"""
import logging

import tornado.httpserver
import tornado.ioloop
import tornado.web

from biothings.web.settings import BiothingESWebSettings


class BiothingsAPI():
    """
    Configure a Biothings Web API Server.

    There are three parts to it:
    * A biothings config module that defines the API handlers.
    * Additional Tornado settings like autoreload and logging.
    * An asyncio event loop to run the tornado application.

    The API can be started with:
    * An external event loop by calling get_server()
    * A default tornado event loop by calling start()

    """

    def __init__(self, config_module=None):
        self.config = BiothingESWebSettings(config_module)
        self.settings = dict(autoreload=False)
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

    def get_server(self):
        """
        Run API in an external event loop.
        """
        settings = dict(self.settings)
        settings.update(self.config.generate_app_settings())
        handlers = self.config.generate_app_handlers()
        app = tornado.web.Application(handlers, **settings)
        server = tornado.httpserver.HTTPServer(app, xheaders=True)
        return server

    def start(self, port=8000):
        """
        Run API in the default event loop.
        """
        http_server = self.get_server()
        http_server.listen(port, self.host)

        logging.debug('Server is running on "%s:%s"...',
                      self.host, port)

        loop = tornado.ioloop.IOLoop.instance()
        loop.start()


BiothingsAPIApp = BiothingsAPI
