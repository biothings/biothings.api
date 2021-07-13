"""
    A simple Biothings API implementation.

    * Process command line arguments to setup the API.
    * Add additional applicaion settings like handlers.

    * ``port``: the port to start the API on, **default** 8000
    * ``debug``: start the API in debug mode, **default** False
    * ``address``: the address to start the API on, **default** 0.0.0.0
    * ``autoreload``: restart the server when file changes, **default** False
    * ``conf``: choose an alternative setting, **default** config
    * ``dir``: path to app directory. **default**: current working directory

"""
import logging
import os
import sys

import tornado.httpserver
import tornado.ioloop
import tornado.log
import tornado.web
from biothings import __version__
from biothings.web.applications import BiothingsAPI
from biothings.web.settings import configs
from tornado.options import define, options


logger = logging.getLogger(__name__)


class BiothingsAPILauncher():
    """
    Configure a Biothings Web API Server.

    There are three parts to it:
    * A biothings config module that defines the API handlers.
    * Additional Tornado handlers and application settings.
    * An asyncio event loop to run the tornado application.

    The API can be started with:
    * An external event loop by calling get_server()
    * A default tornado event loop by calling start()

    Unless started externally, debug mode:
    * Sets proper logging levels for root logger and es,
    * Enables debug mode on tornado except for autoreload,
    * Disables integrated tracking and error monitoring.
    """

    def __init__(self, config=None):
        # About debug mode in tornado:
        # https://www.tornadoweb.org/en/stable/guide/running.html \
        # #debug-mode-and-automatic-reloading
        logging.info("Biothings API %s", __version__)
        self.config = configs.load(config)
        self.handlers = []  # additional handlers
        self.settings = dict(debug=False)
        self.host = None

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

    def _configure_logging(self):
        root_logger = logging.getLogger()

        if isinstance(self.config, configs.ConfigPackage):
            config = self.config.root
        else:  # configs.ConfigModule
            config = self.config

        if hasattr(config, "LOGGING_FORMAT"):
            for handler in root_logger.handlers:
                if isinstance(handler.formatter, tornado.log.LogFormatter):
                    handler.formatter._fmt = config.LOGGING_FORMAT

        logging.getLogger('urllib3').setLevel(logging.ERROR)
        logging.getLogger('elasticsearch').setLevel(logging.WARNING)

        if self.settings['debug']:
            root_logger.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)

    def get_server(self):
        """
        Run API in an external event loop.
        """
        webapp = BiothingsAPI.get_app(self.config, self.settings, self.handlers)
        server = tornado.httpserver.HTTPServer(webapp, xheaders=True)
        return server

    def start(self, port=8000):
        """
        Run API in the default event loop.
        """
        self._configure_logging()

        http_server = self.get_server()
        http_server.listen(port, self.host)

        logger.info(
            'Server is running on "%s:%s"...',
            self.host or '0.0.0.0', port
        )
        loop = tornado.ioloop.IOLoop.instance()
        loop.start()


define("port", default=8000, help="run on the given port")
define("debug", default=False, help="debug settings like logging preferences")
define("address", default=None, help="host address to listen to, default to all interfaces")
define("autoreload", default=False, help="auto reload the web server when file change detected")
define("conf", default='config', help="specify a config module name to import")
define("dir", default=os.getcwd(), help="path to app directory that includes config.py")


def main(app_handlers=None, app_settings=None, use_curl=False):
    """ Start a Biothings API Server

        :param app_handlers: additional web handlers to add to the app
        :param app_settings: `Tornado application settings dictionary
        <http://www.tornadoweb.org/en/stable/web.html#tornado.web.Application.settings>`_
        :param use_curl: Overide the default simple_httpclient with curl_httpclient
        <https://www.tornadoweb.org/en/stable/httpclient.html>
    """
    # TODO this section might very likely have problems
    options.parse_command_line()
    _path = os.path.abspath(options.dir)
    if _path not in sys.path:
        sys.path.append(_path)
    del _path

    app_handlers = app_handlers or []
    app_settings = app_settings or {}
    launcher = BiothingsAPILauncher(options.conf)

    if app_settings:
        launcher.settings.update(app_settings)
    if app_handlers:
        launcher.handlers = app_handlers
    if use_curl:
        launcher.use_curl()

    launcher.host = options.address
    launcher.update(debug=options.debug)
    launcher.update(autoreload=options.autoreload)
    launcher.start(options.port)


if __name__ == '__main__':
    main()
