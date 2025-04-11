"""
Biothings API Launcher

In this module, we have three framework-specific launchers
and a command-line utility to provide both programmatic and
command-line access to start Biothings APIs.

"""

import logging
import os
import sys
from pprint import pformat

import tornado.httpserver
import tornado.ioloop
import tornado.log
import tornado.web
from tornado.options import define, options

from biothings import __version__
from biothings.web.applications import BiothingsAPI
from biothings.web.settings import configs

logger = logging.getLogger(__name__)


class BiothingsAPIBaseLauncher:
    def __init__(self, config=None):
        # see biothings.web.settings.configs.load_module
        # for all supported ways to specify a config module
        logging.info("Biothings API %s", __version__)
        self.config = configs.load(config)  # for biothings APIs
        self.settings = dict(debug=False)  # for web frameworks

    def get_app(self):
        raise NotImplementedError()

    def get_server(self):
        raise NotImplementedError()

    def start(self, port=8000):
        raise NotImplementedError()


class TornadoAPILauncher(BiothingsAPIBaseLauncher):
    # tornado uses its own event loop which is
    # a wrapper around the asyncio event loop

    def __init__(self, config=None):
        # About debug mode in tornado:
        # https://www.tornadoweb.org/en/stable/guide/running.html \
        # #debug-mode-and-automatic-reloading
        super().__init__(config)
        self.handlers = []  # additional handlers
        self.host = None

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

        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("elasticsearch").setLevel(logging.WARNING)
        logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)

        if self.settings["debug"]:
            root_logger.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)

    @staticmethod
    def use_curl():
        """
        Use curl implementation for tornado http clients.
        More on https://www.tornadoweb.org/en/stable/httpclient.html
        """
        tornado.httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

    def get_app(self):
        return BiothingsAPI.get_app(self.config, self.settings, self.handlers)

    def get_server(self):
        # Use case example:
        # Run API in an external event loop.
        app = self.get_app()
        logger.info("All Handlers:\n%s", pformat(app.biothings.handlers, width=200))
        return tornado.httpserver.HTTPServer(app, xheaders=True)

    def start(self, port=8000):
        self._configure_logging()

        http_server = self.get_server()
        http_server.listen(port, self.host)

        logger.info('Server is running on "%s:%s"...', self.host or "0.0.0.0", port)
        loop = tornado.ioloop.IOLoop.instance()
        loop.start()


# WSGI
class FlaskAPILauncher(BiothingsAPIBaseLauncher):
    # Proof of concept
    # Not fully implemented

    # Create the following file under an application folder
    # to serve the application with a WSGI HTTP Server
    # like Gunicorn or use with AWS Elastic Beanstalk *

    # - application.py
    # from biothings.web.launcher import FlaskAPILauncher
    # application = FlaskAPILauncher("config").get_app()

    # * https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/create-deploy-python-apps.html

    def get_app(self):
        from biothings.web.applications import FlaskBiothingsAPI

        return FlaskBiothingsAPI.get_app(self.config)

    def get_server(self):
        raise NotImplementedError()
        # https://flask.palletsprojects.com/en/2.0.x/deploying/wsgi-standalone/
        # from gevent.pywsgi import WSGIServer
        # return WSGIServer(('', 5000), self.get_app())

    def start(self, port=8000, dev=True):
        if dev:
            app = self.get_app()
            app.run(port=port)

        # example implementation
        # for gevent WSGI server
        else:
            server = self.get_server()
            server.serve_forever()


# ASGI
class FastAPILauncher(BiothingsAPIBaseLauncher):
    # Proof of concept
    # Not fully implemented

    # from biothings.web.launcher import FastAPILauncher
    # app = FastAPILauncher("config").get_app()

    # >>> uvicorn main:app --host 0.0.0.0 --port 80
    # INFO: Uvicorn running on http://0.0.0.0:80 (Press CTRL+C to quit)

    def get_app(self):
        from biothings.web.applications import FastAPIBiothingsAPI

        return FastAPIBiothingsAPI.get_app(self.config)


BiothingsAPILauncher = TornadoAPILauncher


# Command Line Utilities
# --------------------------

define("port", default=8000, help="run on the given port")
define("debug", default=False, help="debug settings like logging preferences")
define("address", default=None, help="host address to listen to, default to all interfaces")
define("autoreload", default=False, help="auto reload the web server when file change detected")
define("framework", default="tornado", help="the web freamework to start a web server")
define("conf", default="config", help="specify a config module name to import")
define("dir", default=os.getcwd(), help="path to app directory that includes config.py")


def main(app_handlers=None, app_settings=None, use_curl=False):
    """Start a Biothings API Server"""

    options.parse_command_line()
    _path = os.path.abspath(options.dir)
    if _path not in sys.path:
        sys.path.append(_path)
    del _path

    app_handlers = app_handlers or []
    app_settings = app_settings or {}

    if options.framework == "tornado":
        launcher = TornadoAPILauncher(options.conf)
    elif options.framework == "flask":
        launcher = FlaskAPILauncher(options.conf)
    elif options.framework == "fastapi":
        launcher = FastAPILauncher(options.conf)
    else:  # there are only three supported frameworks for now
        raise ValueError("Unsupported framework.")

    try:
        if app_settings:
            launcher.settings.update(app_settings)
        if app_handlers:
            launcher.handlers = app_handlers
        if use_curl:
            launcher.use_curl()

        launcher.host = options.address
        launcher.settings.update(debug=options.debug)
        launcher.settings.update(autoreload=options.autoreload)
    except Exception:
        pass

    launcher.start(options.port)


if __name__ == "__main__":
    main()
