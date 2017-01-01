'''
    Base event loop function.
'''
import sys
import os
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.escape
from tornado.options import define, options

__USE_SENTRY__ = True
try:
    from raven.contrib.tornado import AsyncSentryClient
except ImportError:
    __USE_SENTRY__ = False
    def AsyncSentryClient(**kwargs):
        pass

__USE_WSGI__ = False

define("port", default=8000, help="run on the given port", type=int)
define("address", default="127.0.0.1", help="run on localhost")
define("debug", default=False, type=bool, help="run in debug mode")
define("appdir", default=os.getcwd(), type=str, help="path to app directory containing (at minimum) a config module")

options.parse_command_line()

# assume config file is root of appdir
src_path = os.path.abspath(options.appdir)
if src_path not in sys.path:
    sys.path.append(src_path)

if options.debug:
    import tornado.autoreload
    import logging
    logging.getLogger().setLevel(logging.DEBUG)
    options.address = '0.0.0.0'

def get_app(APP_LIST, **settings):
    return tornado.web.Application(APP_LIST, **settings)

def main(APP_LIST, app_settings={}, debug_settings={}, sentry_client_key=None):
    settings = app_settings
    if options.debug:
        settings.update(debug_settings)
    application = get_app(APP_LIST, **settings)
    if __USE_SENTRY__ and sentry_client_key:
       application.sentry_client = AsyncSentryClient(sentry_client_key)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port, address=options.address)
    loop = tornado.ioloop.IOLoop.instance()
    if options.debug:
        tornado.autoreload.start(loop)
        logging.info('Server is running on "%s:%s"...' % (options.address, options.port))
    loop.start()
