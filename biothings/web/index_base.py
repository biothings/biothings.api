'''BioThings API ioloop start utilities.

This module contains functions to configure and start the `base event loop <http://www.tornadoweb.org/en/stable/ioloop.html>`_ from command line args.  Command line processing is done using tornado.options, with the following arguments defined:

    * ``port``: the port to start the API on, **default** 8000
    * ``address``: the address to start the API on, **default** 127.0.0.1
    * ``debug``: start the API in debug mode, **default** False
    * ``appdir``: path to API configuration directory, **default**: current working directory

    The **main** function is the boot script for all BioThings API webservers.
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

    def AsyncSentryClient(*args, **kwargs):
        pass

__USE_WSGI__ = False

define("port", default=8000, help="run on the given port", type=int)
define("address", default="127.0.0.1", help="run on localhost")
define("debug", default=False, type=bool, help="run in debug mode")
define("appdir", default=os.getcwd(), type=str,
       help="path to app directory containing (at minimum) a config module")

try:
    options.parse_command_line()
except BaseException:
    pass

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
    ''' Return an Application instance. '''
    return tornado.web.Application(APP_LIST, **settings)

def main(APP_LIST, app_settings={}, debug_settings={}, sentry_client_key=None, use_curl=False):
    ''' Main ioloop configuration and start

        :param APP_LIST: a list of `URLSpec objects or (regex, handler_class) tuples <http://www.tornadoweb.org/en/stable/web.html#tornado.web.Application>`_
        :param app_settings: `Tornado application settings <http://www.tornadoweb.org/en/stable/web.html#tornado.web.Application.settings>`_
        :param debug_settings: Additional application settings for API debug mode
        :param sentry_client_key: Application-specific key for attaching Sentry monitor to the application
        :param use_curl: Overide the default simple_httpclient with curl_httpclient (Useful for Github Login) <https://www.tornadoweb.org/en/stable/httpclient.html>
    '''
    settings = app_settings
    if options.debug:
        settings.update(debug_settings)
        settings.update({"debug": True})
    application = get_app(APP_LIST, **settings)
    if __USE_SENTRY__ and sentry_client_key:
        application.sentry_client = AsyncSentryClient(sentry_client_key)
    if use_curl:
        tornado.httpclient.AsyncHTTPClient.configure(
            "tornado.curl_httpclient.CurlAsyncHTTPClient")
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port, address=options.address)
    loop = tornado.ioloop.IOLoop.instance()
    if options.debug:
        tornado.autoreload.start(loop)
        logging.info('Server is running on "%s:%s"...' % (options.address, options.port))
    loop.start()


if __name__ == '__main__':
    from biothings.web.settings import BiothingESWebSettings
    main(BiothingESWebSettings().generate_app_list())
