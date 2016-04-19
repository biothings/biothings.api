'''
A thin python layer for accessing MyVariant ElasticSearch host.

Currently available URLs:

    /v1/query?q=rs58991260            variant query service
    /v1/variant/<variant_id>    variant annotation service

'''
#import sys
import os.path
#import subprocess
#import json

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.escape
from tornado.options import define, options

from ..settings import BiothingSettings
btsettings = BiothingSettings()

__USE_WSGI__ = False

define("port", default=8000, help="run on the given port", type=int)
define("address", default="127.0.0.1", help="run on localhost")
define("debug", default=False, type=bool, help="run in debug mode")
tornado.options.parse_command_line()
if options.debug:
    import tornado.autoreload
    import logging
    logging.getLogger().setLevel(logging.DEBUG)
    options.address = '0.0.0.0'

settings = {}
if options.debug:
    # from config import STATIC_PATH
    settings.update({
        "static_path": btsettings.static_path,
    })
    # from config import auth_settings
    # settings.update(auth_settings)

def get_app(APP_LIST):
    return tornado.web.Application(APP_LIST, **settings)

def main(APP_LIST):
    application = get_app(APP_LIST)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port, address=options.address)
    loop = tornado.ioloop.IOLoop.instance()
    if options.debug:
        tornado.autoreload.start(loop)
        tornado.autoreload.watch(os.path.join(btsettings.static_path, 'index.html'))
        logging.info('Server is running on "%s:%s"...' % (options.address, options.port))

    loop.start()
