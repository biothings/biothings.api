"""
    A Biothings API demo implementation that supports command line arguments.

    Command line arguments are processed by tornado.options
    The following arguments are defined:

    * ``port``: the port to start the API on, **default** 8000
    * ``address``: the address to start the API on, **default** 127.0.0.1
    * ``debug``: start the API in debug mode, **default** False
    * ``dir``: path to app directory. **default**: current working directory

"""
import os
import sys

from tornado.options import define, options, Error

from biothings.web import BiothingsAPI

__USE_WSGI__ = False

define("port", default=8000, type=int, help="run on the given port")
define("address", default="127.0.0.1", help="host address to listen to")
define("debug", default=False, type=bool, help="debug settings like detailed logging")
define("dir", default=os.getcwd(), type=str, help="path to app directory and config")

try:
    options.parse_command_line()
except Error:
    pass
else:
    APP_PATH = os.path.abspath(options.dir)
    if APP_PATH not in sys.path:
        sys.path.append(APP_PATH)


def main(biothings_config='config', app_settings=None, use_curl=False):
    """ Start a Biothings API Server

        :param biothings_config: the biothings web config module to use
        :param app_settings: `Tornado application settings
        <http://www.tornadoweb.org/en/stable/web.html#tornado.web.Application.settings>`_
        :param use_curl: Overide the default simple_httpclient with curl_httpclient
        <https://www.tornadoweb.org/en/stable/httpclient.html>
    """
    app_settings = app_settings or {}
    api = BiothingsAPI(biothings_config)

    if app_settings:
        api.settings.update(app_settings)
    if use_curl:
        api.use_curl()

    api.debug(options.debug)
    api.host = options.address
    api.start(options.port)


if __name__ == '__main__':
    main()
