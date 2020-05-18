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

from tornado.options import Error, define, options

from biothings.web import BiothingsAPI

__USE_WSGI__ = False

define("port", default=8000, help="run on the given port")
define("debug", default=False, help="debug settings like logging preferences")
define("address", default=None, help="host address to listen to, default to all interfaces")
define("autoreload", default=False, help="auto reload the web server when file change detected")
define("conf", default='config', help="specify a config module name to import")
define("dir", default=os.getcwd(), help="path to app directory that includes config.py")

try:
    options.parse_command_line()
    _path = os.path.abspath(options.dir)
    if _path not in sys.path:
        sys.path.append(_path)
    del _path
except Error:
    pass  # TODO
else:
    pass


def main(app_handlers=None, app_settings=None, use_curl=False):
    """ Start a Biothings API Server

        :param app_handlers: additional web handlers to add to the app
        :param app_settings: `Tornado application settings dictionary
        <http://www.tornadoweb.org/en/stable/web.html#tornado.web.Application.settings>`_
        :param use_curl: Overide the default simple_httpclient with curl_httpclient
        <https://www.tornadoweb.org/en/stable/httpclient.html>
    """
    app_handlers = app_handlers or []
    app_settings = app_settings or {}
    api = BiothingsAPI(options.conf)

    if app_settings:
        api.settings.update(app_settings)
    if app_handlers:
        api.handlers = app_handlers
    if use_curl:
        api.use_curl()

    api.host = options.address
    api.update(debug=options.debug)
    api.update(autoreload=options.autoreload)
    api.start(options.port)


if __name__ == '__main__':
    main()
