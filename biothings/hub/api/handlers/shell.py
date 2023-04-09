import json.decoder

import tornado.web

from biothings.utils.hub import CommandError, CommandNotAllowed, NoSuchCommand

from .base import GenericHandler


class ShellHandler(GenericHandler):
    def initialize(self, shell, shellog, **kwargs):
        super().initialize(shell, **kwargs)
        self.shellog = shellog

    def put(self):
        try:
            bodyargs = tornado.escape.json_decode(self.request.body or "{}")
            cmd = bodyargs["cmd"]
        except KeyError:
            raise tornado.web.HTTPError(400, reason="Bad Request oula (Missing argument cmd)")
        except json.decoder.JSONDecodeError:
            raise tornado.web.HTTPError(400, reason="Invalid JSON payload")

        try:
            outs = self.shell.eval(cmd, secure=True)  # only pre-defined command
            for out in outs:
                if out != "":
                    self.shellog.output(out)
        except CommandError as e:
            raise tornado.web.HTTPError(400, reason="Error: %s" % e)
        except NoSuchCommand as e:
            raise tornado.web.HTTPError(404, reason="No such command: %s" % e)
        except CommandNotAllowed as e:
            raise tornado.web.HTTPError(403, reason="Command not allowed: %s" % e)
