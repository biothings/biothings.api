import json.decoder

import tornado.escape

from biothings.hub.terminal import CommandUsageError, ConfirmationRequired, UnknownCommand
from biothings.utils.hub import AlreadyRunningException, CommandError, CommandNotAllowed, NoSuchCommand

from .base import DefaultHandler


class TerminalCommandsHandler(DefaultHandler):
    """
    GET /terminal/commands: commands available from the terminal, built-in and defined
    in hook files, with their documentation and usage, and the hook files loaded
    """

    def initialize(self, terminal, **kwargs):
        self.terminal = terminal

    def get(self):
        self.write(self.terminal.catalog())


class TerminalRunHandler(DefaultHandler):
    """
    POST /terminal/run: run a command line, given either as a string with {"cmd": "dump mygene --force"},
    or as a list of arguments already split by a shell with {"argv": ["dump", "mygene", "--force"]}.

    Returns the command's result when it's done, or its ID when it runs in background (follow it
    with GET /command/<id>). A failing command isn't an HTTP error, it's reported with "failed": true.

    Commands deleting or overwriting data (see biothings.hub.terminal.CONFIRM) only run with
    "confirmed": true, otherwise the response is a 428 error listing what must be confirmed ("confirm").
    """

    def initialize(self, terminal, shellog=None, **kwargs):
        self.terminal = terminal
        self.shellog = shellog

    def post(self):
        try:
            body = tornado.escape.json_decode(self.request.body or "{}")
        except (json.decoder.JSONDecodeError, UnicodeDecodeError):
            return self.error(400, "Invalid JSON payload")
        if not isinstance(body, dict) or ("cmd" in body) == ("argv" in body):
            return self.error(400, "Expecting either 'cmd' (a command line) or 'argv' (a list of arguments)")
        line, argv = body.get("cmd"), body.get("argv")
        confirmed = body.get("confirmed") is True
        if self.shellog:
            self.shellog.input(line if line is not None else " ".join(map(str, argv or [])))
        try:
            result = self.terminal.run(line=line, argv=argv, confirmed=confirmed)
        except UnknownCommand as e:
            return self.error(404, str(e), suggestions=e.suggestions)
        except NoSuchCommand as e:
            return self.error(404, str(e))
        except CommandNotAllowed as e:
            return self.error(403, str(e))
        except AlreadyRunningException as e:
            return self.error(409, str(e))
        except CommandUsageError as e:
            return self.error(400, str(e), usage=e.usage)
        except ConfirmationRequired as e:
            return self.error(428, str(e), confirm=e.reasons)
        except CommandError as e:
            return self.error(400, str(e))
        self.write(result)

    def error(self, code, message, **details):
        self.set_status(code)
        payload = {"status": "error", "code": code, "error": message}
        payload.update({key: value for key, value in details.items() if value})
        # bypass DefaultHandler.write(), which reports the payload as a successful result
        super(DefaultHandler, self).write(payload)
