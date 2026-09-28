"""
In-memory hub shell and commands for the terminal tests (test_terminal.py, test_terminal_cli.py)
"""

from collections import OrderedDict
from types import SimpleNamespace

from biothings.hub.terminal import HubTerminal
from biothings.utils.hub import CommandDefinition, HubShell, pending


class FakeShell:
    """
    HubShell without IPython and hub db: reuses HubShell's command registration,
    tracking and help, and records saved commands in memory
    """

    launched_commands = {}
    pending_outputs = {}
    cmd_cnt = 1
    saved = {}

    set_commands = HubShell.set_commands
    add_command = HubShell.add_command
    register_command = HubShell.register_command
    extract_command_name = HubShell.extract_command_name
    help = HubShell.help
    eval = HubShell.eval
    restart = HubShell.restart
    stop = HubShell.stop
    command_info = classmethod(HubShell.command_info.__func__)
    refresh_commands = classmethod(HubShell.refresh_commands.__func__)

    @classmethod
    def save_cmd(cls, _id, cmd):
        cls.saved[_id] = dict(cmd)

    def __init__(self, commands=None, extra_commands=None):
        self.__class__.launched_commands = {}
        self.__class__.pending_outputs = {}
        self.__class__.cmd_cnt = 1
        self.__class__.saved = {}
        self.commands = OrderedDict()
        self.hidden = {}
        self.tracked = {}
        self.extra_ns = OrderedDict()
        self.job_manager = SimpleNamespace(loop=None)
        self.shellog = SimpleNamespace(input=lambda *args: None, output=lambda *args: None)
        self.last_std_contents = None
        self.set_commands(OrderedDict(commands or {}), OrderedDict(extra_commands or {}))


class Sources:
    """Minimal stand-in for the dump/upload managers"""

    def __init__(self):
        self.calls = []

    def dump(self, src, force=False, skip_manual=False, **kwargs):
        """Dump a source"""
        self.calls.append(("dump", src, force, kwargs))
        return {"dumped": src, "force": force}

    def dump_all(self, force=False, **kwargs):
        """Run all dumpers, except manual ones"""
        self.calls.append(("dump_all", force, kwargs))
        return "dumped all"

    def upload(self, src, validate=False):
        """Upload a source"""
        self.calls.append(("upload", src, validate))
        return "uploaded %s" % src


def make_terminal(**extra):
    sources = Sources()
    commands = {
        "dump": sources.dump,
        "dump_all": sources.dump_all,
        "upload": sources.upload,
        "status": CommandDefinition(command=lambda: {"source": {"total": 2}}, tracked=False),
        "index_config": {"env": {"local": {}}},  # not callable, not a command for the terminal
    }
    commands.update(extra)
    extra_commands = {
        "pending": CommandDefinition(command=pending, tracked=False),
        "sch": CommandDefinition(command=lambda: "no schedule", tracked=False),
    }
    shell = FakeShell(commands, extra_commands)
    terminal = HubTerminal(shell, hooks_folder="./hooks")
    return terminal, shell, sources
