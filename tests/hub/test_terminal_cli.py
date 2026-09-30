"""
Tests for "biothings-cli hub", against a Hub API served from a thread (terminal
handlers with an in-memory hub shell, see terminal_fakes.py)
"""

import asyncio
import json
import logging
import sys
import textwrap
import threading
import time
from types import SimpleNamespace

import pytest
import tornado.httpserver
import tornado.ioloop
import tornado.netutil
import tornado.web
from terminal_fakes import make_terminal
from typer.testing import CliRunner

import biothings.cli
from biothings.cli.commands.hub import hub_application, print_response, render
from biothings.hub.api import EndpointDefinition, generate_api_routes
from biothings.hub.api.handlers.base import RootHandler
from biothings.hub.api.handlers.terminal import TerminalCommandsHandler, TerminalRunHandler


@pytest.fixture(autouse=True)
def no_logging():
    """
    A log record emitted by the hub thread while CliRunner captures the output makes pytest's
    live logging (log_cli) reset sys.stdout, breaking CliRunner's capture: silence logging
    """
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def serve(routes, shell=None):
    """Run a tornado app in a thread, returns (url, stop function)"""
    loop = asyncio.new_event_loop()
    started = threading.Event()
    state = {}

    def run():
        asyncio.set_event_loop(loop)

        async def start():
            sockets = tornado.netutil.bind_sockets(0, "127.0.0.1")
            server = tornado.httpserver.HTTPServer(tornado.web.Application(routes))
            server.add_sockets(sockets)
            state["port"] = sockets[0].getsockname()[1]
            if shell is not None:
                # the hub refreshes launched commands status every second
                tornado.ioloop.PeriodicCallback(shell.refresh_commands, 20).start()
            started.set()

        loop.run_until_complete(start())
        loop.run_forever()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert started.wait(5)

    def stop():
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)

    return "http://127.0.0.1:%s" % state["port"], stop


@pytest.fixture
def hub(tmp_path):
    async def slow(name, delay=0.05):
        """Take some time"""
        await asyncio.sleep(delay)
        return {"slow": name}

    def boom(what):
        """Always fail"""
        raise ValueError("bad %s" % what)

    terminal, shell, sources = make_terminal(slow=slow, boom=boom, rmmerge=lambda merge_name: "deleted %s" % merge_name)
    hook = tmp_path / "greetings.py"
    hook.write_text(textwrap.dedent('''
            def greet(name, excited=False):
                """Greet someone"""
                return "Hello %s%s" % (name, "!" if excited else "")
            '''))
    terminal.load_hook(str(hook))
    broken = tmp_path / "broken.py"
    broken.write_text("raise RuntimeError('broken hook')\n")
    with pytest.raises(RuntimeError):
        terminal.load_hook(str(broken))
    routes = [
        ("/", RootHandler, {"features": ["terminal", "ws"], "hub_name": "Test Hub"}),
        ("/terminal/commands", TerminalCommandsHandler, {"terminal": terminal}),
        ("/terminal/run", TerminalRunHandler, {"terminal": terminal}),
    ]
    routes += generate_api_routes(
        shell,
        {
            "command": EndpointDefinition(name="command", method="get"),
            "commands": EndpointDefinition(name="commands", method="get"),
        },
    )
    url, stop = serve(routes, shell)
    yield SimpleNamespace(url=url, terminal=terminal, shell=shell, sources=sources)
    stop()


def cli(hub, *args, **kwargs):
    return CliRunner().invoke(hub_application, ["--url", hub.url, *args], **kwargs)


def test_commands_lists_builtin_and_hook_commands(hub):
    result = cli(hub, "commands")
    assert result.exit_code == 0, result.output
    assert "Built-in commands" in result.output
    assert "dump_all" in result.output and "Download the data of all the sources" in result.output
    assert "Hook commands" in result.output
    assert "greet" in result.output and "(greetings.py)" in result.output
    assert "sch" not in result.output.split()  # hidden command
    assert "sch" in cli(hub, "commands", "--all").output.split()
    assert json.loads(cli(hub, "commands", "--json").output)["hooks_folder"] == "./hooks"


def test_help_shows_usage(hub):
    result = cli(hub, "help", "dump-all")
    assert result.exit_code == 0, result.output
    assert "Usage: dump_all [--force] [--<option> <value>...]" in result.output
    assert "Download the data of all the sources, except manual ones" in result.output
    assert "Examples:\n  dump all\n  dump all --force" in result.output
    assert "Run all dumpers, except manual ones" in result.output  # docstring
    result = cli(hub, "help", "greet")
    assert "hook file greetings.py" in result.output
    assert cli(hub, "help", "nope").exit_code == 2


def test_run_sync_command(hub):
    result = cli(hub, "run", "dump", "mygene", "--force")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"dumped": "mygene", "force": True}
    assert hub.sources.calls[-1] == ("dump", "mygene", True, {})
    result = cli(hub, "run", "greet world --excited")  # whole command line in one argument
    assert (result.exit_code, result.output) == (0, "Hello world!\n")
    result = cli(hub, "run", "dump all")
    assert (result.exit_code, result.output) == (0, "dumped all\n")
    result = cli(hub, "run", "--json", "upload('mygene')")
    assert json.loads(result.output)["result"] == "uploaded mygene"


def test_run_waits_for_async_commands(hub):
    result = cli(hub, "run", "--interval", "0.01", "slow", "mygene")
    assert result.exit_code == 0, result.output
    assert "[#1] slow('mygene') done" in result.output
    assert '"slow": "mygene"' in result.output


def test_run_no_wait_and_status(hub):
    result = cli(hub, "run", "--no-wait", "slow", "mygene", "--delay", "1")
    assert result.exit_code == 0, result.output
    assert "[#1] slow('mygene', delay=1.0) started" in result.output
    status = cli(hub, "status", "--running")
    assert "running" in status.output and "slow('mygene', delay=1.0)" in status.output
    assert "running" in cli(hub, "status", "1").output
    for _ in range(500):
        if hub.shell.launched_commands[1].get("is_done"):
            break
        time.sleep(0.02)
    result = cli(hub, "status", "1")
    assert result.exit_code == 0, result.output
    assert "[#1] slow('mygene', delay=1.0) done" in result.output
    assert "done" in cli(hub, "status").output


def test_run_failures(hub):
    result = cli(hub, "run", "boom", "input")
    assert result.exit_code == 1
    assert "ValueError: bad input" in result.output
    assert "Traceback" not in result.output
    assert "Traceback" in cli(hub, "run", "--verbose", "boom", "input").output
    result = cli(hub, "run", "dump_al")
    assert result.exit_code == 2
    assert "Unknown command 'dump_al'" in result.output
    result = cli(hub, "run", "dump")
    assert result.exit_code == 2
    assert "missing a required argument: 'src'" in result.output
    assert "Usage: dump <src> [--force] [--skip-manual] [--<option> <value>...]" in result.output


def test_run_asks_for_a_confirmation(hub):
    result = cli(hub, "run", "rmmerge", "mygene_1")
    assert result.exit_code == 2  # can't be asked: not an interactive terminal
    assert "rmmerge('mygene_1'): Delete a build" in result.output
    assert "use --yes" in result.output
    result = cli(hub, "run", "--yes", "rmmerge", "mygene_1")
    assert (result.exit_code, result.stdout) == (0, "deleted mygene_1\n")


def test_hooks(hub):
    result = cli(hub, "hooks")
    assert result.exit_code == 1  # a hook failed to load
    assert "greetings.py: greet" in result.output
    assert "broken.py failed to load" in result.output
    assert "RuntimeError: broken hook" in result.output


def test_interactive_shell(hub):
    lines = ["help", "help greet", "greet world", "dump_al", "slow mygene --delay 0.01", "jobs", "", "exit"]
    result = cli(hub, "shell", input="\n".join(lines) + "\n")
    assert result.exit_code == 0, result.output
    assert "Connected to Test Hub" in result.output
    assert "From hook files: greet" in result.output
    assert "Usage: greet <name> [--excited]" in result.output
    assert "Hello world" in result.output
    assert "Unknown command 'dump_al'" in result.output
    assert "] slow('mygene', delay=0.01) running in background" in result.output


def test_interactive_shell_asks_for_confirmations(hub):
    lines = ["rmmerge mygene_1", "y", "rmmerge mygene_2", "n", "exit"]
    result = cli(hub, "shell", input="\n".join(lines) + "\n")
    assert result.exit_code == 0, result.output
    assert result.output.count("Delete a build") == 2
    assert "deleted mygene_1" in result.output
    assert "deleted mygene_2" not in result.output


def test_unreachable_and_older_hubs():
    result = CliRunner().invoke(hub_application, ["--url", "http://127.0.0.1:9", "run", "status"])
    assert result.exit_code == 1
    assert "Can't reach the hub at http://127.0.0.1:9" in result.output
    # hubs running an older BioThings version don't have the terminal endpoints
    url, stop = serve([("/", RootHandler, {"features": ["ws"]})])
    try:
        result = CliRunner().invoke(hub_application, ["--url", url, "commands"])
        assert result.exit_code == 1
        assert "doesn't provide the terminal API" in result.output
    finally:
        stop()


def test_hub_commands_ignore_local_configuration(tmp_path, monkeypatch, capsys):
    """biothings-cli is often run from a hub's folder, whose config.py is the hub's own: hub commands don't load it"""
    (tmp_path / "config.py").write_text("from config_hub import *  # can't be imported by the CLI\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["biothings-cli", "hub", "--url", "http://127.0.0.1:9", "run", "status"])
    monkeypatch.setattr(sys, "tracebacklimit", 1000, raising=False)  # changed by the CLI
    with pytest.raises(SystemExit) as exit_info:
        biothings.cli.main()
    assert exit_info.value.code == 1
    assert "Can't reach the hub at http://127.0.0.1:9" in capsys.readouterr().err


def test_render_hides_empty_results():
    assert render(None) == ""
    assert render([[None, None]]) == ""  # eg. dump_all(): one None per job
    assert render([]) == "[]"
    assert render({"a": 1}) == '{\n  "a": 1\n}'
    assert render("text") == "text"


def test_logs_are_printed_on_stderr(capsys):
    response = {"logs": ["Archiving build covid19_1"], "stdout": "", "stderr": "", "failed": False, "result": "done"}
    assert print_response(response) is True
    captured = capsys.readouterr()
    assert captured.out == "done\n"
    assert "Archiving build covid19_1" in captured.err
