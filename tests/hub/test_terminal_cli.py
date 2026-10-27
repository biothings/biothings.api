"""
Tests for "biothings-cli hub", against a Hub API served from a thread (terminal
handlers with an in-memory hub shell, see terminal_fakes.py)
"""

import asyncio
import http.server
import importlib.util
import json
import logging
import re
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

import biothings
import biothings.cli
import biothings.cli.commands.hub as hub_cli
from biothings.cli.commands.admin import build_admin_application
from biothings.cli.commands.hub import (
    HubAPIError,
    HubClient,
    HubTimeout,
    hub_application,
    print_command_results,
    print_response,
    render,
)
from biothings.hub.api import EndpointDefinition, generate_api_routes
from biothings.hub.api.handlers import auth
from biothings.hub.api.handlers.base import LoginHandler, RootHandler
from biothings.hub.api.handlers.terminal import TerminalCommandsHandler, TerminalRunHandler
from biothings.utils.hub import CommandDefinition
from biothings.utils.passwords import hash_password


@pytest.fixture(autouse=True)
def saved_logins(tmp_path, monkeypatch):
    """The logins saved by 'biothings-cli hub login' (not the user's own)"""
    path = tmp_path / "biothings_hub_logins"
    monkeypatch.setattr(hub_cli, "LOGINS_FILE", str(path))
    return path


@pytest.fixture
def hub_users(monkeypatch):
    """The hub requires a login to run commands (users in its HUB_API_USERS)"""
    users = {"alice": hash_password("alice-password", iterations=1000)}
    monkeypatch.setattr(auth, "config", SimpleNamespace(HUB_API_USERS=users, HUB_API_LOGIN_EXPIRY=3600))
    auth._failed_logins.clear()
    yield users
    auth._failed_logins.clear()


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


JOB_MANAGER = {  # the state of a hub's job manager, as returned by its API
    "queue": {
        "process": {
            "running": [24195],
            "pending": ["a", "b"],
            "all": {
                "24195": {
                    "memory": {"size": 397095731},
                    "job": {
                        "category": "uploader",
                        "source": "ctd",
                        "step": "update_data",
                        "description": "CTD_diseases.tsv.gz",
                        "started_at": 1759700000,
                        "duration": "5.26s",
                    },
                },
                "24196": {"memory": {"size": 1310720}},  # idle
            },
        },
        "thread": {"running": [], "pending": [], "all": {"Thread-1": {"is_alive": True}}},
    },
}


class JobManagerHandler(RootHandler):
    def initialize(self):
        pass

    def get(self):
        self.write(JOB_MANAGER)


@pytest.fixture
def hub(tmp_path):
    async def slow(name, delay=0.05):
        """Take some time"""
        await asyncio.sleep(delay)
        return {"slow": name}

    def boom(what):
        """Always fail"""
        raise ValueError("bad %s" % what)

    def dump_sources(delay=0.05):
        """Dump some sources, telling which ones are done (like dump_all)"""

        async def wait():
            for line in ["mondo done (1/2)", "hpo done (2/2)"]:
                await asyncio.sleep(delay)
                progress.append(line)
            return "dumped"

        progress = ["dump mondo, hpo"]
        task = asyncio.ensure_future(wait())
        task.progress = progress
        return task

    def busy(seconds=1.0):
        """Keep the hub busy: it can't answer meanwhile"""
        time.sleep(seconds)
        return "done"

    terminal, shell, sources = make_terminal(
        slow=slow,
        boom=boom,
        busy=busy,
        dump_sources=dump_sources,
        rmmerge=lambda merge_name: "deleted %s" % merge_name,
        inspection=CommandDefinition(command=lambda name: {"inspected": name}, hidden=True),  # eg. used by Studio
    )
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
        ("/job_manager", JobManagerHandler),
        ("/terminal/commands", TerminalCommandsHandler, {"terminal": terminal}),
        ("/terminal/run", TerminalRunHandler, {"terminal": terminal}),
        ("/login", LoginHandler),
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
    assert "dump_all" in result.output and "Download the data of all the data sources" in result.output
    assert "Hook commands" in result.output
    assert "greet" in result.output and "(greetings.py)" in result.output
    assert "sch" not in result.output.split()  # hidden command
    assert "sch" in cli(hub, "commands", "--all").output.split()
    assert json.loads(cli(hub, "commands", "--json").output)["hooks_folder"] == "./hooks"


def test_help_shows_usage(hub):
    result = cli(hub, "help", "dump-all")
    assert result.exit_code == 0, result.output
    assert "Usage: dump_all [--force] [--<option> <value>...]" in result.output
    assert "Download the data of all the data sources, except manual ones" in result.output
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


def test_run_no_wait_and_history(hub):
    result = cli(hub, "run", "--no-wait", "slow", "mygene", "--delay", "1")
    assert result.exit_code == 0, result.output
    assert "[#1] slow('mygene', delay=1.0) started" in result.output
    assert "Follow it with: biothings-cli hub history 1" in result.output
    history = cli(hub, "history", "--running")
    assert "running" in history.output and "slow('mygene', delay=1.0)" in history.output
    assert "running" in cli(hub, "history", "1").output
    for _ in range(500):
        if hub.shell.launched_commands[1].get("is_done"):
            break
        time.sleep(0.02)
    result = cli(hub, "history", "1")
    assert result.exit_code == 0, result.output
    assert "[#1] slow('mygene', delay=1.0) done" in result.output
    assert "done" in cli(hub, "history").output


def test_history_filters(hub):
    for line in ["dump mygene", "upload mygene", "inspection mygene", "upload myvariant"]:
        assert cli(hub, "run", line).exit_code == 0
    result = cli(hub, "history")
    assert result.exit_code == 0, result.output
    assert "dump('mygene')" in result.output and "upload('myvariant')" in result.output
    assert "inspection" not in result.output  # a hidden command
    assert "inspection('mygene')" in cli(hub, "history", "--all").output
    result = cli(hub, "history", "--search", "UPLOAD")
    assert "upload('mygene')" in result.output and "upload('myvariant')" in result.output
    assert "dump" not in result.output
    assert "No command found" in cli(hub, "history", "-s", "nothing").output
    assert len(json.loads(cli(hub, "history", "--json", "-s", "mygene").output)) == 2
    # its former name, easily mixed up with "run status"
    assert "upload('myvariant')" in cli(hub, "status").output


def untimed(output):
    """Lines of an output, without the times added when it's not a terminal (eg. "[16:52:58] mondo done")"""
    return [re.sub(r"^\[[\d:-]+( [\d:]+)?\] ", "", line) for line in output.splitlines()]


def test_run_prints_progress(hub):
    result = cli(hub, "run", "--interval", "0.01", "dump_sources")
    assert result.exit_code == 0, result.output
    lines = untimed(result.output)
    # not in a terminal (eg. a script): the command's ID right away, then its progress, timed
    assert lines[0] == "[#1] dump_sources() started"
    assert lines.index("dump mondo, hpo") < lines.index("mondo done (1/2)") < lines.index("hpo done (2/2)")
    assert re.search(r"^\[\d\d:\d\d:\d\d\] hpo done \(2/2\)$", result.output, re.M)
    assert result.stdout == "dumped\n"  # only the results: "[#1] ... done" is on stderr
    # and "history <id>" while it runs
    command_id = json.loads(cli(hub, "run", "--no-wait", "--json", "dump_sources", "--delay", "0.3").output)["id"]
    for _ in range(100):
        if "mondo done (1/2)" in (hub.shell.launched_commands[command_id].get("progress") or []):
            break
        time.sleep(0.02)
    output = cli(hub, "history", str(command_id)).output
    assert "[#%s] dump_sources(delay=0.3) running\ndump mondo, hpo\nmondo done (1/2)" % command_id in output
    for _ in range(100):
        if hub.shell.launched_commands[command_id].get("is_done"):
            break
        time.sleep(0.02)


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
        assert "biothings-cli hub history" in result.output
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


def test_verbose_prints_the_traceback_of_failed_commands(capsys):
    info = {"id": 3, "cmd": "dump_all()", "failed": True, "results": ["dump failed for 1 of 2 sources"]}
    info["traceback"] = "Traceback (most recent call last):\nTypeError: boom\n"
    assert print_command_results(info) is False
    assert "TypeError: boom" not in capsys.readouterr().err
    assert print_command_results(info, verbose=True) is False
    assert "TypeError: boom" in capsys.readouterr().err


def test_hub_timeouts_are_not_outages():
    """The hub was reached but didn't answer in time: unlike an unreachable hub, what was asked may be done"""

    class Slow(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            time.sleep(0.5)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Slow)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        client = HubClient("http://127.0.0.1:%s" % server.server_address[1], timeout=0.1)
        with pytest.raises(HubTimeout, match=r"didn't answer within 0.1s \(it may be busy\)"):
            client.run(line="upload umls")
    finally:
        server.shutdown()
    with pytest.raises(HubAPIError, match="Can't reach the hub") as error:
        HubClient("http://127.0.0.1:9").run(line="upload umls")
    assert not isinstance(error.value, HubTimeout)


def test_timeouts_say_the_command_may_have_started(hub):
    # (the hub runs in this process: while it runs "busy", what the CLI prints on stderr is captured
    # by the hub, as a command's output, until it's done)
    result = cli(hub, "run", "--timeout", "0.3", "busy", "--seconds", "1.5")
    assert result.exit_code == 1
    assert "The command may have started anyway" in result.output
    assert "[#1] busy(seconds=1.5) done" in result.output  # it did: no need to run it again
    assert "run --timeout <seconds>" in result.output
    result = cli(hub, "run", "--timeout", "60", "busy", "--seconds", "0")
    assert (result.exit_code, result.stdout) == (0, "done\n")


def test_run_options_go_before_the_command(hub):
    result = cli(hub, "run", "--timeoutt", "60", "dump", "all")
    assert result.exit_code == 2
    assert "Unknown option '--timeoutt' for run" in result.output
    assert hub.sources.calls == []  # not sent to the hub


def test_history_wait(hub):
    result = cli(hub, "run", "--no-wait", "dump_sources", "--delay", "0.1")
    assert "Follow it with: biothings-cli hub history 1 --wait" in result.output
    result = cli(hub, "history", "1", "--wait", "--interval", "0.01")
    assert result.exit_code == 0, result.output
    lines = untimed(result.output)
    assert lines.index("dump mondo, hpo") < lines.index("hpo done (2/2)")
    assert lines[-2].startswith("[#1] dump_sources(delay=0.1) done in") and lines[-1] == "dumped"
    assert cli(hub, "history", "1", "--wait").exit_code == 0  # already done: its results
    result = cli(hub, "history", "--wait")
    assert (result.exit_code, "--wait needs the ID of a command" in result.output) == (2, True)


def test_jobs(hub):
    # not in a terminal (eg. piped to grep): tab-separated values, never cut
    result = cli(hub, "jobs")
    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "worker\tcategory\tsource\tstep\tdescription\tduration\tmemory",
        "pid 24195\tuploader\tctd\tupdate_data\tCTD_diseases.tsv.gz\t5.26s\t378.7 MiB",
    ]
    assert "1 job(s) running, 2 pending" in result.stderr
    assert json.loads(cli(hub, "jobs", "--json").output) == JOB_MANAGER


def test_login(hub, hub_users, saved_logins):
    result = cli(hub, "run", "dump", "mygene")
    assert result.exit_code == 2  # not run
    assert "Login required (biothings-cli hub login)" in result.output
    assert cli(hub, "help", "dump").exit_code == 0  # reading stays open
    result = cli(hub, "login", "--username", "alice", "--password-stdin", input="bob-password\n")
    assert (result.exit_code, "Invalid username or password" in result.output) == (1, True)
    result = cli(hub, "login", "-n", "alice", "--password-stdin", input="alice-password\n")
    assert result.exit_code == 0, result.output
    assert "Logged in to %s as alice, until" % hub.url in result.output
    assert saved_logins.stat().st_mode & 0o777 == 0o600  # its token gives access to the hub
    assert json.loads(cli(hub, "run", "dump", "mygene").output) == {"dumped": "mygene", "force": False}
    assert cli(hub, "logout").output == "Logged out of %s\n" % hub.url
    assert cli(hub, "run", "dump", "mygene").exit_code == 2
    # a token for scripts, eg. in BIOTHINGS_HUB_TOKEN
    result = cli(hub, "login", "-n", "alice", "--password-stdin", "--print-token", input="alice-password\n")
    token = result.stdout.strip()
    assert cli(hub, "--token", token, "run", "dump", "mygene").exit_code == 0
    assert json.loads(saved_logins.read_text()) == {}  # not saved


def test_saved_logins_the_hub_refuses_are_forgotten(hub, hub_users, saved_logins):
    cli(hub, "login", "-n", "alice", "--password-stdin", input="alice-password\n")
    hub_users["alice"] = hash_password("new-password", iterations=1000)  # its tokens aren't valid anymore
    result = cli(hub, "run", "dump", "mygene")
    assert "Invalid or expired login" in result.output
    assert json.loads(saved_logins.read_text()) == {}


def test_expired_logins_are_ignored(saved_logins):
    hub_cli.save_login("http://hub:7080/", {"username": "alice", "token": "abc", "expires": "2020-01-01T00:00:00"})
    assert hub_cli.saved_login("http://hub:7080") is None
    assert "X-Biothings-Access-Token" not in HubClient("http://hub:7080").session.headers
    hub_cli.save_login("http://hub:7080", {"username": "alice", "token": "abc", "expires": "2999-01-01T00:00:00Z"})
    assert HubClient("http://hub:7080/").session.headers["X-Biothings-Access-Token"] == "abc"
    assert HubClient("http://hub:7080", token="given").session.headers["X-Biothings-Access-Token"] == "given"


def test_hubs_without_logins(hub):
    result = cli(hub, "login", "-n", "alice", "--password-stdin", input="alice-password\n")
    assert (result.exit_code, "Login isn't enabled on this hub" in result.output) == (1, True)
    url, stop = serve([("/", RootHandler, {"features": ["ws"]})])  # older BioThings versions
    try:
        result = CliRunner().invoke(
            hub_application, ["--url", url, "login", "-n", "alice", "--password-stdin"], input="x\n"
        )
        assert "This hub doesn't support logins" in result.output
    finally:
        stop()


def test_shell_login(hub, hub_users):
    lines = ["dump mygene", "login", "alice", "alice-password", "dump mygene", "logout", "exit"]
    result = cli(hub, "shell", input="\n".join(lines) + "\n")
    assert result.exit_code == 0, result.output
    assert "This hub requires a login to run commands: type login" in result.output
    assert "Login required" in result.output
    assert "Logged in as alice, until" in result.output
    assert '"dumped": "mygene"' in result.output
    assert "Logged out" in result.output


def test_waits_survive_network_drops(monkeypatch):
    """While waiting for a command, the hub can't be reached for a while (eg. the network drops): try again"""
    client = HubClient("http://hub:7080")
    answers = [HubAPIError("Can't reach the hub at http://hub:7080 (connection refused)")] * 3
    answers += [{"id": 7, "is_done": False}, {"id": 7, "is_done": True, "results": ["indexed"]}]
    states, delays = [], []

    def command(command_id, timeout=None):
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(client, "command", command)
    monkeypatch.setattr(hub_cli.time, "sleep", delays.append)
    info = client.wait(7, on_status=states.append, max_interval=2)
    assert info["results"] == ["indexed"]
    assert states[:3] == [None, None, None]  # (unreachable)
    assert delays == [1.0, 1.5, 2, 2]  # less and less often, up to max_interval
    # still unreachable after retry_for: stop waiting
    answers[:] = [HubAPIError("Can't reach the hub at http://hub:7080 (connection refused)")] * 2
    monkeypatch.setattr(hub_cli.time, "time", iter([0, 0, 0, 0, 1000, 1000]).__next__)
    with pytest.raises(HubAPIError, match="for 900s: stopped waiting, command #7 may still be running"):
        client.wait(7)
    # errors which won't go away by trying again (eg. the hub restarted, it doesn't know the command)
    answers[:] = [HubAPIError("No such command with ID 7", status=400)]
    with pytest.raises(HubAPIError, match="No such command"):
        client.wait(7)


def test_heartbeats_when_not_in_a_terminal(hub, monkeypatch):
    """Scripts (not a terminal) get a line now and then saying how it's going, with what the workers do"""
    monkeypatch.setattr(hub_cli, "HEARTBEAT_INTERVAL", 0.1)
    result = cli(hub, "run", "--interval", "0.01", "dump_sources", "--delay", "0.3")
    assert result.exit_code == 0, result.output
    heartbeats = re.findall(r"^\[[\d-]+ [\d:]+\] \[#1\] running for \d+s: (.*)$", result.output, re.M)
    assert heartbeats and heartbeats[0] == "uploader ctd update_data"  # (see JOB_MANAGER)


def test_version():
    result = CliRunner().invoke(build_admin_application(debug=False, rich_traceback=False), ["--version"])
    assert (result.exit_code, result.output) == (0, "biothings %s\n" % biothings.__version__)


def test_cli_without_its_packages(monkeypatch):
    """The CLI's packages are optional (biothings[cli]): without them, it says how to install them"""
    find_spec = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util, "find_spec", lambda name, *args: None if name == "typer" else find_spec(name, *args)
    )
    with pytest.raises(SystemExit) as exit_info:
        biothings.cli.main()
    assert exit_info.value.code == "The biothings-cli requires typer: pip install 'biothings[cli]'"
