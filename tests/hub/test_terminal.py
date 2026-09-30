"""
Tests for biothings.hub.terminal: command line parsing, execution, hook files and catalog,
and the HTTP handlers used by BioThings Studio's terminal and "biothings-cli hub"
"""

import asyncio
import datetime
import json
import logging
import textwrap
import threading
from functools import partial
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import tornado.testing
import tornado.web
from terminal_fakes import FakeShell, make_terminal

from biothings.hub import HubServer
from biothings.hub.api import EndpointDefinition, generate_api_routes
from biothings.hub.api.handlers.terminal import TerminalCommandsHandler, TerminalRunHandler
from biothings.hub.terminal import (
    CommandUsageError,
    ConfirmationRequired,
    UnknownCommand,
    convert,
    parse_literal,
    render,
    split_chain,
)
from biothings.utils.hub import (
    AlreadyRunningException,
    CommandDefinition,
    CommandError,
    CommandNotAllowed,
    CompositeCommand,
)

# -------------------------------------------------------------------------------------
# Parsing
# -------------------------------------------------------------------------------------


def test_split_chain_ignores_quoted_operators():
    assert split_chain("dump mygene && upload mygene") == ["dump mygene", "upload mygene"]
    assert split_chain("dump('a && b') && upload(\"c&&d\")") == ["dump('a && b')", 'upload("c&&d")']
    assert split_chain("dump_all") == ["dump_all"]
    assert split_chain("dump &&") == ["dump", ""]


def test_python_call_syntax():
    terminal, _, sources = make_terminal()
    (invocation,) = terminal.parse('dump("mygene", force=True)')
    assert invocation.name == "dump"
    assert invocation.command == sources.dump
    assert invocation.args == ["mygene"]
    assert invocation.kwargs == {"force": True}
    assert invocation.display == "dump('mygene', force=True)"
    # literal values of any kind, keywords sorted following the signature
    (invocation,) = terminal.parse("dump('x', release={'v': [1, 2]}, force=False, skip_manual=True)")
    assert invocation.kwargs == {"release": {"v": [1, 2]}, "force": False, "skip_manual": True}
    assert invocation.display == "dump('x', force=False, skip_manual=True, release={'v': [1, 2]})"


def test_shell_syntax_uses_signature_to_convert_values():
    terminal, _, _ = make_terminal()
    (invocation,) = terminal.parse("dump mygene --force --release 2024-01 --skip-manual=no")
    assert invocation.name == "dump"
    assert invocation.args == ["mygene"]
    assert invocation.kwargs == {"force": True, "skip_manual": False, "release": "2024-01"}
    assert invocation.display == "dump('mygene', force=True, skip_manual=False, release='2024-01')"
    # same call, whatever the syntax or the order of options
    assert terminal.parse("dump --release 2024-01 --no-skip-manual mygene --force")[0].display == invocation.display
    assert terminal.parse("dump('mygene', release='2024-01', skip_manual=False, force=True)")[0].display == (
        invocation.display
    )


def test_shell_syntax_values():
    def example(name, count=1, ratio=0.5, flag=False, steps=("a", "b"), options=None, *, label="x", **extra):
        return name

    terminal, _, _ = make_terminal(example=example)
    (invocation,) = terminal.parse(
        "example 007 --count 3 --ratio 2 flag=true --steps c,d --options '{\"k\": [1]}' "
        "--label 12 --number 12 --list '[1, 2]' --none none --text hello"
    )
    assert invocation.args == ["007"]  # untyped, but not a canonical number: kept as string
    assert invocation.kwargs == {
        "count": 3,
        "ratio": 2.0,
        "flag": True,
        "steps": ("c", "d"),
        "options": {"k": [1]},
        "label": "12",  # typed from its default value
        "number": 12,  # untyped: parsed as literal
        "list": [1, 2],
        "none": None,
        "text": "hello",
    }
    with pytest.raises(CommandUsageError, match="Invalid int value 'many'") as err:
        terminal.parse("example x --count many")
    assert err.value.usage.startswith("example <name> [--count <value>] [--ratio <value>] [--flag]")


def test_shell_syntax_positionals_skip_slots_given_as_options():
    def example(source, force=False, label="default"):
        return source, force, label

    terminal, _, _ = make_terminal(example=example)
    (invocation,) = terminal.parse("example taxonomy --force custom")
    assert invocation.args == ["taxonomy"]
    assert invocation.kwargs == {"force": True, "label": "custom"}
    assert invocation.command(*invocation.args, **invocation.kwargs) == ("taxonomy", True, "custom")


def test_shell_syntax_varargs_and_separator():
    def example(first, *values, upper=False):
        return first, values, upper

    terminal, _, _ = make_terminal(example=example)
    (invocation,) = terminal.parse("example a -1 2 --upper -- --not-an-option")
    assert invocation.args == ["a", -1, 2, "--not-an-option"]
    assert invocation.kwargs == {"upper": True}


def test_multi_word_and_hyphenated_command_names():
    terminal, _, _ = make_terminal()
    assert terminal.parse("dump all")[0].name == "dump_all"
    (invocation,) = terminal.parse("dump-all --force")
    assert (invocation.name, invocation.kwargs) == ("dump_all", {"force": True})
    (invocation,) = terminal.parse("upload all-sources")
    assert (invocation.name, invocation.args) == ("upload", ["all-sources"])


def test_argv_is_parsed_like_a_command_line():
    terminal, _, _ = make_terminal()
    # a list typed without quotes around it, the shell-like parsing removes the inner quotes
    (invocation,) = terminal.parse('dump mygene --sources ["a","b"]')
    assert invocation.kwargs == {"sources": ["a", "b"]}
    (invocation,) = terminal.parse(argv=["dump", "my source", "--force"])
    assert (invocation.args, invocation.kwargs) == (["my source"], {"force": True})
    (invocation,) = terminal.parse(argv=["dump('mygene', force=True)"])
    assert invocation.display == "dump('mygene', force=True)"
    chain = terminal.parse(argv=["dump", "mygene", "&&", "upload", "mygene"])
    assert [invocation.display for invocation in chain] == ["dump('mygene')", "upload('mygene')"]
    with pytest.raises(CommandError, match="list of strings"):
        terminal.parse(argv="dump mygene")


def test_parse_errors():
    terminal, _, _ = make_terminal()
    with pytest.raises(UnknownCommand) as err:
        terminal.parse("dump_al")
    assert err.value.suggestions == ["dump_all", "dump"]
    with pytest.raises(UnknownCommand, match="Unknown command 'uplaod', did you mean: upload?"):
        terminal.parse("uplaod mygene")
    with pytest.raises(CommandNotAllowed, match="it's not a command"):
        terminal.parse("index_config")
    with pytest.raises(CommandUsageError, match="missing a required argument: 'src'"):
        terminal.parse("dump")
    with pytest.raises(CommandUsageError, match="Too many arguments"):
        terminal.parse("upload a true c")
    with pytest.raises(CommandUsageError, match="Invalid boolean value 'b'"):
        terminal.parse("upload a b")
    with pytest.raises(CommandUsageError, match="Unknown option '--force'"):
        terminal.parse("upload mygene --force")
    with pytest.raises(CommandUsageError, match="Short options aren't supported"):
        terminal.parse("dump mygene -f")
    with pytest.raises(CommandUsageError, match="given more than once"):
        terminal.parse("dump mygene --force --force")
    with pytest.raises(CommandError, match="No closing quotation"):
        terminal.parse("dump 'mygene")
    with pytest.raises(CommandError, match="Invalid syntax"):
        terminal.parse('dump("mygene", force=True')
    with pytest.raises(CommandError, match="between two commands"):
        terminal.parse("dump mygene &&")
    with pytest.raises(CommandError, match="No command given"):
        terminal.parse("   ")


def test_python_syntax_only_allows_commands_and_literals():
    terminal, _, _ = make_terminal()
    with pytest.raises(CommandNotAllowed, match="'dm.dump_src' isn't one"):
        terminal.parse("dm.dump_src('mygene')")
    with pytest.raises(CommandNotAllowed, match="Only literal values"):
        terminal.parse("dump(open('/etc/passwd').read())")
    with pytest.raises(CommandNotAllowed, match="'\\*' arguments"):
        terminal.parse("dump(*['mygene'])")
    with pytest.raises(CommandNotAllowed, match="'\\*\\*' arguments"):
        terminal.parse("dump(**{'src': 'mygene'})")
    with pytest.raises(CommandError, match="Unknown name 'mygene'"):
        terminal.parse("dump(mygene)")
    # names are allowed when they refer to commands, or constants from the hub namespace
    (invocation,) = terminal.parse("help(dump)")
    assert invocation.args == [terminal.shell.commands["dump"]]
    assert invocation.display == "help(dump)"
    (invocation,) = terminal.parse("upload(pending)")
    assert (invocation.args, invocation.display) == (["pending"], "upload(pending)")


def test_composite_commands_are_expanded():
    terminal, _, _ = make_terminal(refresh=CompositeCommand("dump('mygene') && upload mygene"))
    (invocation,) = terminal.parse("refresh")
    assert invocation.display == "refresh()"
    assert [step.display for step in invocation.steps] == ["dump('mygene')", "upload('mygene')"]
    with pytest.raises(CommandUsageError, match="takes no argument"):
        terminal.parse("refresh --force")


def test_value_conversions():
    assert parse_literal("12") == 12
    assert parse_literal("-1.5e3") == -1500.0
    assert parse_literal("0012") == "0012"
    assert parse_literal("1.2.3") == "1.2.3"
    assert parse_literal("True") is True
    assert parse_literal("null") is None
    assert parse_literal('{"a": true}') == {"a": True}
    assert parse_literal("'42'") == "42"
    assert parse_literal("[oops") == "[oops"
    assert parse_literal("[fruits, veggies]") == ["fruits", "veggies"]  # quotes removed by the shell
    assert convert("yes", bool) is True
    assert convert("a, b", list) == ["a", "b"]
    assert convert("12", str) == "12"
    with pytest.raises(CommandUsageError):
        convert("maybe", bool)
    with pytest.raises(CommandUsageError):
        convert("[1]", dict)


# -------------------------------------------------------------------------------------
# Execution
# -------------------------------------------------------------------------------------


def test_run_sync_command():
    def hello(name, excited=False):
        """Say hello"""
        print("saying hello")
        return "Hello %s%s" % (name, "!" if excited else "")

    terminal, shell, _ = make_terminal(hello=hello)
    response = terminal.run("hello world --excited")
    assert response == {
        "cmd": "hello('world', excited=True)",
        "id": None,
        "is_done": True,
        "failed": False,
        "result": "Hello world!",
        "stdout": "saying hello\n",
        "stderr": "",
        "logs": [],
    }
    # tracked commands are kept in the command history, others aren't
    assert [cmd["cmd"] for cmd in shell.launched_commands.values()] == ["hello('world', excited=True)"]
    assert terminal.run("status")["result"] == {"source": {"total": 2}}
    assert len(shell.launched_commands) == 1


def test_run_failing_command():
    def boom(what):
        raise ValueError("bad %s" % what)

    terminal, shell, _ = make_terminal(boom=boom)
    response = terminal.run(argv=["boom", "input"])
    assert response["failed"] is True
    assert response["is_done"] is True
    assert response["error"] == "ValueError: bad input"
    assert "in boom" in response["traceback"]
    assert not shell.launched_commands


def test_run_returns_what_commands_log():
    logger = logging.getLogger("test_terminal.archive")
    logger.setLevel(logging.DEBUG)

    def archive_old(days=3):
        """Report through logging, like many hub commands and hooks"""
        logger.info("Archiving builds older than %s days", days)
        logger.warning("Build %s has no date", "covid19_2")
        logger.debug("details")
        # a job running in another thread meanwhile
        thread = threading.Thread(target=logger.info, args=("from another thread",))
        thread.start()
        thread.join()

    terminal, _, _ = make_terminal(archive_old=archive_old)
    handlers = list(logging.getLogger().handlers)
    response = terminal.run("archive_old --days 30")
    assert response["logs"] == ["Archiving builds older than 30 days", "WARNING: Build covid19_2 has no date"]
    assert logging.getLogger().handlers == handlers
    assert terminal.console("archive_old") == [
        "Archiving builds older than 3 days\nWARNING: Build covid19_2 has no date"
    ]


def test_arguments_preset_by_a_command_are_not_shown():
    calls = []

    def dump(src, force=False, check_only=False):
        """Dump a source"""
        calls.append((src, force, check_only))

    terminal, shell, _ = make_terminal()
    shell.add_command("check", partial(dump, check_only=True))
    check = terminal.describe("check")
    assert check["usage"] == "check <src> [--force]"
    assert check["signature"] == "check(src, force=False)"
    assert [param["name"] for param in check["params"]] == ["src", "force"]
    with pytest.raises(CommandUsageError) as error:
        terminal.parse("check")
    assert error.value.usage == "check <src> [--force]"
    terminal.run("check mygene")
    assert calls == [("mygene", False, True)]


def test_commands_deleting_data_must_be_confirmed():
    deleted = []

    def rmmerge(merge_name):
        deleted.append(merge_name)

    def auto_archive(build_config_name, days=3, dryrun=True):
        return "would archive" if dryrun else "archived"

    def purge(days=30):
        """Delete old data (a hook command asking for a confirmation)"""
        return "purged"

    purge.confirm = True
    terminal, _, _ = make_terminal(rmmerge=rmmerge, auto_archive=auto_archive, purge=purge)
    with pytest.raises(ConfirmationRequired) as error:
        terminal.run("rmmerge mygene_1")
    assert error.value.reasons == ["rmmerge('mygene_1'): Delete a build"]
    assert deleted == []
    terminal.run("rmmerge mygene_1", confirmed=True)
    assert deleted == ["mygene_1"]
    # only when it's not a dry run
    assert terminal.run("auto_archive mygene")["result"] == "would archive"
    with pytest.raises(ConfirmationRequired):
        terminal.run("auto_archive mygene --no-dryrun")
    with pytest.raises(ConfirmationRequired) as error:
        terminal.run("status && purge --days 3")
    assert error.value.reasons == ["purge(days=3): Delete old data (a hook command asking for a confirmation)"]
    confirm = {command["name"]: command["confirm"] for command in terminal.catalog()["commands"]}
    assert [confirm[name] for name in ("rmmerge", "auto_archive", "purge", "dump")] == [True, "dryrun", True, None]
    # the hub console runs any python code already
    terminal.console("rmmerge mygene_2")
    assert deleted == ["mygene_1", "mygene_2"]


class Thing:
    def __repr__(self):
        return "<Thing>"


def test_run_renders_non_json_results_as_text():
    terminal, _, _ = make_terminal(things=lambda: [Thing()], numbers=lambda: {1, 2})
    assert terminal.run("things")["result"] == "[<Thing>]"
    assert terminal.run("numbers")["result"] == {1, 2}  # serialized as a list
    assert render({"a": [1]}) == '{\n  "a": [\n    1\n  ]\n}'
    assert render(None) == ""


async def wait_done(shell, command_id, timeout=5.0):
    """Wait for a command running in background to be done (the hub refreshes their status every second)"""
    for _ in range(int(timeout / 0.01)):
        shell.refresh_commands()
        if shell.launched_commands[command_id].get("is_done"):
            return shell.command_info(id=command_id)
        await asyncio.sleep(0.01)
    raise AssertionError("Command #%s still running after %ss" % (command_id, timeout))


@pytest.mark.asyncio
async def test_run_async_commands_are_tracked():
    async def refresh(source, delay=0.01):
        """Refresh a source (coroutine)"""
        await asyncio.sleep(delay)
        return {"refreshed": source}

    def launch_tasks(count=2):
        async def job(i):
            return i

        return [asyncio.ensure_future(job(i)) for i in range(count)]

    def launch_future():
        future = asyncio.get_running_loop().create_future()
        future.set_result(Thing())  # not JSON-serializable
        return future

    terminal, shell, _ = make_terminal(refresh=refresh, launch_tasks=launch_tasks, launch_future=launch_future)
    response = terminal.run("refresh mygene")
    assert response["is_done"] is False
    assert response["cmd"] == "refresh('mygene')"
    assert response["id"] == 1
    assert shell.launched_commands[1]["is_done"] is False

    tasks = terminal.run("launch tasks --count 3")
    future = terminal.run("launch_future()")
    assert (tasks["id"], future["id"]) == (2, 3)

    info = await wait_done(shell, 1)
    assert (info["is_done"], info["failed"], info["results"]) == (True, False, [{"refreshed": "mygene"}])
    assert (await wait_done(shell, 2))["results"] == [0, 1, 2]
    assert (await wait_done(shell, 3))["results"] == ["<Thing>"]
    assert shell.saved[1]["is_done"] is True


@pytest.mark.asyncio
async def test_run_async_command_failure_and_already_running():
    async def slow(fail=False):
        await asyncio.sleep(0.02)
        if fail:
            raise RuntimeError("it failed")
        return "ok"

    terminal, shell, _ = make_terminal(slow=slow)
    first = terminal.run("slow --fail")
    with pytest.raises(AlreadyRunningException, match="already running"):
        terminal.run("slow(fail=True)")
    other = terminal.run("slow")  # a different call can run at the same time
    info = await wait_done(shell, first["id"])
    assert (info["is_done"], info["failed"], info["results"]) == (True, True, ["it failed"])
    assert (await wait_done(shell, other["id"]))["results"] == ["ok"]
    again = terminal.run("slow --fail")  # done, can run again
    assert (await wait_done(shell, again["id"]))["failed"] is True


@pytest.mark.asyncio
async def test_run_chain_runs_commands_one_after_the_other():
    events = []

    async def step(name, fail=False):
        events.append("start %s" % name)
        await asyncio.sleep(0.01)
        events.append("end %s" % name)
        if fail:
            raise ValueError("%s failed" % name)
        return name

    terminal, shell, _ = make_terminal(step=step)
    response = terminal.run("step one && status && step two")
    assert response["cmd"] == "step('one') && status() && step('two')"
    info = await wait_done(shell, response["id"])
    assert info["results"] == [["one", {"source": {"total": 2}}, "two"]]
    assert events == ["start one", "end one", "start two", "end two"]

    events.clear()
    response = terminal.run("step one --fail && step two")
    info = await wait_done(shell, response["id"])
    assert info["failed"] is True
    assert info["results"] == ["step('one', fail=True) failed: one failed"]
    assert events == ["start one", "end one"]


@pytest.mark.asyncio
async def test_run_composite_command():
    terminal, shell, sources = make_terminal(refresh=CompositeCommand("dump('mygene') && upload mygene"))
    response = terminal.run("refresh")
    assert response["cmd"] == "refresh()"
    assert (await wait_done(shell, response["id"]))["results"] == [
        [{"dumped": "mygene", "force": False}, "uploaded mygene"]
    ]
    assert [call[0] for call in sources.calls] == ["dump", "upload"]


# -------------------------------------------------------------------------------------
# Hooks
# -------------------------------------------------------------------------------------


def write_hook(folder, name, code):
    path = folder / name
    path.write_text(textwrap.dedent(code))
    return str(path)


def test_hook_functions_become_commands(tmp_path):
    terminal, shell, sources = make_terminal()
    path = write_hook(
        tmp_path,
        "my_hook.py",
        """
        from os.path import join
        import json

        LIMIT = 3

        def dump_and_upload(src, force=False):
            '''Dump then upload a source'''
            return [dump(src, force=force), upload(src)]

        async def slow_hello(name):
            return "hello " + name

        def _helper():
            return "private"

        class Thing:
            pass
        """,
    )
    info = terminal.load_hook(path)
    assert info["commands"] == ["dump_and_upload", "slow_hello"]
    assert (info["error"], info["overrides"]) == (None, [])
    # everything is available from the console namespace, only functions become commands
    assert {"join", "json", "LIMIT", "_helper", "Thing"} <= set(shell.extra_ns)
    assert "join" not in shell.commands and "_helper" not in shell.commands
    assert terminal.origins == {"dump_and_upload": "my_hook.py", "slow_hello": "my_hook.py"}
    response = terminal.run("dump and upload mygene --force")
    assert response["result"] == [{"dumped": "mygene", "force": True}, "uploaded mygene"]
    assert "slow_hello" in shell.help()
    catalog = terminal.catalog()
    described = {command["name"]: command for command in catalog["commands"]}
    assert described["dump_and_upload"]["origin"] == "hook"
    assert described["dump_and_upload"]["hook"] == "my_hook.py"
    assert described["slow_hello"]["is_async"] is True
    assert described["dump"]["origin"] == "builtin"
    assert catalog["hooks"][0]["commands"] == ["dump_and_upload", "slow_hello"]


def test_hook_all_lists_commands_explicitly(tmp_path):
    terminal, shell, _ = make_terminal()
    first = write_hook(
        tmp_path,
        "a.py",
        """
        from functools import partial
        __all__ = ["dump_mygene", "report"]

        def report():
            return "report"

        def not_exported():
            return None

        dump_mygene = partial(dump, "mygene")
        """,
    )
    second = write_hook(tmp_path, "b.py", "def other():\n    return 1\n")
    assert terminal.load_hook(first)["commands"] == ["dump_mygene", "report"]
    # __all__ from a previous hook doesn't leak into the next one
    assert terminal.load_hook(second)["commands"] == ["other"]
    assert "__all__" not in shell.extra_ns
    assert terminal.run("dump_mygene")["result"] == {"dumped": "mygene", "force": False}


def test_hook_overriding_a_command_is_reported(tmp_path):
    terminal, shell, _ = make_terminal()
    path = write_hook(tmp_path, "override.py", "def upload(src):\n    return 'custom upload'\n")
    assert terminal.load_hook(path)["overrides"] == ["upload"]
    assert "override.py: upload (replaces: upload)" in terminal.hooks_summary()
    assert terminal.run("upload mygene")["result"] == "custom upload"
    assert shell.commands["upload"] is shell.extra_ns["upload"]


def test_broken_hook_is_reported(tmp_path):
    terminal, _, _ = make_terminal()
    path = write_hook(tmp_path, "broken.py", "def ok():\n    return 1\n\nundefined_name()\n")
    with pytest.raises(NameError):
        terminal.load_hook(path)
    info = terminal.hook_info()[0]
    assert info["commands"] == []
    assert "NameError: name 'undefined_name' is not defined" in info["error"]
    assert 'broken.py", line 4' in info["error"]
    assert "broken.py: failed to load, NameError: name 'undefined_name' is not defined" in terminal.hooks_summary()
    assert "ok" not in terminal.runnable_commands()


def test_outbreak_auto_archive_hook(tmp_path):
    """The kind of hook which started it all: https://github.com/outbreak-info/outbreak.api (hooks/auto_archive.py)"""
    archived, scheduled, exposed = [], [], []
    builds = {"covid19_1": "2020-01-01T00:00:00+00:00", "covid19_2": "2999-01-01T00:00:00+00:00"}
    terminal, shell, _ = make_terminal(
        lsmerge=lambda build_config_name: list(builds),
        archive=archived.append,
    )
    shell.extra_ns["bm"] = SimpleNamespace(build_info=lambda bid: {"_meta": {"build_date": builds[bid]}})
    shell.extra_ns["schedule"] = lambda *args, **kwargs: scheduled.append(args)
    shell.extra_ns["expose"] = lambda **kwargs: exposed.append(kwargs)
    path = write_hook(
        tmp_path,
        "auto_archive.py",
        '''
        import datetime
        from dateutil import parser as dtparser
        from biothings import config
        logger = config.logger

        def auto_archive(build_config_name, days=3, dryrun=True):
            """
            Archive any builds which build date is older than today's date
            by "days" day.
            """
            builds = lsmerge(build_config_name)
            today = datetime.datetime.now().astimezone()
            for bid in builds:
                build = bm.build_info(bid)
                bdate = dtparser.parse(build["_meta"]["build_date"]).astimezone()
                if (today - bdate).days > days and not dryrun:
                    archive(bid)

        schedule("0 17 * * *", auto_archive, "covid19", dryrun=False)
        expose(endpoint_name="auto_archive", command_name="auto_archive", method="put")
        ''',
    )
    assert terminal.load_hook(path)["commands"] == ["auto_archive"]
    assert (len(scheduled), exposed[0]["command_name"]) == (1, "auto_archive")
    description = terminal.describe("auto-archive")
    assert description["summary"] == "Archive any builds which build date is older than today's date"
    assert description["usage"] == "auto_archive <build_config_name> [--days <value>] [--no-dryrun]"
    assert terminal.run("auto_archive covid19 --days 30")["failed"] is False
    assert archived == []  # dry run by default
    with pytest.raises(ConfirmationRequired):  # a hook replacing a built-in command inherits its confirmation
        terminal.run("auto archive covid19 --days 30 --no-dryrun")
    assert archived == []
    response = terminal.run("auto archive covid19 --days 30 --no-dryrun", confirmed=True)
    assert response["cmd"] == "auto_archive('covid19', days=30, dryrun=False)"
    assert archived == ["covid19_1"]


# -------------------------------------------------------------------------------------
# Catalog, HubShell and HubServer integration
# -------------------------------------------------------------------------------------


def test_catalog_describes_runnable_commands():
    terminal, _, _ = make_terminal()
    catalog = terminal.catalog()
    described = {command["name"]: command for command in catalog["commands"]}
    assert "index_config" not in described
    assert {"dump", "dump_all", "upload", "status", "help", "commands", "command", "sch"} - set(described) == set()
    assert "pending" not in described  # a constant, not a command
    dump = described["dump"]
    assert dump["summary"] == "Download the data of a source"  # built-in commands help
    assert dump["examples"] == ["dump mygene", "dump mygene --force"]
    assert dump["doc"] == "Dump a source"
    assert dump["signature"] == "dump(src, force=False, skip_manual=False, **kwargs)"
    assert dump["usage"] == "dump <src> [--force] [--skip-manual] [--<option> <value>...]"
    assert dump["params"][0] == {"name": "src", "kind": "positional", "required": True, "default": None, "type": None}
    assert dump["params"][1] == {
        "name": "force",
        "kind": "positional",
        "required": False,
        "default": "False",
        "type": "bool",
    }
    assert (dump["hidden"], described["sch"]["hidden"]) == (False, True)
    assert catalog["hooks_folder"] == "./hooks"
    json.dumps(catalog)  # served as JSON


def test_hubshell_add_command_and_help_by_name():
    terminal, shell, sources = make_terminal()
    previous = shell.add_command("dump", CommandDefinition(command=sources.dump_all, tracked=False))
    assert previous == sources.dump
    assert shell.commands["dump"] == sources.dump_all
    assert shell.tracked["dump"] is False
    assert shell.extra_ns["dump"] == sources.dump_all
    assert "Run all dumpers" in shell.help("dump")
    assert "Run all dumpers" in shell.help("dump-all")
    assert "\x08" not in shell.help("dump")  # plain text, no terminal bold


def test_hubshell_console_accepts_terminal_syntax():
    terminal, shell, sources = make_terminal()
    shell.server = SimpleNamespace(terminal=terminal)
    assert shell.eval("dump mygene --force") == ['{\n  "dumped": "mygene",\n  "force": true\n}']
    assert sources.calls[-1] == ("dump", "mygene", True, {})
    with pytest.raises(CommandError, match="Too many arguments"):
        shell.eval("upload a true c")
    # python code isn't affected (it would go through IPython)
    assert terminal.is_terminal_syntax("dump('mygene')") is False
    assert terminal.is_terminal_syntax("x = dump_all") is False
    assert terminal.is_terminal_syntax("for x in") is False  # invalid python, not a command
    with pytest.raises(CommandError, match="Unknown command 'uplod', did you mean: upload"):
        shell.eval("uplod mygene")
    assert terminal.is_terminal_syntax("dump all && upload mygene") is True


def test_hubserver_hook_files_are_loaded_by_terminal(tmp_path):
    terminal, shell, _ = make_terminal()
    server = HubServer(source_list=[], name="Test Hub")
    server.shell, server.terminal = shell, terminal
    server.hook_files = [
        write_hook(tmp_path, "a_broken.py", "raise RuntimeError('nope')\n"),
        write_hook(tmp_path, "b_ok.py", "def hello():\n    return 'hi'\n"),
    ]
    server.ingest_hooks()  # a broken hook doesn't prevent other hooks from loading
    assert [info["error"] is None for info in terminal.hook_info()] == [False, True]
    assert terminal.run("hello")["result"] == "hi"


@pytest.mark.parametrize("version_urls, hidden", [([], True), ([{"name": "mygene.info", "url": "..."}], False)])
def test_hubserver_hides_data_release_commands_without_releases(version_urls, hidden):
    server = HubServer(source_list=[], name="Test Hub")
    server.features = ["autohub"]
    server.managers = {"dump_manager": MagicMock(), "upload_manager": MagicMock()}
    server.autohub_feature = SimpleNamespace(version_urls=version_urls, list_biothings=list, install=print)
    server.configure_commands()
    shell = FakeShell(server.commands)
    release_commands = ["list", "versions", "info", "download", "apply", "install", "backend", "reset_backend"]
    assert [shell.hidden[name] for name in release_commands] == [hidden] * len(release_commands)
    assert shell.hidden["check"] is False  # works for any source
    assert shell.hidden["dump"] is False


# -------------------------------------------------------------------------------------
# HTTP handlers
# -------------------------------------------------------------------------------------


class TerminalHandlersTest(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        async def slow(name):
            await asyncio.sleep(0.01)
            return "done " + name

        def last_update():
            return {"started_at": datetime.datetime(2026, 9, 1, 18, 47, 24)}  # naive, as read from MongoDB

        self.terminal, self.shell, _ = make_terminal(
            slow=slow, last_update=last_update, rmmerge=lambda merge_name: "deleted %s" % merge_name
        )
        self.inputs = []
        shellog = SimpleNamespace(input=self.inputs.append)
        routes = [
            ("/terminal/commands", TerminalCommandsHandler, {"terminal": self.terminal}),
            ("/terminal/run", TerminalRunHandler, {"terminal": self.terminal, "shellog": shellog}),
        ]
        routes += generate_api_routes(self.shell, {"command": EndpointDefinition(name="command", method="get")})
        return tornado.web.Application(routes)

    def run_command(self, payload):
        response = self.fetch("/terminal/run", method="POST", body=json.dumps(payload), raise_error=False)
        return response.code, json.loads(response.body)

    def test_commands_deleting_data_must_be_confirmed(self):
        code, body = self.run_command({"cmd": "rmmerge mygene_1"})
        assert (code, body["confirm"]) == (428, ["rmmerge('mygene_1'): Delete a build"])
        code, body = self.run_command({"cmd": "rmmerge mygene_1", "confirmed": True})
        assert (code, body["result"]["result"]) == (200, "deleted mygene_1")

    def test_commands_catalog(self):
        response = self.fetch("/terminal/commands")
        assert response.code == 200
        result = json.loads(response.body)["result"]
        assert "dump_all" in [command["name"] for command in result["commands"]]

    def test_run_command_line_and_argv(self):
        code, body = self.run_command({"cmd": "dump mygene --force"})
        assert (code, body["status"]) == (200, "ok")
        assert body["result"]["result"] == {"dumped": "mygene", "force": True}
        code, body = self.run_command({"argv": ["upload", "mygene"]})
        assert body["result"]["result"] == "uploaded mygene"
        assert self.inputs == ["dump mygene --force", "upload mygene"]

    def test_run_errors(self):
        code, body = self.run_command({"cmd": "dump_al"})
        assert code == 404
        assert body["error"] == "Unknown command 'dump_al', did you mean: dump_all, dump?"
        assert body["suggestions"] == ["dump_all", "dump"]
        code, body = self.run_command({"cmd": "dump"})
        assert code == 400
        assert body["usage"] == "dump <src> [--force] [--skip-manual] [--<option> <value>...]"
        code, body = self.run_command({"cmd": "dm.dump_src('x')"})
        assert code == 403
        code, body = self.run_command({"cmd": "dump", "argv": ["dump"]})
        assert code == 400
        response = self.fetch("/terminal/run", method="POST", body="{oops", raise_error=False)
        assert response.code == 400

    def test_naive_dates_are_sent_as_utc(self):
        code, body = self.run_command({"cmd": "last_update"})
        assert body["result"]["result"] == {"started_at": "2026-09-01T18:47:24Z"}

    def test_async_command_can_be_followed(self):
        code, body = self.run_command({"cmd": "slow mygene"})
        started = body["result"]
        assert (started["is_done"], started["cmd"]) == (False, "slow('mygene')")
        code, body = self.run_command({"cmd": "slow mygene"})
        assert code == 409
        self.io_loop.run_sync(lambda: wait_done(self.shell, started["id"]))
        response = self.fetch("/command/%s" % started["id"])
        info = json.loads(response.body)["result"]
        assert (info["is_done"], info["failed"], info["results"]) == (True, False, ["done mygene"])
