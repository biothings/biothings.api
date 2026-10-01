"""
Module for creating the cli interface to a running BioThings Hub: ``biothings-cli hub``

Runs the same commands as BioThings Studio's terminal through the Hub API: the built-in
commands every hub provides (dump_all, upload_all, sync, ...) and the commands defined
in the hub's hook files (python files in the hub's HOOKS_FOLDER)

> biothings-cli hub commands                     list available commands
> biothings-cli hub help dump                    usage and documentation of a command
> biothings-cli hub run dump all                 run a command, wait for it to finish
> biothings-cli hub run --no-wait upload mygene  run a command in background
> biothings-cli hub status [ID]                  launched commands and their results
> biothings-cli hub hooks                        hook files loaded by the hub
> biothings-cli hub shell                        interactive terminal
"""

import json
import os
import sys
import time
from typing import List, Optional

import requests
import typer
from rich import box
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from typing_extensions import Annotated

DEFAULT_HUB_URL = "http://localhost:7080"
TOKEN_HEADER = "X-Biothings-Access-Token"
HISTORY_FILE = os.path.join(os.path.expanduser("~"), ".biothings_hub_history")

SHORT_HELP = "[green]Run commands on a running BioThings Hub, like the terminal in BioThings Studio.[/green]"
FULL_HELP = (
    SHORT_HELP
    + "\n\n[magenta]   :sparkles: Built-in commands (dump, dump_all, upload_all, sync, ...) and commands defined "
    + "in the hub's hook files.[/magenta]"
    + "\n[magenta]   :sparkles: Same syntax as the terminal: [bold]dump all[/bold], [bold]dump mygene --force[/bold] "
    + "or [bold]dump('mygene', force=True)[/bold][/magenta]"
    + "\n[green]   :point_right: Set [bold]BIOTHINGS_HUB_URL[/bold] (default: http://localhost:7080) "
    + "and [bold]BIOTHINGS_HUB_TOKEN[/bold] ENV variables to avoid passing --url and --token.[/green]"
)

hub_application = typer.Typer(
    help=FULL_HELP,
    short_help=SHORT_HELP,
    no_args_is_help=True,
    rich_markup_mode="rich",
)

console = Console(highlight=False)
err_console = Console(stderr=True, highlight=False)


class HubAPIError(Exception):
    """Error returned by the Hub API, or raised when it can't be reached"""

    def __init__(self, message, status=None, payload=None):
        super().__init__(message)
        self.status = status
        self.payload = payload or {}


class HubClient:
    """Minimal client for the Hub API endpoints used by the terminal"""

    def __init__(self, url, token=None, timeout=30.0):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        if token:
            self.session.headers[TOKEN_HEADER] = token

    def request(self, method, endpoint, **kwargs):
        url = self.url + endpoint
        try:
            # custom token header must not be sent to another host through redirects
            response = self.session.request(method, url, timeout=self.timeout, allow_redirects=False, **kwargs)
        except requests.RequestException as e:
            raise HubAPIError("Can't reach the hub at %s (%s)" % (self.url, e))
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if response.status_code == 404 and endpoint.startswith("/terminal"):
            raise HubAPIError(
                payload.get("error")
                or "This hub doesn't provide the terminal API: it runs an older BioThings version, or without "
                "the 'terminal' feature. Its commands can still be followed with 'biothings-cli hub status'",
                status=404,
                payload=payload,
            )
        if response.status_code >= 400 or payload.get("status") == "error":
            message = payload.get("error") or "%s %s: HTTP %s %s" % (method, url, response.status_code, response.reason)
            raise HubAPIError(message, status=response.status_code, payload=payload)
        return payload.get("result")

    def info(self):
        return self.request("GET", "/")

    def catalog(self):
        return self.request("GET", "/terminal/commands")

    def run(self, line=None, argv=None, confirmed=False):
        body = {"cmd": line} if line is not None else {"argv": list(argv)}
        if confirmed:
            # commands deleting or changing data must be confirmed (otherwise: 428 error)
            body["confirmed"] = True
        return self.request("POST", "/terminal/run", json=body)

    def command(self, command_id):
        return self.request("GET", "/command/%s" % command_id)

    def commands(self, running=False):
        return self.request("GET", "/commands", params={"running": 1} if running else None)

    def wait(self, command_id, timeout=None, interval=1.0):
        """Poll a command running in background until it's done, returning its final state"""
        started = time.time()
        while True:
            info = self.command(command_id)
            if info.get("is_done"):
                return info
            if timeout is not None and time.time() - started > timeout:
                raise TimeoutError("Command #%s still running after %ss" % (command_id, timeout))
            time.sleep(interval)


def is_empty(value):
    """True for results without anything to display: None, or lists of None (eg. dump_all(), one None per job)"""
    if value is None:
        return True
    return isinstance(value, list) and len(value) > 0 and all(is_empty(item) for item in value)


def render(value):
    """Text representation of a command result"""
    if is_empty(value):
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=2, default=str)


def print_text(text, style=None, stderr=False):
    """Print text as is (no rich markup nor emoji codes), eg. a command result"""
    if text:
        (err_console if stderr else console).print(text, style=style, soft_wrap=True, markup=False, emoji=False)


def print_json(value):
    """Machine-readable output, never wrapped nor colored"""
    typer.echo(json.dumps(value, indent=2, default=str))


def print_error(error):
    print_text("Error: %s" % error, style="red", stderr=True)
    payload = getattr(error, "payload", {}) or {}
    if payload.get("usage"):
        print_text("Usage: %s" % payload["usage"], stderr=True)


def print_logs(response):
    """Print what a command logged while it was called, on stderr to keep stdout for its outputs"""
    for line in response.get("logs") or []:  # not sent by older hubs
        print_text(line, style="dim", stderr=True)


def print_response(response, verbose=False):
    """Print the outcome of a command run synchronously (done), returns False if it failed"""
    print_logs(response)
    print_text(response.get("stdout"))
    print_text(response.get("stderr"), stderr=True)
    if response.get("failed"):
        print_text(response.get("error"), style="red", stderr=True)
        if verbose:
            print_text(response.get("traceback"), style="dim", stderr=True)
        return False
    print_text(render(response.get("result")))
    return True


def show_confirmation(error):
    """What a command would delete or change, as reported by the hub when it must be confirmed (428 error)"""
    for reason in error.payload.get("confirm") or [str(error)]:
        print_text(reason, style="yellow", stderr=True)


def print_command_results(info, verbose=False):
    """Print the final state of a command which ran in background, returns False if it failed"""
    results = info.get("results") or []
    failed = bool(info.get("failed"))
    label = "failed" if failed else "done"
    print_text(
        "[#%s] %s %s%s"
        % (info.get("id"), info.get("cmd"), label, " in %s" % info["duration"] if info.get("duration") else ""),
        style="red" if failed else "green",
        stderr=failed,
    )
    for result in results:
        print_text(render(result), style="red" if failed else None, stderr=failed)
    if failed and verbose and info.get("traceback"):  # not sent by older hubs
        print_text(info["traceback"].rstrip(), style="dim", stderr=True)
    return not failed


def get_client(ctx: typer.Context) -> HubClient:
    return ctx.obj


@hub_application.callback()
def hub_options(
    ctx: typer.Context,
    url: Annotated[
        str,
        typer.Option("--url", "-u", envvar="BIOTHINGS_HUB_URL", help="URL of the Hub API"),
    ] = DEFAULT_HUB_URL,
    token: Annotated[
        Optional[str],
        typer.Option(
            "--token",
            envvar="BIOTHINGS_HUB_TOKEN",
            help="Access token, sent in the X-Biothings-Access-Token header (hub behind an authentication proxy)",
        ),
    ] = None,
    timeout: Annotated[float, typer.Option("--timeout", help="Timeout for each request to the hub, in seconds")] = 30.0,
):
    """
    Run commands on a running BioThings Hub
    """
    ctx.obj = HubClient(url, token=token, timeout=timeout)


@hub_application.command(name="commands")
def list_commands(
    ctx: typer.Context,
    search: Annotated[Optional[str], typer.Argument(help="Only list commands containing this text")] = None,
    show_all: Annotated[bool, typer.Option("--all", "-a", help="Include advanced (hidden) commands")] = False,
    as_json: Annotated[bool, typer.Option("--json", help="Print the commands catalog as JSON")] = False,
):
    """
    List the commands available on the hub: built-in commands and the ones defined in hook files
    """
    try:
        catalog = get_client(ctx).catalog()
    except HubAPIError as e:
        print_error(e)
        raise typer.Exit(1)
    if as_json:
        print_json(catalog)
        return
    commands = [cmd for cmd in catalog["commands"] if show_all or not cmd["hidden"]]
    if search:
        commands = [cmd for cmd in commands if search.lower() in (cmd["name"] + cmd["summary"]).lower()]
    for title, origin in (("Built-in commands", "builtin"), ("Hook commands", "hook")):
        rows = [cmd for cmd in commands if cmd["origin"] == origin]
        if not rows:
            continue
        table = Table(title=title, title_justify="left", box=box.SIMPLE, show_header=False)
        table.add_column("command", style="bold cyan", no_wrap=True)
        table.add_column("description")
        for cmd in sorted(rows, key=lambda cmd: cmd["name"]):
            description = cmd["summary"]
            if origin == "hook":
                description = "%s [dim](%s)[/dim]" % (escape(description), escape(cmd["hook"]))
            else:
                description = escape(description)
            table.add_row(cmd["name"], description)
        console.print(table)
    if not commands:
        console.print("No command found")
    else:
        console.print("Use [bold]biothings-cli hub help <command>[/bold] for details about a command.")


@hub_application.command(name="help")
def command_help(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Command name, eg. dump or dump_all")],
):
    """
    Show the usage and documentation of a hub command
    """
    try:
        catalog = get_client(ctx).catalog()
    except HubAPIError as e:
        print_error(e)
        raise typer.Exit(1)
    wanted = name.replace("-", "_").replace(" ", "_")
    commands = {cmd["name"]: cmd for cmd in catalog["commands"]}
    cmd = commands.get(wanted)
    if cmd is None:
        suggestions = [other for other in commands if wanted in other]
        print_text("Unknown command '%s'" % name, style="red", stderr=True)
        if suggestions:
            print_text("Similar commands: %s" % ", ".join(sorted(suggestions)[:10]), stderr=True)
        raise typer.Exit(2)
    print_command_help(cmd)


def print_command_help(cmd):
    origin = "hook file %s" % cmd["hook"] if cmd["origin"] == "hook" else "built-in command"
    console.print("[bold cyan]%s[/bold cyan] [dim](%s)[/dim]" % (escape(cmd["name"]), escape(origin)))
    doc = cmd.get("doc") or ""
    if cmd.get("summary") and not doc.startswith(cmd["summary"]):
        print_text(cmd["summary"])
    console.print("[bold]Usage:[/bold] %s" % escape(cmd["usage"]), soft_wrap=True)
    console.print("[bold]Python:[/bold] %s" % escape(cmd["signature"]), soft_wrap=True)
    if cmd.get("is_async"):
        console.print("Runs in background (asynchronous command)")
    if cmd.get("confirm"):  # not sent by older hubs
        unless = "" if cmd["confirm"] is True else ", unless it's a dry run"
        console.print("Asks for a confirmation before running%s (see run --yes)" % unless)
    if cmd.get("examples"):
        console.print("[bold]Examples:[/bold]")
        for example in cmd["examples"]:
            print_text("  %s" % example)
    if doc:
        console.print()
        print_text(doc)


@hub_application.command(
    name="run",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True, "allow_interspersed_args": False},
)
def run_command(
    ctx: typer.Context,
    argv: Annotated[
        List[str],
        typer.Argument(
            help="Command and its arguments, eg. [bold]dump all[/bold], [bold]dump mygene --force[/bold], "
            "or a whole command line in quotes: [bold]\"dump('mygene', force=True)\"[/bold]",
            show_default=False,
        ),
    ],
    wait: Annotated[
        bool,
        typer.Option(
            "--wait/--no-wait",
            help="Wait for commands running in background to finish (sync), or return right away (async)",
        ),
    ] = True,
    wait_timeout: Annotated[
        Optional[float],
        typer.Option("--wait-timeout", help="Maximum time to wait for the command to finish, in seconds"),
    ] = None,
    interval: Annotated[float, typer.Option("--interval", help="Seconds between status checks while waiting")] = 1.0,
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw JSON response")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Print the traceback when a command fails")] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Don't ask for a confirmation (commands deleting or changing data)"),
    ] = False,
):
    """
    Run a command on the hub. Options for [bold]run[/bold] itself go before the command name.
    Commands deleting or changing data (eg. rmmerge) ask for a confirmation, unless [bold]--yes[/bold] is given.

    Examples:

      biothings-cli hub run dump all

      biothings-cli hub run --no-wait upload mygene

      biothings-cli hub run --yes auto_archive covid19 --days 3 --no-dryrun
    """
    client = get_client(ctx)
    try:
        try:
            response = client.run(argv=argv, confirmed=yes)
        except HubAPIError as e:
            if e.status != 428:
                raise
            show_confirmation(e)
            if not sys.stdin.isatty():
                print_text("Not run: this command must be confirmed, use --yes", style="red", stderr=True)
                raise typer.Exit(2)
            if not typer.confirm("Run it?", default=False, err=True):
                raise typer.Exit(1)
            response = client.run(argv=argv, confirmed=True)
    except HubAPIError as e:
        print_error(e)
        raise typer.Exit(2 if e.status in (400, 403, 404, 409) else 1)

    if response["is_done"]:
        if as_json:
            print_json(response)
            ok = not response["failed"]
        else:
            ok = print_response(response, verbose=verbose)
        raise typer.Exit(0 if ok else 1)

    if not wait:
        if as_json:
            print_json(response)
        else:
            print_logs(response)
            print_text("[#%s] %s started" % (response["id"], response["cmd"]))
            print_text("Follow it with: biothings-cli hub status %s" % response["id"], style="dim")
        return

    if not as_json:
        print_logs(response)
    try:
        if as_json:
            info = client.wait(response["id"], timeout=wait_timeout, interval=interval)
        else:
            with console.status("[#%s] %s running..." % (response["id"], escape(response["cmd"]))):
                info = client.wait(response["id"], timeout=wait_timeout, interval=interval)
    except KeyboardInterrupt:
        print_text("Stopped waiting, command #%s keeps running on the hub" % response["id"], stderr=True)
        raise typer.Exit(130)
    except (TimeoutError, HubAPIError) as e:
        print_error(e)
        raise typer.Exit(1)
    if as_json:
        print_json(info)
        ok = not info.get("failed")
    else:
        ok = print_command_results(info, verbose=verbose)
    raise typer.Exit(0 if ok else 1)


@hub_application.command(name="status")
def command_status(
    ctx: typer.Context,
    command_id: Annotated[Optional[int], typer.Argument(help="ID of a command, as returned by 'run'")] = None,
    running: Annotated[bool, typer.Option("--running", help="Only list commands still running")] = False,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Number of commands to list")] = 20,
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw JSON response")] = False,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Print the traceback when the command failed")
    ] = False,
):
    """
    Show the commands launched on the hub, or the status and results of one of them
    """
    client = get_client(ctx)
    try:
        result = client.command(command_id) if command_id is not None else client.commands(running=running)
    except HubAPIError as e:
        print_error(e)
        raise typer.Exit(1)
    if as_json:
        print_json(result)
        return
    if command_id is not None:
        if not result.get("is_done"):
            print_text("[#%s] %s running" % (result["id"], result["cmd"]))
            return
        if not print_command_results(result, verbose=verbose):
            raise typer.Exit(1)
        return
    commands = sorted(result.values(), key=lambda cmd: cmd.get("id", 0), reverse=True)[:limit]
    if not commands:
        console.print("No command %s" % ("running" if running else "launched yet"))
        return
    table = Table(box=box.SIMPLE)
    table.add_column("id", justify="right")
    table.add_column("status")
    table.add_column("duration")
    table.add_column("command")
    for cmd in commands:
        if not cmd.get("is_done"):
            status = "[yellow]running[/yellow]"
        elif cmd.get("failed"):
            status = "[red]failed[/red]"
        else:
            status = "[green]done[/green]"
        table.add_row(str(cmd.get("id")), status, cmd.get("duration") or "", escape(cmd.get("cmd", "")))
    console.print(table)


@hub_application.command(name="hooks")
def list_hooks(ctx: typer.Context):
    """
    List the hook files loaded by the hub (python files in its HOOKS_FOLDER), and the commands they define
    """
    try:
        catalog = get_client(ctx).catalog()
    except HubAPIError as e:
        print_error(e)
        raise typer.Exit(1)
    console.print("Hooks folder: %s" % escape(str(catalog.get("hooks_folder"))))
    if not catalog["hooks"]:
        console.print("No hook file loaded")
    failed = False
    for hook in catalog["hooks"]:
        if hook["error"]:
            failed = True
            console.print("[red]:heavy_multiplication_x: %s failed to load[/red]" % escape(hook["name"]))
            print_text(hook["error"].rstrip(), style="dim")
        else:
            commands = ", ".join(hook["commands"]) or "no command defined"
            console.print("[green]:heavy_check_mark:[/green] %s: %s" % (escape(hook["name"]), escape(commands)))
        if hook.get("overrides"):
            console.print("   [yellow]replaces: %s[/yellow]" % escape(", ".join(hook["overrides"])))
    if failed:
        raise typer.Exit(1)


@hub_application.command(name="shell")
def interactive_shell(ctx: typer.Context):
    """
    Interactive terminal: type commands as in BioThings Studio's terminal (help, exit...)
    """
    client = get_client(ctx)
    try:
        info = client.info() or {}
        catalog = client.catalog()
    except HubAPIError as e:
        print_error(e)
        raise typer.Exit(1)
    commands = {cmd["name"]: cmd for cmd in catalog["commands"]}
    console.print("Connected to [bold]%s[/bold] (%s)" % (escape(str(info.get("name") or "hub")), escape(client.url)))
    console.print(
        "Type [bold]help[/bold] to list commands, [bold]help <command>[/bold] for details, "
        "[bold]jobs[/bold] for commands running in background, [bold]exit[/bold] to quit."
    )
    readline = setup_history()
    running = {}
    try:
        while True:
            report_finished(client, running)
            try:
                line = input("hub> ").strip()
            except EOFError:
                break
            except KeyboardInterrupt:
                console.print()
                continue
            if not line:
                continue
            words = line.split()
            if line in ("exit", "quit"):
                break
            if words[0] == "help" and len(words) <= 2:
                if len(words) == 1:
                    shell_help(commands)
                elif words[1].replace("-", "_") in commands:
                    print_command_help(commands[words[1].replace("-", "_")])
                else:
                    print_text("Unknown command '%s'" % words[1], style="red", stderr=True)
                continue
            if line == "jobs":
                for command_id, cmd in sorted(running.items()):
                    print_text("[#%s] %s running" % (command_id, cmd))
                if not running:
                    print_text("No command running in background")
                continue
            try:
                try:
                    response = client.run(line=line)
                except HubAPIError as e:
                    if e.status != 428:
                        raise
                    show_confirmation(e)
                    if not typer.confirm("Run it?", default=False, err=True):
                        continue
                    response = client.run(line=line, confirmed=True)
            except HubAPIError as e:
                print_error(e)
                continue
            if response["is_done"]:
                print_response(response)
            else:
                print_logs(response)
                running[response["id"]] = response["cmd"]
                print_text("[#%s] %s running in background" % (response["id"], response["cmd"]), style="dim")
    finally:
        if readline is not None:
            try:
                readline.write_history_file(HISTORY_FILE)
            except OSError:
                pass


def shell_help(commands):
    visible = sorted(name for name, cmd in commands.items() if not cmd["hidden"])
    console.print("Commands: %s" % escape(", ".join(visible)), soft_wrap=True)
    hooks = sorted(name for name, cmd in commands.items() if cmd["origin"] == "hook")
    if hooks:
        console.print("From hook files: %s" % escape(", ".join(hooks)), soft_wrap=True)
    console.print("Terminal commands: help [<command>], jobs, exit")


def report_finished(client, running):
    """Print the results of the commands running in background which are now done"""
    for command_id in list(running):
        try:
            info = client.command(command_id)
        except HubAPIError:
            continue
        if info.get("is_done"):
            running.pop(command_id)
            print_command_results(info)


def setup_history():
    """Enable line editing and history for the interactive terminal, when readline is available"""
    if not sys.stdin.isatty():
        return None
    try:
        import readline
    except ImportError:
        return None
    try:
        readline.read_history_file(HISTORY_FILE)
    except OSError:
        pass
    readline.set_history_length(1000)
    return readline
