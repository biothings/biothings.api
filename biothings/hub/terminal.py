"""
Command runner behind the Hub terminal (BioThings Studio's terminal and ``biothings-cli hub``).

The terminal runs the commands registered in the hub shell: the built-in commands every hub
provides according to its features (``dump_all``, ``upload_all``, ``sync``, ``status``, ...) and
the commands defined by hook files found in ``config.HOOKS_FOLDER``. Unlike the SSH console,
which is a python interpreter, the terminal only calls registered commands with literal
arguments, so no arbitrary code can be executed through the Hub API.

A command line uses either a python-like or a shell-like syntax. Commands can be chained
with ``&&`` to run one after the other, stopping at the first failure::

    dump("mygene", force=True)
    dump mygene --force
    dump all                                  # multi-word names: same as dump_all
    auto_archive covid19 --days 3 --no-dryrun
    dump mygene && upload mygene

In the shell-like syntax, values are converted according to the parameter they're given to
(its type annotation or default value), otherwise parsed as python literals when possible
(numbers, true/false, none, lists, dicts), and kept as strings otherwise.

Commands returning a job (asyncio task, future, coroutine...) run in the background and are
tracked like any other hub command (see ``commands()`` and ``command(id)``), other commands
return their result right away. What a command prints and logs while it's called is returned
too (outputs and log statements of jobs running in the background go to the hub logs).
"""

import ast
import asyncio
import concurrent.futures
import difflib
import inspect
import json
import keyword
import logging
import os
import re
import shlex
import threading
import traceback
import types
import typing
from collections import OrderedDict
from dataclasses import dataclass, field
from functools import partial
from pprint import pformat

from biothings import config
from biothings.utils.hub import (
    AlreadyRunningException,
    CommandError,
    CommandInformation,
    CommandNotAllowed,
    CompositeCommand,
    NoSuchCommand,
)
from biothings.utils.redirect_streams import RedirectStdStreams
from biothings.utils.serializer import to_json

logger = config.logger

# a word which can be part of a command name typed with spaces ("dump all") or hyphens ("dump-all")
NAME_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
INTEGER = re.compile(r"[-+]?(0|[1-9][0-9]*)")
FLOAT = re.compile(r"[-+]?([0-9]+\.[0-9]*|\.[0-9]+|[0-9]+(\.[0-9]*)?[eE][-+]?[0-9]+)")
PYTHON_CALL = re.compile(r"\s*[A-Za-z_][A-Za-z0-9_]*\s*\(")
TRUE_VALUES = {"true", "yes", "on", "1"}
FALSE_VALUES = {"false", "no", "off", "0"}
CONSTANT_TYPES = (str, int, float, bool, type(None))
SIMPLE_TYPES = {t.__name__: t for t in (bool, int, float, str, list, tuple, dict, set)}
MAX_COMPOSITE_DEPTH = 5
MISSING = object()
UNION_TYPES = (typing.Union, getattr(types, "UnionType", typing.Union))
PARAM_KINDS = {
    inspect.Parameter.POSITIONAL_ONLY: "positional",
    inspect.Parameter.POSITIONAL_OR_KEYWORD: "positional",
    inspect.Parameter.VAR_POSITIONAL: "varargs",
    inspect.Parameter.KEYWORD_ONLY: "option",
    inspect.Parameter.VAR_KEYWORD: "kwargs",
}


# Description and examples of the built-in commands, as shown by terminal clients.
# Other commands (eg. defined in hook files) are described by their docstring.
BUILTIN_HELP = {
    # hub
    "status": (
        "Summary of the hub: number of sources, documents, builds and APIs (launched commands: see commands)",
        ["status"],
    ),
    "envs": (
        "Index, snapshot and release environments (INDEX_CONFIG, SNAPSHOT_CONFIG, RELEASE_CONFIG), secrets hidden",
        ["envs"],
    ),
    "help": ("Help about a command, or list all commands", ["help", "help dump"]),
    "commands": ("Commands launched on the hub, with their status", ["commands", "commands --running true"]),
    "command": ("Status and results of a launched command", ["command 12"]),
    "hooks": ("Hook files loaded from the hooks folder, with the commands they define", ["hooks"]),
    "config": ("Hub configuration, or one of its parameters", ["config", "config HUB_MAX_WORKERS"]),
    "setconf": (
        "Change a configuration parameter, saved in the hub database (requires CONFIG_READONLY = False)",
        ["setconf HUB_MAX_WORKERS 4"],
    ),
    "resetconf": (
        "Reset a configuration parameter to its default value, or all of them if no name is given",
        ["resetconf HUB_MAX_WORKERS"],
    ),
    "restart": ("Restart the hub", ["restart"]),
    "stop": ("Stop the hub", ["stop"]),
    "backup": (
        "Backup the hub database (sources, builds, commands...) to a file, in the hub's folder by default",
        ["backup", "backup --folder /data/backups"],
    ),
    "restore": (
        "Restore the hub database from a backup file (see backup)",
        ["restore biothings_backup_20240101_abcdefgh.pyobj"],
    ),
    "upgrade": ("Update the code of the application or of the BioThings SDK (git pull)", ["upgrade application"]),
    "top": ("Jobs running in the job manager", ["top"]),
    "sch": ("Scheduled jobs", ["sch"]),
    # data sources
    "dump": ("Download the data of a source", ["dump mygene", "dump mygene --force"]),
    "dump_all": ("Download the data of all the data sources, except manual ones", ["dump all", "dump all --force"]),
    "check": ("Check if a new release of a source is available, without downloading it", ["check mygene"]),
    "mark_dump_success": (
        "Mark a source as downloaded, without downloading anything (dry run unless --no-dry-run)",
        ["mark_dump_success mygene", "mark_dump_success mygene --no-dry-run"],
    ),
    "upload": ("Upload the downloaded data of a source (or sub-source) to the source database", ["upload mygene"]),
    "upload_all": ("Upload the downloaded data of all the data sources", ["upload all"]),
    "update_source_meta": ("Update the metadata of a source (version, license...)", ["update_source_meta mygene"]),
    "sources": ("All the sources, with their dump and upload information", ["sources"]),
    "source_info": ("Details about a source: dump, upload, mapping...", ["source_info mygene"]),
    "source_summary": (
        "Download and upload status of each data source: release, dates, documents, errors",
        ["source_summary"],
    ),
    "source_reset": (
        "Delete the information stored about a source's upload (or its dump, or inspection)",
        ["source_reset mygene", "source_reset mygene download"],
    ),
    "inspect": (
        "Inspect the data of a source or a build to report its structure, stats or mapping",
        ["inspect [src,mygene] --mode mapping", "inspect mygene_20240101_abcdefgh --mode stats"],
    ),
    "flatten_inspection_data": ("Inspection results of a source or a build, flattened (used by BioThings Studio)", []),
    # data plugins
    "register_url": (
        "Register a data plugin from a git repository",
        ["register_url https://github.com/sirloon/mvcgi.git"],
    ),
    "unregister_url": ("Unregister a data plugin, and delete its code", ["unregister_url --name mvcgi"]),
    "dump_plugin": ("Download (git clone/pull) the code of a data plugin", ["dump_plugin mvcgi"]),
    "export_plugin": ("Export a data plugin as python code", ["export_plugin mvcgi"]),
    # builds
    "whatsnew": ("Sources updated since the last build of each build configuration", ["whatsnew"]),
    "builds": ("All the builds", ["builds"]),
    "build": ("Details about a build", ["build mygene_20240101_abcdefgh"]),
    "lsmerge": ("List the builds, optionally for a build configuration only", ["lsmerge", "lsmerge mygene"]),
    "build_summary": (
        "Summary of a build: configuration, sources, steps, index, snapshot, release, pending actions (secrets hidden)",
        ["build_summary mygene_20240101_abcdefgh"],
    ),
    "build_config": (
        "Build configurations, or one of them, with their builds",
        ["build_config", "build_config mygene"],
    ),
    "merge": ("Create a new build from a build configuration", ["merge mygene"]),
    "rmmerge": ("Delete a build", ["rmmerge mygene_20240101_abcdefgh"]),
    "archive": ("Archive a build: delete its data but keep its metadata", ["archive mygene_20240101_abcdefgh"]),
    "auto_archive": (
        "Archive the builds of a build configuration older than some days",
        ["auto_archive mygene --days 30 --no-dryrun"],
    ),
    "diff": (
        "Compute the differences between two builds",
        ["diff jsondiff-selfcontained mygene_20240101_abcdefgh mygene_20240201_ijklmnop"],
    ),
    "report": (
        "Report about the differences between two builds (see diff)",
        ["report mygene_20240101_abcdefgh mygene_20240201_ijklmnop"],
    ),
    "sync": (
        "Apply the differences between two builds (see diff) to a target, Elasticsearch (es) or MongoDB (mongo)",
        [
            "sync es mygene_20240101_abcdefgh mygene_20240201_ijklmnop "
            "--target-backend [localhost:9200,mygene_20240101_abcdefgh,gene]"
        ],
    ),
    # indices, snapshots and releases
    "index": (
        "Index a build in an Elasticsearch environment (see INDEX_CONFIG)",
        ["index local mygene_20240101_abcdefgh"],
    ),
    "index_cleanup": (
        "Delete older indices, keeping the most recent ones (dry run unless --no-dryrun)",
        ["index_cleanup", "index_cleanup local --keep 3 --no-dryrun"],
    ),
    "indexes_by_name": (
        "Find indices by name (wildcards allowed) in the Elasticsearch environments (see INDEX_CONFIG)",
        ["indexes_by_name", "indexes_by_name mygene*"],
    ),
    "snapshot": (
        "Snapshot an index in a snapshot environment (see SNAPSHOT_CONFIG)",
        ["snapshot s3_env mygene_20240101_abcdefgh"],
    ),
    "snapshot_cleanup": (
        "Delete older snapshots, keeping the most recent ones (dry run unless --no-dryrun)",
        ["snapshot_cleanup", "snapshot_cleanup --keep 3 --no-dryrun"],
    ),
    "list_snapshots": ("List the snapshots", ["list_snapshots"]),
    "delete_snapshots": (
        "Delete snapshots, given by snapshot environment",
        ['delete_snapshots \'{"s3_env": ["mygene_20240101_abcdefgh"]}\''],
    ),
    "validate_snapshots": (
        "Delete the records of the snapshots which don't exist anymore in their environment",
        ["validate_snapshots"],
    ),
    "list_mongo_builds": ("List the build collections in MongoDB", ["list_mongo_builds"]),
    "delete_mongo_builds": (
        "Delete builds: their MongoDB collections and their records",
        ["delete_mongo_builds [mygene_20240101_abcdefgh]"],
    ),
    "validate_mongo_builds": (
        "Delete the records of the builds whose MongoDB collection doesn't exist anymore (archived builds are kept)",
        ["validate_mongo_builds"],
    ),
    "create_release_note": (
        "Create the release note between two builds",
        ["create_release_note mygene_20240101_abcdefgh mygene_20240201_ijklmnop"],
    ),
    "get_release_note": (
        "Release note between two builds (see create_release_note)",
        ["get_release_note mygene_20240101_abcdefgh mygene_20240201_ijklmnop"],
    ),
    "publish": (
        "Publish a release, full (snapshot) or incremental (diff), to a release environment (see RELEASE_CONFIG)",
        ["publish s3_env mygene_20240201_ijklmnop"],
    ),
    "publish_diff": (
        "Publish an incremental release (diff) of a build",
        ["publish_diff s3_env mygene_20240201_ijklmnop"],
    ),
    "publish_snapshot": (
        "Publish a full release (snapshot) of a build",
        ["publish_snapshot s3_env mygene_20240201_ijklmnop"],
    ),
    "quick_index": ("Build and index a single source, to quickly test it", []),
    # installing data releases published by other hubs (see VERSION_URLS)
    "list": ("BioThings APIs whose data releases can be installed (see VERSION_URLS)", ["list"]),
    "versions": ("Data releases available for a BioThings API", ["versions mygene.info"]),
    "info": ("Release note of a data release, the latest one by default", ["info mygene.info"]),
    "install": (
        "Install a data release, the latest one by default, applying full and incremental updates as needed",
        ["install mygene.info", "install mygene.info --dry"],
    ),
    "download": ("Download a data release, without installing it (see install)", ["download mygene.info"]),
    "apply": ("Install a downloaded data release (see download)", ["apply mygene.info"]),
    "backend": ("Elasticsearch index a BioThings API's data releases are installed in", ["backend mygene.info"]),
    "reset_backend": (
        "Delete the Elasticsearch index a BioThings API's data releases are installed in",
        ["reset_backend mygene.info"],
    ),
    # other
    "export_command_documents": ("Write the documentation of the hub commands to a file", []),
}

# More about the commands of a build's release, shown by "help <command>" before their docstring
BUILTIN_DETAILS = {
    "merge": (
        "Merges the sources of a build configuration into a new build, in the target database, named "
        "<configuration>_<version>_<random>: its version is the date (YYYYMMDD) unless the configuration's "
        "build_version says otherwise. Its sources must have been uploaded successfully. If the configuration "
        "automates the next steps (autobuild, see build_config), they're queued once merged: diff and/or "
        "snapshot (pending, see build_summary). Otherwise: index it, then snapshot it, and/or diff it against "
        "a previous build."
    ),
    "diff": (
        "Computes the differences between two builds, in DIFF_PATH: used by report, sync and publish_diff. "
        "Then, in background, the new build's release note is created (pending: release_note, see build_summary)."
    ),
    "index": (
        "Indexes a build in an environment of INDEX_CONFIG (see envs), in an index named after the build "
        "(unless --index-name). Then: snapshot <environment> <index>."
    ),
    "snapshot": (
        "Snapshots an index in the repository of an environment of SNAPSHOT_CONFIG (see envs), eg. an S3 "
        "bucket, named after the index (unless --snapshot). Then, in background, the build's release note is "
        "created against the previous build (pending: release_note, see build_summary), which publish_snapshot "
        "waits for."
    ),
    "publish_snapshot": (
        "Publishes a full release from a build's snapshot, to an environment of RELEASE_CONFIG (see envs): "
        "uploads the build's release note (created after the snapshot) and the release's metadata (<version>.json, "
        "versions.json and latest.json updated) to the release bucket. The snapshot data stays in its repository."
    ),
    "publish_diff": (
        "Publishes an incremental release from a build's diff against a previous build (see diff), to an "
        "environment of RELEASE_CONFIG (see envs): uploads the diff files to the diff bucket, then the release "
        "note and the release's metadata (<version>.json, versions.json, latest.json) to the release bucket."
    ),
    "publish": (
        "Runs publish_snapshot for a snapshot, or publish_diff for a build with a diff. A build with both a "
        "snapshot and a diff is ambiguous: use publish_snapshot or publish_diff."
    ),
    "create_release_note": (
        "Creates the release note between two builds (source versions, document counts, fields added and removed, "
        "and with a diff, the documents added, updated and deleted), in RELEASE_PATH. Created automatically "
        "after a snapshot or a diff, against the previous build."
    ),
}

# Commands deleting or overwriting data, or stopping the hub: terminals ask for a confirmation before
# running them. True: always, or the name of the parameter making it a dry run: unless it's a dry run.
# A hook command can ask for one too, with a "confirm" attribute (eg. "purge.confirm = True").
CONFIRM = {
    "rmmerge": True,
    "archive": True,
    "auto_archive": "dryrun",
    "delete_mongo_builds": True,
    "validate_mongo_builds": True,
    "delete_snapshots": True,
    "validate_snapshots": True,
    "index_cleanup": "dryrun",
    "snapshot_cleanup": "dryrun",
    "source_reset": True,
    "mark_dump_success": "dry_run",
    "unregister_url": True,
    "reset_backend": True,
    "resetconf": True,
    "restore": True,
    "upgrade": True,
    "stop": True,
}


class UnknownCommand(NoSuchCommand):
    """Raised when a command line doesn't start with a known command"""

    def __init__(self, name, suggestions=None):
        self.name = name
        self.suggestions = list(suggestions or [])
        message = "Unknown command '%s'" % name
        if self.suggestions:
            message += ", did you mean: %s?" % ", ".join(self.suggestions)
        super().__init__(message)


class CommandUsageError(CommandError):
    """Raised when a command is called with invalid arguments"""

    def __init__(self, message, usage=None):
        self.usage = usage
        super().__init__(message)


class ConfirmationRequired(CommandError):
    """Raised when a command line must be confirmed before running (see CONFIRM)"""

    def __init__(self, reasons):
        # what must be confirmed, one line per command
        self.reasons = reasons
        super().__init__("Confirmation required: %s" % "; ".join(reasons))


@dataclass
class Invocation:
    """A command and the arguments it will be called with"""

    name: str
    command: object
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    # command line as registered in the hub command history, eg. "dump('mygene', force=True)"
    display: str = ""
    # for a composite command, the invocations it's made of
    steps: list = field(default_factory=list)


def split_chain(line):
    """Split a command line on the '&&' operators found outside quoted strings"""
    parts = []
    current = []
    quote = None
    escaped = False
    idx = 0
    while idx < len(line):
        char = line[idx]
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif quote:
            if char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif line.startswith("&&", idx):
            parts.append("".join(current).strip())
            current = []
            idx += 2
            continue
        current.append(char)
        idx += 1
    parts.append("".join(current).strip())
    return parts


def render(value):
    """Text representation of a command result, as displayed in terminals"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return to_json(value, indent=True)
    except Exception:
        return pformat(value)


def json_safe(value):
    """Return value if it can be serialized as JSON, its text representation otherwise"""
    if isinstance(value, BaseException):
        return "%s: %s" % (type(value).__name__, value)
    if isinstance(value, list) and any(isinstance(item, BaseException) for item in value):
        # eg. results of jobs run with asyncio.gather(..., return_exceptions=True)
        value = [json_safe(item) if isinstance(item, BaseException) else item for item in value]
    try:
        to_json(value)
        return value
    except Exception:
        return pformat(value)


def get_signature(command):
    try:
        return inspect.signature(command)
    except (TypeError, ValueError):
        return None


def unwrap(command):
    """Return the actual function behind a command (partial, decorated function...)"""
    while isinstance(command, partial):
        command = command.func
    return inspect.unwrap(command)


def public_signature(command, signature):
    """
    Signature shown to users, without the arguments a command gives itself, ie. the keywords
    of a functools.partial (eg. check_only=True for "check", which is dump_src(check_only=True))
    """
    preset = set()
    while isinstance(command, partial):
        preset.update(command.keywords)
        command = command.func
    if signature is None or not preset:
        return signature
    return signature.replace(parameters=[p for p in signature.parameters.values() if p.name not in preset])


def param_type(param):
    """
    Type used to convert a value given as a string to that parameter,
    from its annotation or default value. None when unknown.
    """
    if param is None:
        return None
    annotation = param.annotation
    if isinstance(annotation, str):  # postponed evaluation of annotations
        names = [part.strip() for part in re.sub(r"^Optional\[(.*)\]$", r"\1", annotation).split("|")]
        names = [name for name in names if name != "None"]
        annotation = SIMPLE_TYPES.get(names[0]) if len(names) == 1 else None
    elif typing.get_origin(annotation) in UNION_TYPES:
        candidates = [arg for arg in typing.get_args(annotation) if arg is not type(None)]
        annotation = candidates[0] if len(candidates) == 1 else None
    if typing.get_origin(annotation) in (list, tuple, dict, set):
        annotation = typing.get_origin(annotation)
    if annotation in SIMPLE_TYPES.values():
        return annotation
    default = param.default
    if default is not param.empty and type(default) in SIMPLE_TYPES.values():
        return type(default)
    return None


def parse_literal(raw):
    """
    Parse a value typed in a terminal, without type information: numbers, booleans, none,
    python/JSON lists, dicts, tuples or quoted strings, and lists of unquoted words ([a,b]).
    Anything else is kept as a string.
    """
    lowered = raw.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in ("none", "null"):
        return None
    if INTEGER.fullmatch(raw):
        return int(raw)
    if FLOAT.fullmatch(raw):
        return float(raw)
    if raw and raw[0] in "[{(\"'":
        try:
            return ast.literal_eval(raw)
        except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
            pass
        try:
            return json.loads(raw)
        except ValueError:
            pass
        if raw[0] == "[" and raw[-1] == "]":
            # a list whose quotes were removed by shell-like parsing, eg. --sources ["a","b"] typed unquoted
            return [parse_literal(item.strip()) for item in raw[1:-1].split(",") if item.strip()]
    return raw


def convert(raw, kind=None):
    """Convert a value typed in a terminal to the given type (see param_type())"""
    if kind is str:
        return raw
    if kind is bool:
        if raw.lower() in TRUE_VALUES:
            return True
        if raw.lower() in FALSE_VALUES:
            return False
        raise CommandUsageError("Invalid boolean value '%s', use true or false" % raw)
    if kind in (int, float):
        try:
            return kind(raw)
        except ValueError:
            raise CommandUsageError("Invalid %s value '%s'" % (kind.__name__, raw))
    if kind in (list, tuple, set, dict):
        value = parse_literal(raw)
        if kind is dict:
            if isinstance(value, dict):
                return value
            raise CommandUsageError("Invalid dict value '%s', use a JSON object" % raw)
        if isinstance(value, (list, tuple, set)):
            return kind(value)
        # comma separated values, eg. --steps mapping,content
        return kind(parse_literal(item.strip()) for item in raw.split(",") if item.strip())
    return parse_literal(raw)


def usage(name, signature):
    """Shell-like usage of a command, eg. "dump <src> [--force] [--skip-manual] [--<option> <value>...]" """
    if signature is None:
        return "%s [<arg>...]" % name
    parts = [name]
    for param in signature.parameters.values():
        option = param.name.replace("_", "-")
        if param.kind == param.VAR_POSITIONAL:
            parts.append("[<%s>...]" % param.name)
        elif param.kind == param.VAR_KEYWORD:
            parts.append("[--<option> <value>...]")
        elif param.default is param.empty:
            parts.append("--%s <value>" % option if param.kind == param.KEYWORD_ONLY else "<%s>" % param.name)
        elif param_type(param) is bool:
            parts.append("[--no-%s]" % option if param.default else "[--%s]" % option)
        else:
            parts.append("[--%s <value>]" % option)
    return " ".join(parts)


def format_call(name, arg_texts, kwarg_texts, signature=None):
    """
    Command line as recorded in the command history, eg. "dump('mygene', force=True)", from
    formatted arguments. Keyword arguments are sorted following the command's signature
    so the same call always gives the same command line.
    """
    order = list(signature.parameters) if signature is not None else []
    keys = sorted(kwarg_texts, key=lambda key: (order.index(key), "") if key in order else (len(order), key))
    params = list(arg_texts) + ["%s=%s" % (key, kwarg_texts[key]) for key in keys]
    return "%s(%s)" % (name, ", ".join(params))


def defined_in(value, path):
    """True if value is a function defined in the python file 'path'"""
    try:
        func = inspect.unwrap(value)
    except ValueError:
        return False
    code = getattr(func, "__code__", None)
    return inspect.isfunction(func) and code is not None and os.path.abspath(code.co_filename) == path


async def json_safe_result(job):
    return json_safe(await job)


class CapturedLogs(logging.Handler):
    """
    Log statements of a command while it runs in the current thread, so terminals can show
    them with its outputs (eg. hook commands reporting what they do with logger.info())
    """

    def __init__(self, limit=200):
        super().__init__(logging.INFO)
        self.thread = threading.get_ident()
        self.limit = limit
        self.lines = []
        self.skipped = 0

    def emit(self, record):
        if record.thread != self.thread:
            return  # eg. a job running in a thread meanwhile
        if len(self.lines) >= self.limit:
            self.skipped += 1
            return
        try:
            message = record.getMessage()
        except Exception:  # arguments not matching the message's format
            message = str(record.msg)
        self.lines.append(message if record.levelno <= logging.INFO else "%s: %s" % (record.levelname, message))

    def __enter__(self):
        logging.getLogger().addHandler(self)
        return self

    def __exit__(self, *exc_info):
        logging.getLogger().removeHandler(self)

    def get_lines(self):
        if self.skipped:
            return self.lines + ["... %s more lines in the hub logs" % self.skipped]
        return list(self.lines)


class HubTerminal:
    """
    Parse, run and describe the commands registered in a hub shell (HubShell instance),
    and register the commands defined in hook files.
    """

    def __init__(self, shell, hooks_folder=None):
        self.shell = shell
        self.hooks_folder = hooks_folder
        # hook file path => loading information (commands, errors...)
        self.hooks = OrderedDict()
        # command name => name of the hook file defining that command
        self.origins = {}

    # ---------------------------------------------------------------------------------
    # Commands
    # ---------------------------------------------------------------------------------

    def runnable_commands(self):
        """Commands that can be called from the terminal: callables and composite commands"""
        return OrderedDict(
            (name, command)
            for name, command in self.shell.commands.items()
            if callable(command) or isinstance(command, CompositeCommand)
        )

    def get_command(self, name):
        """Return (canonical name, command) for a command name, accepting hyphens (dump-all)"""
        commands = self.runnable_commands()
        canonical = name.replace("-", "_")
        if canonical in commands:
            return canonical, commands[canonical]
        if canonical in self.shell.commands:
            raise CommandNotAllowed("'%s' can't be run from the terminal, it's not a command" % name)
        raise UnknownCommand(name, difflib.get_close_matches(canonical, list(commands), n=3))

    def describe(self, name):
        """Describe a command: documentation, signature, usage, parameters and origin"""
        name, command = self.get_command(name)
        signature = None
        params = []
        if isinstance(command, CompositeCommand):
            doc = "Composite command, runs: %s" % command.cmd
        else:
            doc = inspect.getdoc(unwrap(command)) or ""
            signature = public_signature(command, get_signature(command))
        if signature is not None:
            for param in signature.parameters.values():
                params.append(
                    {
                        "name": param.name,
                        "kind": PARAM_KINDS[param.kind],
                        "required": param.default is param.empty
                        and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD),
                        "default": None if param.default is param.empty else repr(param.default),
                        "type": getattr(param_type(param), "__name__", None),
                    }
                )
        summary, examples = BUILTIN_HELP.get(name, (None, [])) if name not in self.origins else (None, [])
        details = BUILTIN_DETAILS.get(name) if name not in self.origins else None
        if details:
            doc = details + ("\n\n" + doc if doc else "")
        return {
            "name": name,
            "summary": summary or (doc.strip().splitlines()[0] if doc.strip() else ""),
            "doc": doc,
            "examples": examples,
            "signature": "%s%s" % (name, signature) if signature is not None else "%s()" % name,
            "usage": usage(name, signature) if not isinstance(command, CompositeCommand) else name,
            "params": params,
            "origin": "hook" if name in self.origins else "builtin",
            "hook": self.origins.get(name),
            "hidden": bool(self.shell.hidden.get(name, False)),
            "is_async": inspect.iscoroutinefunction(unwrap(command)) if callable(command) else False,
            "composite": command.cmd if isinstance(command, CompositeCommand) else None,
            "confirm": self.confirmation(name, command),
        }

    def confirmation(self, name, command):
        """
        When a command asks for a confirmation before running (see CONFIRM): True (always), the name
        of the parameter making it a dry run (unless it's a dry run), or None (never)
        """
        rule = getattr(unwrap(command), "confirm", None) if callable(command) else None
        if rule is True or isinstance(rule, str):
            return rule
        return CONFIRM.get(name)

    def needs_confirmation(self, invocation):
        """True if a command, called with these arguments, must be confirmed before running"""
        rule = self.confirmation(invocation.name, invocation.command)
        if rule is None or rule is True:
            return bool(rule)
        signature = get_signature(invocation.command)
        if signature is None or rule not in signature.parameters:
            return True
        try:
            arguments = signature.bind(*invocation.args, **invocation.kwargs)
        except TypeError:
            return True
        arguments.apply_defaults()
        return not arguments.arguments[rule]

    def catalog(self):
        """Commands available from the terminal and hook files loaded, as consumed by terminal clients"""
        commands = []
        for name in self.runnable_commands():
            try:
                commands.append(self.describe(name))
            except Exception as e:
                logger.warning("Can't describe command '%s': %s", name, e)
        return {
            "commands": commands,
            "hooks": self.hook_info(),
            "hooks_folder": self.hooks_folder,
        }

    # ---------------------------------------------------------------------------------
    # Parsing
    # ---------------------------------------------------------------------------------

    def parse(self, line=None, argv=None):
        """
        Parse a command line, or a list of arguments already split by a shell (argv),
        into a list of Invocation objects (more than one when commands are chained with '&&')
        """
        if argv is not None:
            if not isinstance(argv, (list, tuple)) or not all(isinstance(arg, str) for arg in argv):
                raise CommandError("argv must be a list of strings")
            if len(argv) == 1:
                # a whole command line given as one argument, eg. "dump('mygene', force=True)"
                line = argv[0]
            else:
                chain = [[]]
                for arg in argv:
                    if arg == "&&":
                        chain.append([])
                    else:
                        chain[-1].append(arg)
                if not all(chain):
                    raise CommandError(
                        "No command given" if len(chain) == 1 else "'&&' must be used between two commands"
                    )
                return [self.parse_tokens(tokens) for tokens in chain]
        if not isinstance(line, str):
            raise CommandError("A command line (string) is required")
        parts = split_chain(line)
        if parts == [""]:
            raise CommandError("No command given")
        if not all(parts):
            raise CommandError("'&&' must be used between two commands")
        return [self.parse_command(part) for part in parts]

    def parse_command(self, text, depth=0):
        """Parse one command, using python-like syntax if it's a call, shell-like syntax otherwise"""
        call = self.parse_python_call(text)
        if call is None:
            try:
                tokens = shlex.split(text)
            except ValueError as e:
                raise CommandError("Can't parse '%s': %s" % (text, e))
            return self.parse_tokens(tokens, depth=depth)

        name, args, kwargs, arg_texts, kwarg_texts = call
        name, command = self.get_command(name)
        if isinstance(command, CompositeCommand):
            if args or kwargs:
                raise CommandUsageError("'%s' is a composite command, it takes no argument" % name)
            return self.composite(name, command, depth)
        signature = get_signature(command)
        if signature is not None:
            try:
                signature.bind(*args, **kwargs)
            except TypeError as e:
                raise CommandUsageError("%s: %s" % (name, e), usage(name, public_signature(command, signature)))
        return Invocation(name, command, args, kwargs, format_call(name, arg_texts, kwarg_texts, signature))

    def parse_python_call(self, text):
        """
        Parse python-like calls such as 'dump("mygene", force=True)', returning
        (name, args, kwargs, formatted args, formatted kwargs), or None if text isn't a call.
        """
        try:
            node = ast.parse(text.strip(), mode="eval").body
        except SyntaxError as e:
            if PYTHON_CALL.match(text):
                raise CommandError("Invalid syntax in '%s': %s" % (text, e.msg))
            return None
        if not isinstance(node, ast.Call):
            return None
        if not isinstance(node.func, ast.Name):
            raise CommandNotAllowed(
                "Only hub commands can be called from the terminal, '%s' isn't one "
                "(python code can only be run from the hub console)" % ast.unparse(node.func)
            )
        args, kwargs, arg_texts, kwarg_texts = [], {}, [], {}
        for arg in node.args:
            if isinstance(arg, ast.Starred):
                raise CommandNotAllowed("'*' arguments aren't supported from the terminal")
            value, text = self.literal(arg)
            args.append(value)
            arg_texts.append(text)
        for kwarg in node.keywords:
            if kwarg.arg is None:
                raise CommandNotAllowed("'**' arguments aren't supported from the terminal")
            kwargs[kwarg.arg], kwarg_texts[kwarg.arg] = self.literal(kwarg.value)
        return node.func.id, args, kwargs, arg_texts, kwarg_texts

    def literal(self, node):
        """
        Evaluate an argument given in a python-like call. Only literals are allowed, and names
        referring to hub commands (eg. help(dump)) or constants (eg. top(pending)).
        """
        if isinstance(node, ast.Name):
            if node.id in self.shell.commands:
                return self.shell.commands[node.id], node.id
            value = self.shell.extra_ns.get(node.id, MISSING)
            if value is not MISSING and isinstance(value, CONSTANT_TYPES):
                return value, node.id
            raise CommandError("Unknown name '%s' (strings must be quoted, eg. \"%s\")" % (node.id, node.id))
        try:
            value = ast.literal_eval(node)
        except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
            raise CommandNotAllowed(
                "Only literal values (strings, numbers, lists, dicts...) can be given to commands, "
                "got '%s'" % ast.unparse(node)
            )
        return value, repr(value)

    def parse_tokens(self, tokens, depth=0):
        """Parse a shell-like command, already split into tokens (eg. ["dump", "mygene", "--force"])"""
        if not tokens:
            raise CommandError("No command given")
        commands = self.runnable_commands()
        words = []
        for token in tokens:
            if not NAME_WORD.fullmatch(token):
                break
            words.append(token.replace("-", "_"))
        # multi-word names: longest match first, so "dump all" runs dump_all, not dump("all")
        for size in range(len(words), 0, -1):
            name = "_".join(words[:size])
            if name in commands:
                return self.bind(name, commands[name], tokens[size:], depth)
        if words and words[0] in self.shell.commands:
            raise CommandNotAllowed("'%s' can't be run from the terminal, it's not a command" % words[0])
        candidates = ["_".join(words[:2]), words[0]] if words else [tokens[0]]
        suggestions = []
        for candidate in candidates:
            for match in difflib.get_close_matches(candidate, list(commands), n=3):
                if match not in suggestions:
                    suggestions.append(match)
        raise UnknownCommand(tokens[0], suggestions[:3])

    def bind(self, name, command, tokens, depth=0):
        """Turn shell-like arguments into the args/kwargs a command is called with"""
        if isinstance(command, CompositeCommand):
            if tokens:
                raise CommandUsageError("'%s' is a composite command, it takes no argument" % name)
            return self.composite(name, command, depth)
        signature = get_signature(command)
        cmd_usage = usage(name, public_signature(command, signature))
        try:
            positionals, options = self.parse_options(name, signature, tokens)
            args, kwargs = self.arrange(signature, positionals, options)
            if signature is not None:
                signature.bind(*args, **kwargs)
        except TypeError as e:  # from signature.bind()
            raise CommandUsageError("%s: %s" % (name, e), cmd_usage)
        except CommandUsageError as e:
            e.usage = e.usage or cmd_usage
            raise
        call = format_call(name, [repr(arg) for arg in args], {k: repr(v) for k, v in kwargs.items()}, signature)
        return Invocation(name, command, args, kwargs, call)

    def parse_options(self, name, signature, tokens):
        """
        Split shell-like arguments into positional values (raw strings, see arrange()) and options
        (--name value, --flag, --no-flag, name=value), converted according to the command's signature
        """
        params = signature.parameters if signature is not None else {}
        accepts_any = signature is None or any(p.kind == p.VAR_KEYWORD for p in params.values())
        not_options = (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        )
        positionals = []
        options = {}

        def set_option(key, value):
            if key in options:
                raise CommandUsageError("Option '%s' given more than once" % key)
            options[key] = value

        idx = 0
        only_positionals = False
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            if only_positionals:
                positionals.append(token)
            elif token == "--":
                only_positionals = True
            elif token.startswith("--"):
                key, has_value, raw = token[2:].partition("=")
                key = key.replace("-", "_")
                if not IDENTIFIER.fullmatch(key):
                    raise CommandUsageError("Invalid option '%s'" % token)
                param = params.get(key)
                if param is None and key.startswith("no_") and not has_value and key[3:] in params:
                    negated = params[key[3:]]
                    if negated.kind not in not_options and param_type(negated) in (bool, None):
                        set_option(negated.name, False)
                        continue
                if param is not None and param.kind in not_options:
                    raise CommandUsageError("'%s' can't be given as an option" % param.name)
                if param is None and not accepts_any:
                    raise CommandUsageError("Unknown option '%s' for command '%s'" % (token, name))
                kind = param_type(param)
                if has_value:
                    value = convert(raw, kind)
                elif kind is bool:
                    value = True
                    if idx < len(tokens) and tokens[idx].lower() in TRUE_VALUES | FALSE_VALUES:
                        value = convert(tokens[idx], bool)
                        idx += 1
                elif idx < len(tokens) and not tokens[idx].startswith("--"):
                    value = convert(tokens[idx], kind)
                    idx += 1
                elif kind is None:
                    value = True  # untyped flag
                else:
                    raise CommandUsageError("Option '%s' requires a value" % token)
                set_option(key, value)
            elif re.match(r"-[A-Za-z]", token):
                raise CommandUsageError("Short options aren't supported, use '--<name>' ('%s')" % token)
            else:
                key, sep, raw = token.partition("=")
                param = params.get(key)
                if (
                    sep
                    and IDENTIFIER.fullmatch(key)
                    and (param is not None or accepts_any)
                    and (param is None or param.kind not in not_options)
                ):
                    # python-like keyword argument, eg. force=true
                    set_option(key, convert(raw, param_type(param)))
                else:
                    positionals.append(token)
        return positionals, options

    def arrange(self, signature, positionals, options):
        """
        Assign positional values to the parameters that weren't given as options, in order,
        converting them according to these parameters. Returns (args, kwargs) to call the command.
        """
        if signature is None:
            return [parse_literal(raw) for raw in positionals], dict(options)
        params = list(signature.parameters.values())
        fixed = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
        varargs = next((p for p in params if p.kind == p.VAR_POSITIONAL), None)
        free = [p for p in fixed if p.name not in options]
        assigned = {}
        extra = []
        for raw in positionals:
            if free:
                param = free.pop(0)
                assigned[param.name] = convert(raw, param_type(param))
            elif varargs is not None:
                extra.append(convert(raw, param_type(varargs)))
            else:
                raise CommandUsageError("Too many arguments (%s)" % raw)
        kwargs = dict(options)
        kwargs.update(assigned)
        positional_only = [idx for idx, p in enumerate(fixed) if p.kind == p.POSITIONAL_ONLY and p.name in kwargs]
        if extra or positional_only:
            # parameters before *args, and positional-only ones, must be given positionally
            last = len(fixed) if extra else max(positional_only) + 1
        else:
            # values given positionally to the first parameters stay positional
            last = 0
            while last < len(fixed) and fixed[last].name in assigned:
                last += 1
        args = []
        for param in fixed[:last]:
            if param.name in kwargs:
                args.append(kwargs.pop(param.name))
            elif param.default is not param.empty:
                args.append(param.default)
            else:
                raise CommandUsageError("Missing argument '%s'" % param.name)
        return args + extra, kwargs

    def composite(self, name, command, depth):
        """Expand a composite command into the commands it's made of"""
        if depth >= MAX_COMPOSITE_DEPTH:
            raise CommandError("Composite command '%s' is nested too deeply" % name)
        steps = []
        for part in split_chain(command.cmd):
            invocation = self.parse_command(part, depth=depth + 1)
            steps.extend(invocation.steps or [invocation])
        return Invocation(name, command, display="%s()" % name, steps=steps)

    # ---------------------------------------------------------------------------------
    # Execution
    # ---------------------------------------------------------------------------------

    def run(self, line=None, argv=None, confirmed=False):
        """
        Run a command line (see parse()) and return a JSON-serializable description of the
        outcome: the result when the command is done, or the ID of the command running in
        background, which can then be followed with command(id). Commands deleting or
        overwriting data only run once confirmed (see CONFIRM), ConfirmationRequired otherwise.
        """
        invocations = self.parse(line=line, argv=argv)
        cmdline = " && ".join(invocation.display for invocation in invocations)
        for info in list(self.shell.launched_commands.values()):
            if info.get("cmd") == cmdline and not info.get("is_done"):
                raise AlreadyRunningException("'%s' is already running (command #%s)" % (cmdline, info.get("id")))
        steps = [step for invocation in invocations for step in (invocation.steps or [invocation])]
        if not confirmed:
            reasons = [
                "%s: %s" % (step.display, self.describe(step.name)["summary"])
                for step in steps
                if self.needs_confirmation(step)
            ]
            if reasons:
                raise ConfirmationRequired(reasons)
        if len(steps) == 1:
            return self.run_one(cmdline, steps[0])
        return self.run_chain(cmdline, steps)

    def run_one(self, cmdline, invocation):
        streams = RedirectStdStreams()
        logs = CapturedLogs()
        try:
            with streams, logs:
                result = invocation.command(*invocation.args, **invocation.kwargs)
        except Exception as e:
            logger.info("Terminal command '%s' failed: %s", cmdline, e)
            return self.failed(cmdline, e, streams.get_std_contents(), logs.get_lines())
        std = streams.get_std_contents()
        job = self.as_job(result)
        if job is not None:
            return self.started(self.shell.register_command(cmdline, job, force=True), std, logs.get_lines())
        try:
            # keep track in the command history (unless the command isn't tracked)
            self.shell.register_command(cmdline, result)
        except Exception as e:
            logger.warning("Can't register command '%s' in history: %s", cmdline, e)
        return {
            "cmd": cmdline,
            "id": None,
            "is_done": True,
            "failed": False,
            "result": json_safe(result),
            "stdout": std["stdout"],
            "stderr": std["stderr"],
            "logs": logs.get_lines(),
        }

    def run_chain(self, cmdline, steps):
        """Run commands one after the other, in background, stopping at the first failure"""

        async def chain():
            results = []
            for step in steps:
                try:
                    result = step.command(*step.args, **step.kwargs)
                    job = self.as_job(result)
                    if isinstance(job, list):
                        result = await asyncio.gather(*job)
                    elif job is not None:
                        result = await job
                except Exception as e:
                    raise CommandError("%s failed: %s" % (step.display, e)) from e
                results.append(json_safe(result))
            return results

        job = self.loop().create_task(chain())
        return self.started(self.shell.register_command(cmdline, job, force=True), None)

    def as_job(self, result):
        """
        Return result as a job which can be tracked (a Task, or a list of Tasks) if it's
        asynchronous (coroutine, future, list of futures...), None otherwise.
        """
        if inspect.iscoroutine(result) or asyncio.isfuture(result):
            task = self.loop().create_task(json_safe_result(result))
            # eg. dump_all(): which sources are done, see biothings.hub.dataload.manager.wait_for_sources()
            task.progress = getattr(result, "progress", None)
            return task
        if isinstance(result, concurrent.futures.Future):
            return self.loop().create_task(json_safe_result(asyncio.wrap_future(result)))
        if isinstance(result, (list, tuple)) and result:
            jobs = [item for item in result if inspect.iscoroutine(item) or asyncio.isfuture(item)]
            if len(jobs) == len(result):
                if all(isinstance(job, asyncio.Task) for job in jobs):
                    return list(jobs)  # tracked as is, one result per task
                return self.loop().create_task(json_safe_result(asyncio.gather(*jobs)))
        return None

    def loop(self):
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return self.shell.job_manager.loop

    def started(self, cmdinfo, std, logs=None):
        if not isinstance(cmdinfo, CommandInformation):
            raise CommandError("Job couldn't be tracked in the command history")
        std = std or {}
        return {
            "cmd": cmdinfo["cmd"],
            "id": cmdinfo["id"],
            "is_done": False,
            "failed": False,
            "started_at": cmdinfo["started_at"],
            "stdout": std.get("stdout", ""),
            "stderr": std.get("stderr", ""),
            "logs": logs or [],
        }

    def failed(self, cmdline, error, std, logs=None):
        return {
            "cmd": cmdline,
            "id": None,
            "is_done": True,
            "failed": True,
            "error": "%s: %s" % (type(error).__name__, error),
            "traceback": "".join(traceback.format_exception(type(error), error, error.__traceback__)),
            "stdout": std["stdout"],
            "stderr": std["stderr"],
            "logs": logs or [],
        }

    def is_terminal_syntax(self, line):
        """
        True when line isn't python code but a shell-like command line, starting with a word
        which isn't a python keyword (eg. "dump mygene --force"). Used to accept that syntax
        from the hub console, which otherwise evaluates python code.
        """
        try:
            ast.parse(line)
            return False
        except SyntaxError:
            pass
        try:
            tokens = shlex.split(split_chain(line)[0])
        except ValueError:
            return False
        return bool(tokens) and NAME_WORD.fullmatch(tokens[0]) is not None and not keyword.iskeyword(tokens[0])

    def console(self, line):
        """Run a shell-like command line from the hub console, returning the outputs to display"""
        try:
            # the console runs any python code already, no confirmation asked
            response = self.run(line=line, confirmed=True)
        except (NoSuchCommand, CommandNotAllowed) as e:
            raise CommandError(str(e))
        if response["failed"]:
            raise CommandError(response["error"])
        outputs = ["\n".join(response["logs"]), response["stdout"], response["stderr"]]
        if response["is_done"]:
            outputs.append(render(response["result"]))
        # else: running in background, progress is reported by the console like other commands
        return [output for output in outputs if output]

    # ---------------------------------------------------------------------------------
    # Hooks
    # ---------------------------------------------------------------------------------

    def load_hook(self, path):
        """
        Execute a hook file in the hub console namespace, so it can use any hub command
        and anything it defines is available from the console. Functions defined in the
        file (or names listed in its __all__, if any) are registered as hub commands, available
        from the terminal and the CLI too. Names starting with an underscore are kept private.
        """
        path = os.path.abspath(path)
        info = {
            "name": os.path.basename(path),
            "path": path,
            "commands": [],
            "overrides": [],
            "error": None,
        }
        self.hooks[path] = info
        namespace = self.shell.extra_ns
        namespace.pop("__all__", None)
        try:
            with open(path, encoding="utf-8") as hook_file:
                code = compile(hook_file.read(), path, "exec")
            exec(code, namespace)  # nosec: hook files are part of the hub's own code
        except Exception as e:
            info["error"] = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            namespace.pop("__all__", None)
            raise
        exported = namespace.pop("__all__", None)
        if exported is not None:
            names = []
            for name in exported:
                if not callable(namespace.get(name)):
                    logger.warning("Hook '%s': '%s' is listed in __all__ but isn't callable", info["name"], name)
                    continue
                names.append(name)
        else:
            names = [name for name, value in namespace.items() if not name.startswith("_") and defined_in(value, path)]
        for name in names:
            if name in self.shell.commands and self.origins.get(name) not in (None, info["name"]):
                logger.warning(
                    "Hook '%s': command '%s' replaces the one defined in hook '%s'",
                    info["name"],
                    name,
                    self.origins[name],
                )
                info["overrides"].append(name)
            elif name in self.shell.commands and name not in self.origins:
                logger.warning("Hook '%s': command '%s' replaces a built-in command", info["name"], name)
                info["overrides"].append(name)
            self.shell.add_command(name, namespace[name])
            self.origins[name] = info["name"]
            info["commands"].append(name)
        logger.info("Hook '%s' loaded, commands: %s", info["name"], info["commands"])
        return info

    def hook_info(self):
        """Loading information for each hook file, as a list"""
        return [dict(info) for info in self.hooks.values()]

    def hooks_summary(self):
        """List the hook files loaded from the hooks folder, with the commands they define"""
        lines = ["Hooks folder: %s" % self.hooks_folder]
        if not self.hooks:
            lines.append("  no hook file loaded")
        for info in self.hooks.values():
            if info["error"]:
                lines.append("  %s: failed to load, %s" % (info["name"], info["error"].strip().splitlines()[-1]))
            else:
                line = "  %s: %s" % (info["name"], ", ".join(info["commands"]) or "no command defined")
                if info["overrides"]:
                    line += " (replaces: %s)" % ", ".join(info["overrides"])
                lines.append(line)
        return "\n".join(lines)
