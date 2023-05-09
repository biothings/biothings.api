import asyncio
import datetime
import gzip
import logging
import os
from collections import OrderedDict, UserList
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from functools import partial
from itertools import chain
from logging.handlers import TimedRotatingFileHandler
from threading import Thread
from types import MethodType
from typing import NamedTuple, Union

import requests

LOG_FORMAT_STRING = "%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s"
DATEFMT = "%H:%M:%S"


class GZipRotator:
    def __call__(self, source, dest):
        os.rename(source, dest)
        f_in = open(dest, "rb")
        f_out = gzip.open("%s.gz" % dest, "wb")
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        os.remove(dest)


def create_logger(log_folder, logger_name, level=logging.DEBUG):
    """
    Create and return a file logger if log_folder is provided.
    If log_folder is None, no file handler will be created.
    """
    if log_folder:
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)
        logfile = os.path.join(log_folder, logger_name)
        if not logfile.endswith(".log"):
            logfile += ".log"
    else:
        logfile = None
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    return logger, logfile


def configurate_file_handler(logger, logfile, formater=None, force=False):
    if not formater:
        formater = logging.Formatter(LOG_FORMAT_STRING, datefmt=DATEFMT)

    fh = TimedRotatingFileHandler(logfile, when="D")
    fh.setFormatter(formater)
    fh.rotator = GZipRotator()
    fh.name = "logfile"
    if force or fh.name not in [h.name for h in logger.handlers]:
        logger.addHandler(fh)


def setup_default_log(default_logger_name, log_folder, level=logging.DEBUG):
    # this will affect any logging calls
    logging.basicConfig(level=level)
    logger, logfile = create_logger(log_folder, default_logger_name, level=level)
    if logfile:
        configurate_file_handler(logger, logfile)
    return logger


def get_logger(logger_name, log_folder=None, handlers=("console", "file", "slack"), timestamp=None, force=False):
    """
    Configure a logger object from logger_name and return (logger, logfile)
    """
    from biothings import config as btconfig

    if timestamp:
        raise DeprecationWarning("Timestamp is deprecated")

    if not log_folder:
        log_folder = btconfig.LOG_FOLDER
    # this will affect any logging calls
    logger, logfile = create_logger(log_folder, logger_name)
    fmt = logging.Formatter(LOG_FORMAT_STRING, datefmt=DATEFMT)
    if "file" in handlers and logfile:
        configurate_file_handler(logger, logfile, formater=fmt, force=force)

    if "hipchat" in handlers:
        raise DeprecationWarning("Hipchat is dead...")

    if "slack" in handlers and getattr(btconfig, "SLACK_WEBHOOK", None):
        nh = SlackHandler(
            btconfig.SLACK_WEBHOOK,
            getattr(btconfig, "SLACK_MENTIONS", []),
        )
        nh.setFormatter(fmt)
        nh.name = "slack"
        if force or nh.name not in [h.name for h in logger.handlers]:
            logger.addHandler(nh)

    def notify(self, *args, **kwargs):
        return self.info(*args, **kwargs, extra={"notify": True})

    logger.notify = MethodType(notify, logger)

    return (logger, logfile)


class EventRecorder(logging.StreamHandler):
    def __init__(self, *args, **kwargs):
        super(EventRecorder, self).__init__(*args, **kwargs)
        from biothings.utils.hub_db import get_event

        self.eventcol = get_event()

    def emit(self, record):
        async def aioemit(msg):
            def recorded(f):
                # res = f.result()
                f.result()

            fut = loop.run_in_executor(None, partial(self.eventcol.save, msg))
            fut.add_done_callback(recorded)
            await fut
            return fut

        if record.__dict__.get("notify") or record.__dict__.get("event"):
            try:
                loop = asyncio.get_event_loop()
                msg = {
                    "_id": record.created,
                    "asctime": record.asctime,
                    "msg": record.message,
                    "level": record.levelname,
                    "name": record.name,
                    "pid": record.process,
                    "pname": record.processName,
                }
                fut = aioemit(msg)
                asyncio.ensure_future(fut)
            except Exception as e:
                logging.error("Couldn't record event: %s", e)


class WSLogHandler(logging.StreamHandler):
    """
    when listener is a bt.hub.api.handlers.ws.LogListener instance,
    log statements are propagated through existing websocket
    """

    def __init__(self, listener):
        super().__init__()
        self.listener = listener
        self.count = 0

    def payload(self, record):
        msg = self.format(record)
        return {
            "_id": self.count,
            "op": "log",
            "msg": msg,
            "logger": record.name,
            "level": record.levelname,
            "ts": datetime.datetime.now().isoformat(),
        }

    def emit(self, record):
        self.count += 1
        self.listener.read(self.payload(record))


class WSShellHandler(WSLogHandler):
    """
    when listener is a bt.hub.api.handlers.ws.LogListener instance,
    log statements are propagated through existing websocket
    """

    def payload(self, record):
        types = {ShellLogger.INPUT: "input", ShellLogger.OUTPUT: "output"}
        return {
            "_id": self.count,
            "op": "shell",
            "cmd": record.msg,
            "type": types.get(record.levelno, "unknown"),
            "ts": datetime.datetime.now().isoformat(),
        }


class ShellLogger(logging.Logger):
    """
    Custom "levels" for input going to the shell
    and output coming from it (just for naming)
    """

    OUTPUT = 1000
    INPUT = 1001

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manager.loggerDict[self.name] = self

    def input(self, msg, *args, **kwargs):
        self._log(self.__class__.INPUT, msg, args, **kwargs)

    def output(self, msg, *args, **kwargs):
        self._log(self.__class__.OUTPUT, msg, args, **kwargs)


# -----------------------------------------------
#                SLACK INTEGRATION
# -----------------------------------------------
# https://api.slack.com/reference/block-kit/blocks
# https://api.slack.com/reference/block-kit/composition-objects#text
# https://api.slack.com/reference/messaging/attachments


class Squares(Enum):
    CRITICAL = ":large_purple_square:"
    ERROR = ":large_red_square:"
    WARNING = ":large_orange_square:"
    INFO = ":large_blue_square:"
    DEBUG = ":white_large_square:"
    NOTSET = ""


class Colors(Enum):
    CRITICAL = "#7b0099"
    ERROR = "danger"  # red
    WARNING = "warning"  # yellow
    INFO = "good"  # green
    DEBUG = "#a1a1a1"
    NOTSET = "#d6d2d2"


@dataclass
class Range:
    start: Union[int, float] = 0  # inclusive
    end: Union[int, float] = float("inf")  # exclusive


class Record(NamedTuple):
    range: Range
    value: Enum


class LookUpList(UserList):
    def __init__(self, initlist):
        super().__init__(initlist)
        assert all(isinstance(x, Record) for x in self.data)
        assert all(isinstance(x.range, Range) for x in self.data)

    def find_index(self, val):
        _l, _r = 0, len(self.data)
        while _l < _r:
            mid = (_l + _r) // 2
            start = self.data[mid].range.start
            end = self.data[mid].range.end
            if val < start:
                _r = mid
            elif val >= end:
                _l = mid + 1
            else:  # found
                return mid

    def find(self, val):
        index = self.find_index(val)
        if index is not None:
            return self.data[index].value


ICONS = LookUpList(
    [
        Record(Range(float("-inf"), logging.DEBUG), Squares.NOTSET),
        Record(Range(logging.DEBUG, logging.INFO), Squares.DEBUG),
        Record(Range(logging.INFO, logging.WARNING), Squares.INFO),
        Record(Range(logging.WARNING, logging.ERROR), Squares.WARNING),
        Record(Range(logging.ERROR, logging.CRITICAL), Squares.ERROR),
        Record(Range(logging.CRITICAL, float("inf")), Squares.CRITICAL),
    ]
)

COLORS = LookUpList(
    [
        Record(Range(float("-inf"), logging.DEBUG), Colors.NOTSET),
        Record(Range(logging.DEBUG, logging.INFO), Colors.DEBUG),
        Record(Range(logging.INFO, logging.WARNING), Colors.INFO),
        Record(Range(logging.WARNING, logging.ERROR), Colors.WARNING),
        Record(Range(logging.ERROR, logging.CRITICAL), Colors.ERROR),
        Record(Range(logging.CRITICAL, float("inf")), Colors.CRITICAL),
    ]
)


class SlackMessage:
    def __init__(self):
        self._blocks = []
        self._attachments = []

    def markdown(self, text, prefixes=(), suffixes=()):
        text = text.strip()
        text = " ".join((*filter(None, prefixes), text))
        text = " ".join((text, *filter(None, suffixes)))
        if text:
            # empty value causes 400
            # error in slack API
            self._blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    def plaintext(self, text, color):
        text = text.strip()
        if text:
            # empty value causes 400
            # error in slack API
            self._attachments.append({"text": text, "color": color})

    def build(self):
        return deepcopy({"blocks": self._blocks, "attachments": self._attachments})


class ReceiverGroup(UserList):
    def __init__(self, initlist=None):
        super().__init__(initlist or [])
        self.prev = None
        self.range = Range()


class SlackMentionPolicy:
    # TODO Support string representation
    # of a level throughout this class

    def __init__(self, policy):
        if isinstance(policy, dict):
            assert all(isinstance(lvl, int) for lvl in policy.keys())
            assert all(isinstance(m, (list, tuple)) for m in policy.values())
            assert all(isinstance(p, str) for p in chain(*policy.values()))

            # TODO maybe a user should only subscribe to
            # one logging level considering how logging
            # propagation works?

            self._policy = OrderedDict(sorted((level, ReceiverGroup(receivers)) for level, receivers in policy.items()))

        elif isinstance(policy, (tuple, list)):
            assert all(isinstance(m, str) for m in policy)
            assert len(set(policy)) == len(policy)

            self._policy = OrderedDict({logging.ERROR: ReceiverGroup(policy)})

        elif isinstance(policy, str):
            self._policy = OrderedDict({logging.ERROR: ReceiverGroup([policy])})

        else:  # see test cases for supported values.
            raise TypeError("Unsupported Slack Mentions.")

        _prev = None
        for level, mention in self._policy.items():
            mention.prev = _prev
            mention.range.start = level
            if _prev:
                # previously configured logging receivers handle
                # all levels until the next highest log level
                _prev.range.end = level
            _prev = mention

    def mentions(self, level):
        assert isinstance(level, int)

        if level not in self._policy:
            # a logging level can be an integer between
            # the commonly defined, named ones, search for
            # which range it belongs if that's the case
            levels = list(self._policy.values())
            _l, _r = 0, len(levels)
            while _l < _r:
                mid = (_l + _r) // 2
                start = levels[mid].range.start
                end = levels[mid].range.end
                if level < start:
                    _r = mid
                elif level >= end:
                    _l = mid + 1
                else:  # found
                    level = start
                    break

        # all receivers that should be
        # mentioned for this log level
        mentions = []

        # TODO what should we do if we encounter
        # duplicated entries due to bad configurations

        if level in self._policy:
            current = self._policy[level]
            while current:
                mentions.extend(current)
                current = current.prev

        return mentions


class SlackHandler(logging.StreamHandler):
    def __init__(self, webhook, mentions):
        super(SlackHandler, self).__init__()
        self.webhook = webhook
        self.mentions = SlackMentionPolicy(mentions)

    @staticmethod
    def send(webhook, message, level, mentions=()):  # logging entry point
        msg = SlackMessage()
        msg.markdown("", suffixes=mentions)
        msg.plaintext(message, COLORS.find(level).value)
        res = requests.post(webhook, json=msg.build())
        res.raise_for_status()

    def emit(self, record):
        if record.__dict__.get("notify"):
            Thread(
                target=SlackHandler.send,  # blocking
                args=(
                    self.webhook,
                    self.format(record),
                    record.levelno,
                    self.mentions.mentions(record.levelno),
                ),
            ).start()
