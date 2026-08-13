"""Compatibility imports for structured asyncio concurrency."""

import sys

if sys.version_info >= (3, 11):
    from asyncio import TaskGroup
    from builtins import BaseExceptionGroup, ExceptionGroup
else:
    from exceptiongroup import BaseExceptionGroup, ExceptionGroup
    from taskgroup import TaskGroup

__all__ = ["BaseExceptionGroup", "ExceptionGroup", "TaskGroup"]
