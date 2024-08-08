"""
Handles setting up and accessing the logger instance
for the biothings-cli tooling
"""

import logging

from rich.logging import RichHandler
import typer


def setup_logging(cli: typer.Typer, debug: bool):
    """
    Configures the logging based off our environment configuration
    """
    logging_level = logging.INFO
    if debug:
        logging_level = logging.DEBUG

    rich_handler = RichHandler(
        level=logging_level,
        rich_tracebacks=False,  # typer creates it already
        tracebacks_suppress=[typer],
        show_path=False,
    )

    logging.basicConfig(level=logging_level, format="%(message)s", datefmt="[%X]", handlers=[rich_handler])
    logger = logging.getLogger("biothings-cli")


def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger with the given name.
    If name is None, return the root logger.
    """
    logger = logging.getLogger(name)
    return logger
