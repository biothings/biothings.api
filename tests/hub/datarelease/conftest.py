""" comment out the following code for now

import os
import logging
from dummy_config import HUB_DB_BACKEND, DATA_HUB_DB_DATABASE

LOGGER = logging.getLogger(__name__)


def pytest_sessionstart(session):
    os.environ["HUB_CONFIG"] = "dummy_config"
    LOGGER.info("os.environ['HUB_CONFIG'] set.")


def pytest_sessionfinish(session):
    del os.environ["HUB_CONFIG"]
    LOGGER.info("os.environ['HUB_CONFIG'] reset.")

    db_folder = HUB_DB_BACKEND.get("sqlite_db_folder", ".")
    db_filename = DATA_HUB_DB_DATABASE
    db_filepath = os.path.join(db_folder, db_filename)

    if os.path.exists(db_filepath):
        os.remove(db_filepath)
        LOGGER.info(f"{db_filepath} deleted.")

    if not os.listdir(db_folder):
        LOGGER.info(f"{db_folder} is empty.")
        os.rmdir(db_folder)
        LOGGER.info(f"{db_folder} deleted.")
    else:
        LOGGER.info(f"{db_folder} not empty, kept as is.")
"""
