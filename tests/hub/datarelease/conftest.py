import logging
import os

from dummy_config import DATA_HUB_DB_DATABASE, HUB_DB_BACKEND

LOGGER = logging.getLogger(__name__)


def pytest_sessionstart(session):
    os.environ["HUB_CONFIG"] = "dummy_config"
    LOGGER.info("os.environ['HUB_CONFIG'] set.")


def pytest_sessionfinish(session):
    if "HUB_CONFIG" in os.environ:
        del os.environ["HUB_CONFIG"]
    LOGGER.info("os.environ['HUB_CONFIG'] reset.")

    db_folder = HUB_DB_BACKEND.get("sqlite_db_folder", ".")
    db_filename = DATA_HUB_DB_DATABASE
    db_filepath = os.path.join(db_folder, db_filename)
    if not os.path.exists(db_folder):
        os.mkdir(db_folder)
    if os.path.exists(db_filepath):
        os.remove(db_filepath)
        LOGGER.info(f"{db_filepath} deleted.")

    if not os.listdir(db_folder):
        LOGGER.info(f"{db_folder} is empty.")
        os.rmdir(db_folder)
        LOGGER.info(f"{db_folder} deleted.")
    else:
        LOGGER.info(f"{db_folder} not empty, kept as is.")
