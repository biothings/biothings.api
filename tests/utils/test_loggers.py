import logging
import os
from datetime import datetime

import pytest

from biothings import config
from biothings.utils.loggers import get_logger, setup_default_log


LOGGER_NAME = "test_logger"
TIMESTAMP = "%Y%m%d"


def test_set_default_log():
    log_level = logging.WARN
    expected_file_name = os.path.join(config.LOG_FOLDER, f"{LOGGER_NAME}_{datetime.now().strftime(TIMESTAMP)}.log")

    logger = setup_default_log(LOGGER_NAME, config.LOG_FOLDER, level=log_level)

    assert logger.name == LOGGER_NAME
    assert logger.level == log_level
    assert len(logger.handlers) == 1

    file_handler = logger.handlers[0]
    assert file_handler.name == "logfile"
    assert file_handler.baseFilename == expected_file_name


class TestGetLogger:
    def test_logfile_name_using_default_log_folder(self):
        expected_log_folder = config.LOG_FOLDER

        _, log_file = get_logger(LOGGER_NAME, log_folder=None)

        assert log_file.startswith(expected_log_folder)

    def test_logfile_name_using_custom_log_folder(self):
        expected_log_folder = os.path.join(config.LOG_FOLDER, "extra_path")

        _, log_file = get_logger(LOGGER_NAME, log_folder=expected_log_folder)

        assert log_file.startswith(expected_log_folder)

    def test_logfile_name_without_timestamp(self):
        expected_logfile_name = f"{LOGGER_NAME}.log"

        _, log_file = get_logger(LOGGER_NAME, timestamp=None)

        assert log_file.endswith(expected_logfile_name)

    def test_logfile_name_with_default_timestamp(self):
        expected_logfile_name = (
            f"{LOGGER_NAME}_{datetime.now().strftime(TIMESTAMP)}"
            ".log"
        )

        _, log_file = get_logger(LOGGER_NAME)

        assert log_file.endswith(expected_logfile_name)

    def test_logfile_name_with_timestamp(self):
        expected_logfile_name = (
            f"{LOGGER_NAME}_{datetime.now().strftime(TIMESTAMP)}"
            ".log"
        )

        _, log_file = get_logger(LOGGER_NAME, timestamp=TIMESTAMP)

        assert log_file.endswith(expected_logfile_name)

    def test_logger_with_hipchat_handler(self):
        with pytest.raises(DeprecationWarning) as exc_info:
            get_logger("test_logger", handlers=["hipchat"])

        assert "Hipchat is dead..." in str(exc_info.value)

    def test_logger_with_valid_handlers(self, mocker):
        mock_objects = mocker.patch.multiple(
            "biothings.utils.configuration.ConfigurationWrapper",
            new_callable=mocker.PropertyMock,
            create=True,
            SLACK_WEBHOOK=mocker.DEFAULT,
            SLACK_MENTIONS=mocker.DEFAULT
        )
        mock_objects["SLACK_WEBHOOK"].return_value = "https://slack/webhook"
        mock_objects["SLACK_MENTIONS"].return_value = {logging.ERROR: ["admin@admin.com"]}
        expected_log_format = '%(asctime)s [%(process)d:%(threadName)s] - %(name)s - %(levelname)s -- %(message)s'
        expected_log_datefmt = "%H:%M:%S"

        logger, _ = get_logger("test_logger", handlers=("console", "file", "slack"))

        assert len(logger.handlers) == 2

        file_handler = logger.handlers[0]
        slack_handler = logger.handlers[1]

        assert file_handler.name == "logfile"
        assert file_handler.formatter._fmt == expected_log_format
        assert file_handler.formatter.datefmt == expected_log_datefmt

        assert slack_handler.name == "slack"
        assert slack_handler.formatter._fmt == expected_log_format
        assert slack_handler.formatter.datefmt == expected_log_datefmt
