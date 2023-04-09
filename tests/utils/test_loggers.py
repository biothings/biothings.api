import logging
import os
from datetime import datetime

import pytest

from biothings import config
from biothings.utils.loggers import DATEFMT, LOG_FORMAT_STRING, GZipRotator, get_logger, setup_default_log

LOGGER_NAME = "test_logger"


def test_set_default_log():
    log_level = logging.WARN
    expected_file_name = os.path.join(config.LOG_FOLDER, LOGGER_NAME)
    expected_file_name += ".log"

    logger = setup_default_log(LOGGER_NAME, config.LOG_FOLDER, level=log_level)

    assert logger.name == LOGGER_NAME
    assert logger.level == log_level
    assert len(logger.handlers) == 1

    file_handler = logger.handlers[0]
    assert file_handler.name == "logfile"
    assert file_handler.baseFilename == expected_file_name
    assert isinstance(file_handler.rotator, GZipRotator)


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
        _, log_file = get_logger(LOGGER_NAME, timestamp=None)

        assert log_file.endswith(LOGGER_NAME + ".log")

    def test_logfile_name_with_timestamp(self):
        with pytest.raises(DeprecationWarning) as exc_info:
            get_logger(LOGGER_NAME, timestamp=DATEFMT)

        assert "Timestamp is deprecated" in str(exc_info.value)

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
            SLACK_MENTIONS=mocker.DEFAULT,
        )
        mock_objects["SLACK_WEBHOOK"].return_value = "https://slack/webhook"
        mock_objects["SLACK_MENTIONS"].return_value = {logging.ERROR: ["admin@admin.com"]}

        logger, _ = get_logger("test_logger", handlers=("console", "file", "slack"))

        assert len(logger.handlers) == 2

        file_handler = logger.handlers[0]
        slack_handler = logger.handlers[1]

        assert file_handler.name == "logfile"
        assert isinstance(file_handler.rotator, GZipRotator)
        assert file_handler.formatter._fmt == LOG_FORMAT_STRING
        assert file_handler.formatter.datefmt == DATEFMT

        assert slack_handler.name == "slack"
        assert slack_handler.formatter._fmt == LOG_FORMAT_STRING
        assert slack_handler.formatter.datefmt == DATEFMT
