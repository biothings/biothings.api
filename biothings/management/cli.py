import logging
import pathlib
import sys

import typer

from biothings.utils.common import DummyConfig

cli = typer.Typer()

logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)


@cli.callback()
def callback():
    """
    Biothing Admin CLI app. Available subcommands:\n

    biothings-admin dataplugin --help \n
    biothings-admin dataplugin-localhub --help \n

    """


def main():
    working_dir = pathlib.Path().resolve()
    _config = DummyConfig("config")
    _config.HUB_DB_BACKEND = {
        "module": "biothings.utils.sqlite3",
        "sqlite_db_folder": ".biothings_hub",
    }
    _config.DATA_SRC_DATABASE = ".data_src_database"
    _config.DATA_ARCHIVE_ROOT = ".biothings_hub/archive"
    _config.LOG_FOLDER = ".biothings_hub/logs"
    _config.DATA_PLUGIN_FOLDER = f"{working_dir}"

    sys.modules["config"] = _config
    sys.modules["biothings.config"] = _config

    from .dataplugin import app as dataplugin_app
    from .dataplugin_localhub import app as dataplugin_localhub_app

    cli.add_typer(dataplugin_app, name="dataplugin")
    cli.add_typer(dataplugin_localhub_app, name="dataplugin-localhub")
    return cli()
