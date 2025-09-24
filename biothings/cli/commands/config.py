"""
Module for creating the CLI application for the configuration interface

Provides the following capabilities:

- View the default configuration file
- Generate a local configuration file
- Delete a local configuration file
- Modify the default configuration file

By default, a config.py module isn't required to the biothings-cli locally.
A default config module is setup at launch, however an additional config module
can be provided to override the default config settings.

The available config settings can be found at biothings.hub.default_config module (note that
not all settings are relevant to the CLI)

*** HUB MODE ***
config.py
.biothings_hub
  .data_src_database
  archive
  biothings_hubdb
data_plugin0
  ...
data_plugin1
  ...
data_plugin2
  ...

*** SINGULAR MODE ***
config.py
.biothings_hub
  .data_src_database
  archive
  biothings_hubdb
manifest.json
parser.py

*** Example Configuration ***
########################################
# DATA PLUGIN CONFIGURATION VARIABLES #
########################################
DATA_SRC_DATABASE = '.data_src_database'
DATA_HUB_DB_DATABASE = 'data_hub_db_database'
HUB_DB_BACKEND = {
    "module": "biothings.utils.sqlite3",
    "sqlite_db_folder": ".biothings_hub""
}
DATA_ARCHIVE_ROOT = ".biothings_hub/archive"

# Add new entry in DOCKER_CONFIG if you want to use a different docker host for your
# docker-based data plugin, other than the default docker host running on your localhost.
DOCKER_CONFIG = {
    "docker1": {"tls_cert_path": None, "tls_key_path": None, "client_url": ""},
    "localhost": {"client_url": "unix://var/run/docker.sock"},
}
"""
import enum
import importlib
import json
import logging
import os
import pathlib
import sys
import types
from typing import Union

from rich import box
from rich.console import Console
from rich.panel import Panel
import typer
from typing_extensions import Annotated

from biothings.utils.common import DummyConfig
from biothings.utils.configuration import ConfigurationError



SHORT_HELP = "[green]CLI tool for handling the biothings configuration.[/green]"
FULL_HELP = (
    f"{SHORT_HELP}"
    "\n"
    "\n[green] * View the default configuration file [/green]"
    "\n[green] * Generate a local configuration file [/green]"
    "\n[green] * Modify the backend storage configuration [/green]"
    "\n"
    "\nBy default, a config.py module isn't required to the biothings-cli locally."
    "\nA default config module is setup at launch, however an additional config module "
    "\ncan be provided to override the default config settings."
    "\n"
    "\nThe available config settings can be found at biothings.hub.default_config module "
    "\n(note that not all settings are relevant to the CLI)"
)

config_application = typer.Typer(
    help=FULL_HELP,
    short_help=SHORT_HELP,
    no_args_is_help=True,
    rich_markup_mode="rich",
)

logger = logging.getLogger(name="biothings-cli")


@config_application.command(name="display")
def display_default_configuration():
    """
    Displays the default configuration stored for the biothings-cli
    """
    default_configuration = default_biothings_configuration()
    console = Console()
    panel = Panel(
        f"{build_configuration_repr(default_configuration)}\n",
        title="[white]Default Biothings Configuration[/white]",
        title_align="left",
        box=box.ROUNDED,
    )
    console.print(panel)


@config_application.command(name="create")
def create_local_configuration(
    db_backend: Annotated[
        bool,
        typer.Option("--override-backend", help="If provided, will prompt for overriding the HUB_DB_BACKEND value"),
    ] = False,
    index_backend: Annotated[
        bool,
        typer.Option("--override-index", help="If provided, will prompt for overriding the INDEX_CONFIG value"),
    ] = False,
):
    """
    Creates a local configuration file (named config.py) in the current working directory
    """
    configuration = default_biothings_configuration()

    class BackendType(str, enum.Enum):
        SQLITE3 = "sqlite3"
        MONGODB = "mongodb"

    if db_backend:
        db_type = typer.prompt(
            "What backend would you like to use? (supported options \"sqlite3\"|\"mongodb\"",
            type=BackendType

        )
        if db_type == "sqlite3":
            backend = {
                "module": "biothings.utils.sqlite3",
                "sqlite_db_folder": ".biothings_hub",
            }
            logger.info("Setting HUB_DB_BACKEND:\n%s", json.dumps(backend, indent=2))
        elif db_type == "mongodb":
            backend = {
                "module" : "biothings.utils.mongo",
                "uri" : "mongodb://localhost:27017",
            }
            custom_uri = typer.prompt(
                "Please specify the server uri for mongodb", default="mongodb://localhost:27017"
            )
            if custom_uri is not None:
                backend["uri"] = custom_uri
            logger.info("Setting HUB_DB_BACKEND:\n%s", json.dumps(backend, indent=2))
        configuration["HUB_DB_BACKEND"] = backend

    if index_backend:
        host_address = typer.prompt(
            "Please specify the host address for elasticsearch you would like to use", default="http://localhost:9200"
        )
        backend = {
            "indexer_select": {},
            "env": {
                "commandhub": {
                    "host": host_address,
                    "indexer": {
                        "args": {
                            "request_timeout": 300,
                            "retry_on_timeout": True,
                            "max_retries": 10
                        }
                    }
                }
            }
        }
        logger.info("Setting INDEX_CONFIG:\n%s", json.dumps(backend, indent=2))
        configuration["INDEX_CONFIG"] = backend

    with open("config.py", "w", encoding="utf-8") as handle:
        configuration_repr = build_configuration_repr(configuration)
        handle.write(configuration_repr)

    console = Console()
    panel = Panel(
        f"{build_configuration_repr(configuration)}\n",
        title="[white]Local Biothings Configuration[/white]",
        title_align="left",
        box=box.ROUNDED,
    )
    console.print(panel)



def build_configuration_repr(configuration_values: dict) -> str:
    """
    Generates a string representation of the configuration
    """
    header_string = (
        "########################################\n"
        "# DATA PLUGIN CONFIGURATION VARIABLES  #\n"
        "########################################"
    )
    configuration_repr = [header_string]
    for configuration_key, configuration_value in configuration_values.items():
        if isinstance(configuration_value, dict):
            mapping_repr = f"{configuration_key} = {json.dumps(configuration_value, indent=2)}"
            mapping_repr = mapping_repr.replace("true", "True")
            mapping_repr = mapping_repr.replace("false", "False")
            configuration_repr.append(mapping_repr)
        elif isinstance(configuration_value, (pathlib.Path, str)):
            configuration_repr.append(
                f"{configuration_key} = \"{configuration_value}\""
            )
        else:
            configuration_repr.append(
                f"{configuration_key} = {configuration_value}"
            )
    return "\n".join(configuration_repr).rstrip("\n")


def default_biothings_configuration() -> dict:
    """
    Function call to build the default biothings configuration

    Stores all the default values for the biothings configuration
    for reference and updating
    """

    configuration = {
        "HUB_DB_BACKEND": {
            "module": "biothings.utils.sqlite3",
            "sqlite_db_folder": ".biothings_hub",
        },
        "DATA_SRC_SERVER": "localhost",
        "DATA_SRC_DATABASE": "data_src_database",
        "DATA_ARCHIVE_ROOT": ".biothings_hub/archive",
        "LOG_FOLDER": ".biothings_hub/logs",
        "DATA_PLUGIN_FOLDER": pathlib.Path().cwd(),
        "DATA_TARGET_SERVER": "localhost",
        "DATA_TARGET_PORT": 27017,
        "DATA_TARGET_DATABASE": "plugin-hub",
        "INDEX_CONFIG": {
            "indexer_select": {},
            "env": {
                "commandhub": {
                    "host": "http://localhost:9200",
                    "indexer": {
                        "args": {
                            "request_timeout": 300,
                            "retry_on_timeout": True,
                            "max_retries": 10
                        }
                    }
                }
            }
        },
        "RUN_DIR": pathlib.Path().cwd(),
        "HUB_MAX_WORKERS": os.cpu_count(),
        "MAX_QUEUED_JOBS": 1000
    }
    return configuration



def load_local_configuration() -> types.ModuleType:
    """
    Attempts to load a local configuration file first before
    falling back to a default configuration
    """
    current_directory = pathlib.Path.cwd()
    config_module_file = current_directory.joinpath("config.py")
    if config_module_file.exists():
        spec = importlib.util.spec_from_file_location("config", location=str(config_module_file))
        config_module = importlib.util.module_from_spec(spec)
        sys.modules["config"] = config_module
        sys.modules["biothings.config"] = config_module

        try:
            backend = getattr(config_module, "HUB_DB_BACKEND")
            setattr(config_module, "hub_db", importlib.import_module(backend["module"]))
        except ImportError as import_err:
            logging.exception(import_err)
            raise import_err

        spec.loader.exec_module(config_module)

        for attr in dir(config_module):
            value = getattr(config_module, attr)
            if isinstance(value, ConfigurationError):
                raise ConfigurationError(f"{attr}: {value}")

        return config_module
    return None


def load_default_configuration():
    """
    Loads the default configuration into a DummyConfig
    """
    configuration_instance = DummyConfig("config")
    default_configuration_values = default_biothings_configuration()
    for configuration_key, configuration_value in default_configuration_values.items():
        setattr(configuration_instance, configuration_key, configuration_value)

    sys.modules["config"] = configuration_instance
    sys.modules["biothings.config"] = configuration_instance

    return configuration_instance

def load_configuration() -> Union[types.ModuleType, DummyConfig]:
    """
    Setup a config module necessary to launch the biothings-cli.

    Attempts to load a local file named config.py in the current working directory,
    otherwise loads a default configuration through a DummyConfig instance

    Depending on the backend hub database, the order of configuration
    matters. If we attempt to load a module that checks for the configuration
    we'll have to ensure that the configuration is properly configured prior
    to loading the module
    """
    configuration = load_local_configuration()
    if configuration is None:
        logging.debug("Unable to find `config` module. Using the default configuration")
        configuration = load_default_configuration()
    return configuration
