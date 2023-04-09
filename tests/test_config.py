########################################
# APP-SPECIFIC CONFIGURATION VARIABLES #
########################################
# The following variables should or must be defined in your
# own application. Create a config.py file, import that config_common
# file as:
#
#   from config_hub import *
#
# then define the following variables to fit your needs. You can also override any
# any other variables in this file as required. Variables defined as ValueError() exceptions
# *must* be defined
#

import os

from biothings.utils.loggers import setup_default_log

# * 7. Hub Internals *#
# Define hostname for source database
DATA_SRC_SERVER = "localhost"
# Define port for source database
DATA_SRC_PORT = 27017
# Define name for source database
DATA_SRC_DATABASE = "testhub_src"
# Define username for source database connection (or None if not needed)
DATA_SRC_SERVER_USERNAME = ""
# Define password for source database connection (or None if not needed)
DATA_SRC_SERVER_PASSWORD = ""

# Target (merged collection) database connection
# Define hostname for target database (merged collections)
DATA_TARGET_SERVER = "localhost"
# Define port for target database (merged collections)
DATA_TARGET_PORT = 27017
# Define name for target database (merged collections)
DATA_TARGET_DATABASE = "testhub"
# Define username for target database connection (or None if not needed)
DATA_TARGET_SERVER_USERNAME = ""
# Define password for target database connection (or None if not needed)
DATA_TARGET_SERVER_PASSWORD = ""

# Define Hub DB connection
# Internal backend. Default to mongodb
# For now, other options are: mongodb, sqlite3, elasticsearch
HUB_DB_BACKEND = {
    "module": "biothings.utils.es",
    "host": "localhost",
}

# Hub environment (like, prod, dev, ...)
# Used to generate remote metadata file, like "latest.json", "versions.json"
# If non-empty, this constant will be used to generate those url, as a prefix
# with "-" between. So, if "dev", we'll have "dev-latest.json", etc...
# "" means production
HUB_ENV = ""

# * 2. Datasources *#
# List of package paths for active datasources
ACTIVE_DATASOURCES = []

# * 3. Folders *#
# Path to a folder to store all downloaded files, logs, caches, etc...
DATA_ARCHIVE_ROOT = "/tmp/testhub/datasources"

# cached data (it None, caches won't be used at all)
CACHE_FOLDER = None

# Path to a folder to store all 3rd party parsers, dumpers, etc...
DATA_PLUGIN_FOLDER = "/tmp/testhub/plugins"

# Path to folder containing diff files
# Usually inside DATA_ARCHIVE_ROOT
DIFF_PATH = os.path.join(DATA_ARCHIVE_ROOT, "diff")

# Path to folder containing release note files
# Usually inside DATA_ARCHIVE_ROOT
RELEASE_PATH = os.path.join(DATA_ARCHIVE_ROOT, "release")

# Define path to folder which will contain log files
# Usually inside DATA_ARCHIVE_ROOT
LOG_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, "logs")

# Provide a default hub logger instance (use setup_default_log(name,log_folder)
logger = setup_default_log("hub", LOG_FOLDER)

ES_HOST = "localhost:9200"  # optional
ES_INDICES = {"dev": "main_build_configuration"}
ANNOTATION_DEFAULT_SCOPES = ["_id", "symbol"]
