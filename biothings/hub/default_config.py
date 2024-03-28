"""
###########################################
# THE DEFAULT HUB CONFIGURATION VARIABLES #
###########################################

This file is used to define all Hub features’ settings with their metadata, documentation, and default value.

In your own application, you should create a config.py file by copying from the config.py.example,
then define/override the variables to fit your needs.

The documentation for a variable can be defined by one of these case:
    1. the configuration value is a ConfigurationDefault instance (specify a default value)
        or a ConfigurationError instance, in which case the documentation is taken from the instance doc.
    2. the documentation can be specified as an inline comment
    3. the documentation can be specified as comments above

There are several special comment formats used to organize and manager params:
    - all comments above a param are considered as documentation/description for the parameter,
      until a empty line or a non-comment line is found.
    - to be considered as documentation, comments also need to have a space after "#". So:
            # my comment
      will be kepts as documentation, but:
            #my comment
      will just be ignored (so python code can be commented that way, without then being part
      of the documentation)
    - A section title can be added to organize parameters. A section title looks like this:
            #* my section title *#
      It can be added for each parameters, or the first section found above is the section
      the parameter will be associated to. An empty section title can be used to reset the
      section title and associate the current parameter to default one:
            #* *#
      If no section is found, all parameters are part of the default one (None).
    - some parameters needs to be kept secret (like passwords for instance):
            #- invisible -#
      will hide the parameter, including the name, value, description, from the configuration
            #- hidden -#
      will keep the parameter in the configuration displayed to users, but its value will be omitted
            #- readonly -#
      will allow the parameter to shown, but not editable

      Note: special comments can stacked:
            #- readonly -#
            #- hidden -#
      will make the parameter read-only, and its value won't be displayed

For example:

- Defined variable using ConfigurationDefault
```
DATA_HUB_DB_DATABASE = ConfigurationDefault(
    default='biothings_hubdb',
    desc="db containing the following (internal use)"
)

or use a python statement:

DIFF_PATH = ConfigurationDefault(
        default=ConfigurationValue('set_default_folder(DATA_ARCHIVE_ROOT,"diff")'),
        desc="Define path to folder which will contain output files from diff")
```

- Defined variable using ConfigurationError
```
DATA_SRC_SERVER = ConfigurationError("Define hostname for source database")
```

- Defined variable with documentation as a comment line, and metadata
```
# API port
#- hide -#
#- readonly -#
HUB_API_PORT = 7080
```

"""

import importlib

# logging is needed for evaluating default logger value below
import logging  # pylint: disable=unused-import      # noqa
import os

import biothings.utils.jsondiff

# set_default_folder is needed for evaluating some default values below
from biothings.utils.configuration import set_default_folder  # pylint: disable=unused-import      # noqa
from biothings.utils.configuration import ConfigurationDefault, ConfigurationError, ConfigurationValue

# * 1. General *#
# Hub name/icon url/version, for display purpose
HUB_NAME = "Biothings Hub"
HUB_ICON = None

# SSH port for hub console
# - readonly -#
HUB_SSH_PORT = 7022
# API port
# - readonly -#
HUB_API_PORT = 7080
# Readonly API port
# - readonly -#
READONLY_HUB_API_PORT = ConfigurationDefault(
    default=ConfigurationValue("HUB_API_PORT+1"), desc='Readonly API port when "readonly" feature is enabled'
)

# The format is a dictionary of 'username': 'cryptedpassword'
# Generate crypted passwords with 'openssl passwd -crypt'
# - hide -#
# - readonly -#
HUB_PASSWD = {"guest": "9RKfd8gDuNf0Q"}

# Webhook to publish notifications to a Slack channel
SLACK_WEBHOOK = None

# When code changes in plugins or "manual" datasources, Hub automatically restarts
# to reflect those changes
USE_RELOADER = True

# * 2. Datasources *#
ACTIVE_DATASOURCES = ConfigurationDefault(default=[], desc="List of package paths for active datasources")

# Skip all scheduled dumper jobs
SKIP_DUMPER_SCHEDULE = False

# Skip all scheduled uploader jobs after a success dump
SKIP_UPLOADER_POLL = False

# Auto archive feature will use this configuration to get schedule config for corresponding build configuration.
# If not set it will use the default value defined in AutoArchiveManager
AUTO_ARCHIVE_CONFIG = None
"""
AUTO_ARCHIVE_CONFIG = {
    "build_configuration_name": {
        "schedule: "* 0 * * *",  # run daily at 0am UTC
        "days": 3
    },
    ...
}
"""

# Docker connection configuration
# client_url should match the following formats:
# ssh://ubuntu@remote_ip:port
# unix://var/run/docker.sock
# http://remote_ip:port
# https://remote_ip:port
DOCKER_CONFIG = {
    "docker1": {"tls_cert_path": None, "tls_key_path": None, "client_url": ""},
    "localhost": {"client_url": "unix://var/run/docker.sock"},
}

# Optional, can used to override the docker connection name from the src_url
DOCKER_HOST = None

# * 3. Folders *#
# Path to a folder to store all downloaded files, logs, caches, etc...
DATA_ARCHIVE_ROOT = ConfigurationError(
    "Define path to folder which will contain all downloaded data, cache files, etc..."
)

# where to store info about processes launched by the hub
RUN_DIR = ConfigurationDefault(
    default=ConfigurationValue("""set_default_folder(DATA_ARCHIVE_ROOT,"run")"""),
    desc="Define path to folder which will store info about processes launched by the hub",
)

# cached data (it None, caches won't be used at all)
CACHE_FOLDER = ConfigurationDefault(
    default=ConfigurationValue("""set_default_folder(DATA_ARCHIVE_ROOT,"cache")"""),
    desc="Define path to folder which will contain cache files, set to None to disable",
)

# Path to a folder to store all 3rd party parsers, dumpers, etc...
DATA_PLUGIN_FOLDER = ConfigurationDefault(
    default="./plugins", desc="Define path to folder which will contain all 3rd party parsers, dumpers, etc..."
)

# Path to folder containing diff files
# Usually inside DATA_ARCHIVE_ROOT
DIFF_PATH = ConfigurationDefault(
    default=ConfigurationValue("""set_default_folder(DATA_ARCHIVE_ROOT,"diff")"""),
    desc="Define path to folder which will contain output files from diff",
)

# Path to folder containing release note files
# Usually inside DATA_ARCHIVE_ROOT
RELEASE_PATH = ConfigurationDefault(
    default=ConfigurationValue("""set_default_folder(DATA_ARCHIVE_ROOT,"release")"""),
    desc="Define path to folder which will contain release files",
)


# Define a folder to store uploaded files via biothings.hub.api.handlder.UploadHandler
# It's enabled with "upload" feature for HubServer
DATA_UPLOAD_FOLDER = ConfigurationDefault(
    default=ConfigurationValue("""set_default_folder(DATA_ARCHIVE_ROOT,"dataupload")"""),
    desc="Define path to folder which will store uploaded files via upload handler",
)


# Root folder containing ElasticSearch backups, created
# by snapshots with repo type "fs". This setting must match
# elasticsearch.yml value, param "path.repo"
# If using "fs" type repository with post-step "archive",
# this folder must have permissions set for user/group running the hub
ES_BACKUPS_FOLDER = ConfigurationDefault(
    default=ConfigurationValue('set_default_folder(DATA_ARCHIVE_ROOT,"es_backups")'),
    desc="Define path to folder which will contain containing ElasticSearch snapshot backups",
)

# this dir must be created manually
# Usually inside DATA_ARCHIVE_ROOT
LOG_FOLDER = ConfigurationDefault(
    default=ConfigurationValue('set_default_folder(DATA_ARCHIVE_ROOT,"logs")'),
    desc="Define path to folder which will contain log files",
)

# hub logger name
LOGGER_NAME = "hub"

logger = ConfigurationDefault(
    default=ConfigurationValue("""logging.getLogger(LOGGER_NAME)"""),
    desc="Provide a default hub logger instance (use setup_default_log(name,log_folder)",
)

# * 4. Index & Diff *#
# Pre-prod/test ES definitions
INDEX_CONFIG = {
    "indexer_select": {
        # default
    },
    "env": {
        "localhub": {
            "host": "http://localhost:9200",
            "indexer": {"args": {"request_timeout": 300, "retry_on_timeout": True, "max_retries": 10}},
        }
    },
}

# Snapshot environment configuration
SNAPSHOT_CONFIG = {}

# Auto snapshot cleaner feature will use this configuration to get schedule config for corresponding environment.
AUTO_SNAPSHOT_CLEANUP_CONFIG = None
"""
AUTO_SNAPSHOT_CLEANUP_CONFIG = {
        "environment_name": {
            "schedule": "* 0 * * *",      # run daily at 0am UTC
            "keep": 3,                   # the number of most recent snapshots to keep in one group
            "group_by": "build_config",  # the attr of which its values form groups
            "extra_filters": {}          # a set of criterions to limit which snapshots are to be cleaned
        },
        ...
    }
"""

# reporting diff results, number of IDs to consider (to avoid too much mem usage)
MAX_REPORTED_IDS = 1000
# for diff updates, number of IDs randomly picked as examples when rendering the report
MAX_RANDOMLY_PICKED = 10
# size of a diff file when in memory (used when merged/reduced)
MAX_DIFF_SIZE = 50 * 1024**2  # 50MiB (~1MiB on disk when compressed)


# * 5. Release *#
# Release configuration
# Each root keys define a release environment (test, prod, ...)
RELEASE_CONFIG = {
    "env": {
        "tests3": {
            "cloud": {
                "type": "aws",  # default, only one supported by now
                "access_key": None,
                "secret_key": None,
            },
            "release": {
                "bucket": None,
                "region": None,
                "folder": "%(build_config.name)s",
                "auto": True,  # automatically generate release-note ?
            },
            "diff": {
                "bucket": None,
                "region": None,
                "folder": "%(build_config.name)s",
                "auto": True,  # automatically generate diff ? Careful if lots of changes
            },
            "publish": {
                "pre": {
                    "snapshot": [
                        {
                            "action": "archive",
                            "format": "tar.xz",
                            "name": "%(build_config.name)s_snapshot_%(_meta.build_version)s.tar.xz",
                            "es_backups_folder": ConfigurationValue("""ES_BACKUPS_FOLDER"""),
                        },
                        {
                            "action": "upload",
                            "type": "s3",
                            "bucket": ConfigurationValue("""S3_SNAPSHOT_BUCKET"""),
                            "region": ConfigurationValue("""S3_REGION"""),
                            "base_path": "%(build_config.name)s/$(Y)",
                            "file": "%(build_config.name)s_snapshot_%(_meta.build_version)s.tar.xz",
                            "acl": "private",
                            "es_backups_folder": ConfigurationValue("""ES_BACKUPS_FOLDER"""),
                            "overwrite": True,
                        },
                    ],
                    "diff": [],
                }
            },
        }
    }
}

# Specify AWS credentials to access snapshots stored in S3 bucket
STANDALONE_AWS_CREDENTIALS = {"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": ""}

# Standalone configuration, relates to how the Hub
# install data releases. You can specify, per data release name,
# which ES host/index to address, or use the default for all data
# releases.
# Note: if data release name doesn't any key here, a _default will be
# used (it must then exist, key = "_default")
# Note: ES host can be a string which stands for a single host,
# or a dict, which contains a collection of es hosts for same index, same release name
STANDALONE_CONFIG = {
    # default config
    "_default": {
        "es_host": "http://localhost:9200",
        "index": "biothings_current",
    },
    # # custom definition
    # "release_name" : {
    #     "es_host" : "anotherhost:9200",
    #     "index" : "specical_index_name",
    # }
    # # release with multiple es hosts
    # "release_name_2" : {
    #     "es_host" : {
    #         "prod": "prodserver:9200",
    #         "staging": "stagingserver:9200"
    #     },
    #     "index" : "specical_index_name",
    # }
}

# Default targeted standalone version
# (once published, data is fetched and deployed by what's called
# a standalone instance). Modify thorougly (ie. don't modify it)
STANDALONE_VERSION = {"branch": "standalone_v2", "commit": None, "date": None}

# A list of URLs to the versions.json files, which contain data release metadata
VERSION_URLS = []

# Use this configuration to customize validation logic of the auto hub feature.
# the AutoHubValidator will be use as default. Any customize class must be extended from it.
AUTOHUB_VALIDATOR_CLASS = None

# Set to True to skip checking application/biothings version matching, before installing
# a data release, in version settings like "app_version", "standalone_version", "biothings_version"
SKIP_CHECK_COMPAT = True

# This configuration used for autorelease feature,
# which specifies when and how the backend should automatically install a release
# the key is the source name,
# the value can be a string that will be a croniter's schedule: Ref: https://pypi.python.org/pypi/croniter
# or it can be a dict, which has a schedule and an extra item which will be passed to the install function.
AUTO_RELEASE_CONFIG = None
# AUTO_RELEASE_CONFIG = {
#   "source_name": "*/1 * * * *",
#   "source_name": {
#     "schedule": "*/1 * * * *",
#     "extra": {
#         "force": True,
#         "use_no_downtime_method": False,
#     }
#   },
# }


# Specific how many old indices should we keep, after restore a snapshot.
# 0: delete all old index.
# 1-10: keep recent n of old indices
# -1: do not delete old index at all
RELEASE_KEEP_N_RECENT_INDICES = 0

# Specify "batch_size" passed to BaseSyncer.sync,
# which applys diffs during an incremental data release.
# Set to a lower value if the sync job puts too much load to the server.
SYNC_BATCH_SIZE = 10000

# * 6. Job Manager *#
# How much memory hub is allowed to use:
# "auto" will let hub decides (will use 50%-60% of available RAM)
# while None won't put any limits. Number of bytes can also be
# specified
HUB_MAX_MEM_USAGE = None

# Max number of *processes* hub can access to run jobs
HUB_MAX_WORKERS = int(os.cpu_count() / 4)
# Max number of *processes* used when syncing data
# (applygin diff/incremental data)
MAX_SYNC_WORKERS = HUB_MAX_WORKERS

# Max queued jobs in job manager
# this shouldn't be 0 to make sure a job is pending and ready to be processed
# at any time (avoiding job submission preparation) but also not a huge number
# as any pending job will consume some memory).
MAX_QUEUED_JOBS = os.cpu_count() * 4

# * 7. Hub Internals *#
DATA_SRC_SERVER = ConfigurationError("Define hostname for source database")
DATA_SRC_PORT = ConfigurationDefault(default=27017, desc="Define port for source database")
DATA_SRC_DATABASE = ConfigurationError("Define name for source database")
DATA_SRC_SERVER_USERNAME = ConfigurationDefault(
    default=None, desc="Define username for source database connection (or None if not needed)"
)
DATA_SRC_SERVER_PASSWORD = ConfigurationDefault(
    default=None, desc="Define password for source database connection (or None if not needed)"
)

# Target (merged collection) database connection
DATA_TARGET_SERVER = ConfigurationError("Define hostname for target database (merged collections)")
DATA_TARGET_PORT = ConfigurationDefault(default=27017, desc="Define port for source database")
DATA_TARGET_DATABASE = ConfigurationError("Define name for target database (merged collections)")
DATA_TARGET_SERVER_USERNAME = ConfigurationDefault(
    default=None, desc="Define username for target database connection (or None if not needed)"
)
DATA_TARGET_SERVER_PASSWORD = ConfigurationDefault(
    default=None, desc="Define password for target database connection (or None if not needed)"
)

DATA_HUB_DB_DATABASE = ConfigurationDefault(
    default=ConfigurationValue("""'biothings_hubdb'"""), desc="db containing the following (internal use)"
)
DATA_SRC_MASTER_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'src_master'"""), desc="for metadata of each src collections"
)
DATA_SRC_DUMP_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'src_dump'"""), desc="for src data download information"
)
DATA_SRC_BUILD_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'src_build'"""), desc="for src data build information"
)
DATA_PLUGIN_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'data_plugin'"""), desc="for data plugins information"
)
API_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'api'"""), desc="for api information (running under hub control)"
)
CMD_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'cmd'"""), desc="for launched/running commands in shell"
)
EVENT_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'event'"""), desc="for launched/running commands in shell"
)
HUB_CONFIG_COLLECTION = ConfigurationDefault(
    default=ConfigurationValue("""'hub_config'"""), desc="for values overrifing config files"
)
DATA_TARGET_MASTER_COLLECTION = ConfigurationDefault(default=ConfigurationValue("""'db_master'"""), desc="")

# Internal backend. Default to mongodb
# For now, other options are: mongodb, sqlite3, elasticsearch
HUB_DB_BACKEND = ConfigurationError("Define Hub DB connection")
# HUB_DB_BACKEND = {
#        "module" : "biothings.utils.sqlite3",
#        "sqlite_db_foder" : "./db",
#        }
# HUB_DB_BACKEND = {
#        "module" : "biothings.utils.mongo",
#        "uri" : "mongodb://localhost:27017",
#        #"uri" : "mongodb://user:passwd@localhost:27017", # mongodb std URI
#        }
# HUB_DB_BACKEND = {
#        "module" : "biothings.utils.es",
#        "host" : "localhost:9200",
#        }

# Hub environment (like, prod, dev, ...)
# Used to generate remote metadata file, like "latest.json", "versions.json"
# If non-empty, this constant will be used to generate those url, as a prefix
# with "-" between. So, if "dev", we'll have "dev-latest.json", etc...
# "" means production
HUB_ENV = ""

# Set whether configuration parameters can be edited
# and superseded by user through ConfigurationManger
# Note: once config manager has been configured with this
# field, it's deleted to make sure we can't change it at runtime
CONFIG_READONLY = True

# Don't check used versions, just propagate them when publishing.
# That is, Hub won't ensure that all version information is
# properly set)
SKIP_CHECK_VERSIONS = False

# cache file format ("": ascii/text uncompressed, or "gz|zip|xz"
CACHE_FORMAT = "xz"

# Deprecated. Don't it's used any more
# Role, when master, hub will publish data (updates, snapshot, etc...) that
# other instances can use (production, standalones)
BIOTHINGS_ROLE = "slave"

####################################################
# Additional optional settings avaiable if needed: #
####################################################

# Pass any optional tornado settings to tornado.httpserver.HTTPServer
# see biothings.hub.api.start_api
# TORNADO_SETTINGS = {
#    # max 10GiB upload
#    "max_buffer_size" : 10*1024*1024*1024,
# }

# don't bother with elements order in a list when diffing,
# mygene optmized uploaders can't produce different results
# when parsing data (parallelization)
importlib.reload(biothings.utils.jsondiff)
biothings.utils.jsondiff.UNORDERED_LIST = True
