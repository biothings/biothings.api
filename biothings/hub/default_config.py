from biothings.utils.configuration import ConfigurationDefault, ConfigurationValue

# ######### #
# HUB VARS  #
# ######### #
import os

#* 7. Hub Internals *#

DATA_HUB_DB_DATABASE = ConfigurationDefault(
        default=ConfigurationValue("""'gene_hubdb'"""),
        desc="db containing the following (internal use)")
DATA_SRC_MASTER_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'src_master'"""),
        desc="for metadata of each src collections")
DATA_SRC_DUMP_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'src_dump'"""),
        desc="for src data download information")
DATA_SRC_BUILD_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'src_build'"""),
        desc="for src data build information")
DATA_PLUGIN_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'data_plugin'"""),
        desc="for data plugins information")
API_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'api'"""),
        desc="for api information (running under hub control)")
CMD_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'cmd'"""),
        desc="for launched/running commands in shell")
EVENT_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'event'"""),
        desc="for launched/running commands in shell")
HUB_CONFIG_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'hub_config'"""),
        desc="for values overrifing config files")
DATA_TARGET_MASTER_COLLECTION = ConfigurationDefault(
        default=ConfigurationValue("""'db_master'"""),
        desc="")


#* 6. Job Manager *#
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

#* 1. General *#
# Hub name/icon url/version, for display purpose
HUB_NAME = "Biothings Hub"
HUB_ICON = None
#STANDALONE_VERSION = "standalone_v2"
# SSH port for hub console
#- readonly -#
HUB_SSH_PORT = 7022
# API port
#- readonly -#
HUB_API_PORT = 7080

# The format is a dictionary of 'username': 'cryptedpassword'
# Generate crypted passwords with 'openssl passwd -crypt'
#- hide -#
#- readonly -#
HUB_PASSWD = {"guest":"9RKfd8gDuNf0Q"}

# Webhook to publish notifications to a Slack channel
SLACK_WEBHOOK = None

# When code changes in plugins or "manual" datasources, Hub automatically restarts
# to reflect those changes
USE_RELOADER = True

#* 4. Index & Diff *#
# Pre-prod/test ES definitions
INDEX_CONFIG = {
        "indexer_select": {
            # default
            },
        "env" : {
            "localhub" : {
                "host" : "localhost:9200",
                "indexer" : {
                    "args" : {
                        "timeout" : 300,
                        "retry_on_timeout" : True,
                        "max_retries" : 10,
                        },
                    },
                },
            },
        }

# Snapshot environment configuration
SNAPSHOT_CONFIG = {}

# reporting diff results, number of IDs to consider (to avoid too much mem usage)
MAX_REPORTED_IDS = 1000
# for diff updates, number of IDs randomly picked as examples when rendering the report
MAX_RANDOMLY_PICKED = 10
# size of a diff file when in memory (used when merged/reduced)                                                                                                                                      
MAX_DIFF_SIZE = 50 * 1024**2  # 50MiB (~1MiB on disk when compressed)


#* 5. Release *#
# Release configuration
# Each root keys define a release environment (test, prod, ...)
RELEASE_CONFIG = {
        "env" : {
            "tests3" : {
                "cloud" : {
                    "type" : "aws", # default, only one supported by now
                    "access_key" : None,
                    "secret_key" : None,
                    },
                "release" : {
                    "bucket" : None,
                    "region" : None,
                    "folder" : "%(build_config.name)s",
                    "auto" : True, # automatically generate release-note ?
                    },
                "diff" : {
                    "bucket" : None,
                    "region" : None,
                    "folder" : "%(build_config.name)s",
                    "auto" : True, # automatically generate diff ? Careful if lots of changes
                    },
                "publish" : {
                    "pre" : {
                        "snapshot" :
                        [
                            {
                                "action" : "archive",
                                "format" : "tar.xz",
                                "name" : "%(build_config.name)s_snapshot_%(_meta.build_version)s.tar.xz",
                                "es_backups_folder" : ConfigurationValue("""ES_BACKUPS_FOLDER"""),
                                },
                            {
                                "action" : "upload",
                                "type" : "s3",
                                "bucket" : ConfigurationValue("""S3 SNAPSHOT BUCKET"""),
                                "region" : ConfigurationValue("""S3 REGION"""),
                                "base_path" : "%(build_config.name)s/$(Y)",
                                "file" : "%(build_config.name)s_snapshot_%(_meta.build_version)s.tar.xz",
                                "acl" : "private",
                                "es_backups_folder" : ConfigurationValue("""ES_BACKUPS_FOLDER"""),
                                "overwrite" : True
                                }
                            ],
                        "diff" : [],
                        }                    
                    },
                }
            }
        }

# Standalone configuration, relates to how the Hub
# install data releases. You can specify, per data release name,
# which ES host/index to address, or use the default for all data
# releases.
# Note: if data release name doesn't any key here, a _default will be
# used (it must then exist, key = "_default")
STANDALONE_CONFIG = { 
    # default config
    "_default": {
        "es_host" : "localhost:9200",
        "index" : "biothings_current",
    },  
    ## custom definition
    #"release_name" : { 
    #    "es_host" : "anotherhost:9200",
    #    "index" : "specical_index_name",
    #    }   
}   

# Default targeted standalone version
# (once published, data is fetched and deployed by what's called 
# a standalone instance). Modify thorougly (ie. don't modify it)
STANDALONE_VERSION = {"branch" : "standalone_v2", "commit": None, "date" : None}

# Don't check used versions, just propagate them when publishing.
# That is, Hub won't ensure that all version information is
# properly set)
SKIP_CHECK_VERSIONS = False

# Root folder containing ElasticSearch backups, created
# by snapshots with repo type "fs". This setting must match
# elasticsearch.yml value, param "path.repo"
# If using "fs" type repository with post-step "archive",
# this folder must have permissions set for user/group running the hub
ES_BACKUPS_FOLDER = "/data/es_backups"


# don't bother with elements order in a list when diffing,
# mygene optmized uploaders can't produce different results
# when parsing data (parallelization)
import importlib
import biothings.utils.jsondiff
importlib.reload(biothings.utils.jsondiff)
biothings.utils.jsondiff.UNORDERED_LIST = True

RUN_DIR = './run'
