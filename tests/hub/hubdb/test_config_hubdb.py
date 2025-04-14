HUB_DB_BACKEND = {
    "module": "biothings.utils.es",
    "host": "http://localhost:9200",
}

# db containing the following (internal use)
DATA_HUB_DB_DATABASE = "biothings"
# for metadata of each src collections
DATA_SRC_MASTER_COLLECTION = "src_master"
# for src data download information
DATA_SRC_DUMP_COLLECTION = "src_dump"
# for src data build information
DATA_SRC_BUILD_COLLECTION = "src_build"
# for src data build configuration
DATA_SRC_BUILD_CONFIG_COLLECTION = "src_build_config"
# for data plugins information
DATA_PLUGIN_COLLECTION = "data_plugin"
# for api information (running under hub control)
API_COLLECTION = "api"
EVENT_COLLECTION = "event"
CMD_COLLECTION = "cmd"
