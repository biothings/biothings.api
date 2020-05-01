import os

import biothings, config
biothings.config_for_app(config)

import biothings.hub.dataload.uploader

# when code is exported, import becomes relative
try:
    from $SRC_NAME.$PARSER_MOD import $PARSER_FUNC as parser_func
except ImportError:
    from .$PARSER_MOD import $PARSER_FUNC as parser_func

$IMPORT_IDCONVERTER_FUNC

class $UPLOADER_NAME($BASE_CLASSES):

    name = "$SRC_NAME"
    __metadata__ = {"src_meta" : $__metadata__}
    idconverter = $IDCONVERTER_FUNC
    storage_class = $STORAGE_CLASS

    def load_data(self,data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return $CALL_PARSER_FUNC

    $JOBS_FUNC

    $MAPPING_FUNC
