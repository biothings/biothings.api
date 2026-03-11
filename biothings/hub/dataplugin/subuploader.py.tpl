import os

import biothings, config
biothings.config_for_app(config)

import biothings.hub.dataload.uploader


$PARSER_FACTORY_CODE

$IMPORT_IDCONVERTER_FUNC

class $UPLOADER_NAME($BASE_CLASSES):

    main_source = "$SRC_NAME"
    name = "$SUB_SRC_NAME"
    __metadata__ = {"src_meta" : $__metadata__}
    idconverter = $IDCONVERTER_FUNC
    storage_class = $STORAGE_CLASS

    def load_data(self, data_path):
        self.logger.info("Load data from directory or file: '%s'" % data_path)
        return $CALL_PARSER_FUNC

    $JOBS_FUNC

    $MAPPING_FUNC
