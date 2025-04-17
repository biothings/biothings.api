import os

import biothings, config
biothings.config_for_app(config)
from config import DATA_ARCHIVE_ROOT

from biothings.utils.common import uncompressall


import biothings.hub.dataload.dumper

class $DUMPER_NAME($BASE_CLASSES):

    SRC_NAME = "$SRC_NAME"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, "$SRC_FOLDER_NAME")
    SCHEDULE = $SCHEDULE
    UNCOMPRESS = $UNCOMPRESS
    SRC_URLS = $SRC_URLS
    DISABLED = $DISABLED
    __metadata__ = {"src_meta" : $__metadata__}

    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" % self.new_data_folder)
            uncompressall(self.new_data_folder)
        super().post_dump(*args, **kwargs)

    $SET_RELEASE_FUNC
