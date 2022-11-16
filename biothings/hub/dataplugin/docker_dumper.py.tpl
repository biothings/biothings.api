import os
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
import biothings, config
biothings.config_for_app(config)
from config import DATA_ARCHIVE_ROOT

from biothings.utils.common import uncompressall

# Disable InsecureRequestWarning: Unverified HTTPS request is being made to host
disable_warnings(InsecureRequestWarning)

import biothings.hub.dataload.dumper

class $DUMPER_NAME($BASE_CLASSES):

    SRC_NAME = "$SRC_NAME"
    SRC_ROOT_FOLDER = os.path.join(DATA_ARCHIVE_ROOT, SRC_NAME)
    SCHEDULE = $SCHEDULE
    UNCOMPRESS = $UNCOMPRESS
    TLS_CERT_PATH = "$TLS_CERT_PATH"
    TLS_KEY_PATH = "$TLS_KEY_PATH"
    SRC_URLS = $SRC_URLS
    __metadata__ = {"src_meta" : $__metadata__}

    def post_dump(self, *args, **kwargs):
        if self.__class__.UNCOMPRESS:
            self.logger.info("Uncompress all archive files in '%s'" % self.new_data_folder)
            uncompressall(self.new_data_folder)

    $SET_RELEASE_FUNC
