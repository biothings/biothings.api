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

class ManifestDumper(type):
    """
    Metaclass for defining the class structure
    for a manifest based dumper
    """
    SRC_NAME = None
    DATA_PLUGIN_FOLDER = None
    SRC_ROOT_FOLDER = None
    SCHEDULE = None
    UNCOMPRESS = None
    SRC_URLS = None
    __metadata__ = None

    def __new__(cls, name: str, bases: list[str], attrs: list[str]):
        return super().__new__(cls, name, bases, attrs)

class AssistedDumper(metaclass=ManifestDumper):
    pass
