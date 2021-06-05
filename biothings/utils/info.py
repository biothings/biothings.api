
import json
import os
import logging
from biothings.utils.version import get_software_info

logger = logging.getLogger(__name__)

class FieldNote:

    def __init__(self, path):

        try:  # populate field notes if exist
            inf = open(path, 'r')
            self._fields_notes = json.load(inf)
            inf.close()
        except Exception:
            self._fields_notes = {}

    def get_field_notes(self):
        '''
        Return the cached field notes associated with this instance.
        '''
        return self._fields_notes

class DevInfo:

    def __init__(self, path):

        # for metadata dev details
        if os.path.isdir(os.path.join(path, '.git')):
            self._git_repo_path = path
        else:
            self._git_repo_path = None

    def get(self):

        if self._git_repo_path:
            return get_software_info(self._git_repo_path)

        return {}
