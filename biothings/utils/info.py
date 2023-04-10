import json
import logging
import os
import sys

from biothings.utils.version import get_software_info

logger = logging.getLogger(__name__)


class FieldNote:
    def __init__(self, path):
        try:  # populate field notes if exist
            inf = open(path, "r")
            self._fields_notes = json.load(inf)
            inf.close()
        except Exception:
            self._fields_notes = {}

    def get_field_notes(self):
        """
        Return the cached field notes associated with this instance.
        """
        return self._fields_notes


class DevInfo:
    def __init__(self):
        # the path can be a sub-folder of a git repository
        # as long as it responds to "git" command line call

        if sys.argv[0]:  # running as a script
            self._repo = os.path.dirname(sys.argv[0])
        else:  # REPL
            self._repo = os.getcwd()

    def get(self):
        # {
        #     "python-package-info": [
        #         "aiocron                 1.6",
        #         "aiohttp                 3.6.2",
        #         "async-timeout           3.0.1",
        #         ...
        #     ],
        #     "codebase": {
        #         "repository-url": "https://github.com/biothings/biothings.api.git",
        #         "commit-hash": "8715a8ebcf49bc436bca24b1ce1a35f1777a7c4c"
        #     },
        #     "biothings": {
        #         "repository-url": "https://github.com/biothings/biothings.api.git",
        #         "commit-hash": "f3ca99a1f5261a70eaef196bbab1163302c16859",
        #         "master-commits": "2478",
        #         "version": "0.10.0"
        #     },
        #     "python-info": {
        #         "version": "3.9.5 (tags/v3.9.5:0a7dcbd, May  3 2021, 17:27:52)",
        #         "version_info": {
        #             "major": 3,
        #             "minor": 9,
        #             "micro": 5
        #         }
        #     }
        # }

        try:
            return get_software_info(self._repo)
        except Exception:
            return {}
