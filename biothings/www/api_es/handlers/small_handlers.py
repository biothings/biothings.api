import re
import json
from tornado.web import HTTPError
from biothings.www.helper import BaseHandler
from biothings.utils.common import split_ids
from biothings.utils.version import get_software_info
from collections import OrderedDict

class StatusHandler(BaseHandler):
    ''' Handles requests to check the status of the server. '''

    def head(self):
        r = self.esq.status_check(biothing_settings.status_check_id)
        if r is None:
            # we failed to retrieve ref/test doc, something is wrong -> service unavailable
            raise HTTPError(503)

    def get(self):
        self.head()
        self.write('OK')
