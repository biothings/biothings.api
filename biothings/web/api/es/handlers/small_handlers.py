from tornado.web import HTTPError
from biothings.web.api.es.handlers.base_handler import BaseESRequestHandler

class StatusHandler(BaseESRequestHandler):
    ''' Handles requests to check the status of the server. '''

    def head(self):
        #r = self.esq.status_check(self.web_settings.STATUS_CHECK_ID)
        #if r is None:
        #    # we failed to retrieve ref/test doc, something is wrong -> service unavailable
        #    raise HTTPError(503)
        pass

    def get(self):
        self.head()
        self.write('OK')
