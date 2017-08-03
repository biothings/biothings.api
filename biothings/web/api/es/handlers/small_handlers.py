from tornado.web import HTTPError
from biothings.web.api.es.handlers.base_handler import BaseESRequestHandler

class StatusHandler(BaseESRequestHandler):
    ''' Handles requests to check the status of the server. '''

    def head(self):
        try:
            r = self.web_settings.es_client.get(**self.web_settings.STATUS_CHECK)
        except:
            raise HTTPError(503)

        if not r:
            raise HTTPError(503)

    def get(self):
        self.head()
        self.write('OK')
