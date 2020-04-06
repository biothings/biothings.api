"""
    TODO load balancer check settings
"""

from elasticsearch.exceptions import ElasticsearchException
from tornado.web import HTTPError

from biothings.web.api.es.handlers.base_handler import BaseESRequestHandler


class StatusHandler(BaseESRequestHandler):
    '''
    Handles requests to check the status of the server.
    '''
    async def head(self):
        try:
            res = await self.web_settings.async_es_client.get(**self.web_settings.STATUS_CHECK)
        except ElasticsearchException:
            raise HTTPError(503)
        if not res:
            raise HTTPError(503)

    async def get(self):
        await self.head()
        self.write('OK')
