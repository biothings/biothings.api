''' For Standalone myvariant tracking '''
from tornado.httpclient import HTTPRequest, AsyncHTTPClient

# This is a mixin for biothing handlers, and references class variables from that class, cannot be used
# without mixing in
class StandaloneTrackingMixin:
    def self_track(self, data={}):
        no_tracking = self.get_argument('no_tracking', None)
        if not no_tracking and self.web_settings.STANDALONE_TRACKING_URL:
            req = HTTPRequest(self.web_settings.STANDALONE_TRACKING_URL, method='POST', body='category={0}&action={1}'.format(data.get('category', 'NA'), data.get('action', 'NA')))

            #now send actual async requests
            http_client = AsyncHTTPClient()
            http_client.fetch(req)
