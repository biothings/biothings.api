import logging
import urllib.parse
import json
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

class AsyncHipchatLoggingHandler(logging.Handler):
    def __init__(self, hipchat_room, hipchat_auth_token, hipchat_msg_from=None, hipchat_msg_color='yellow', 
                hipchat_base_url='https://sulab.hipchat.com', hipchat_api_version='v2', 
                hipchat_message_format='text', *args, **kwargs):
        super(AsyncHipchatLoggingHandler, self).__init__(*args, **kwargs)
        self.hipchat_base_url = hipchat_base_url
        self.hipchat_api_version = hipchat_api_version
        self.hipchat_room = hipchat_room
        self.auth_token = hipchat_auth_token
        self.msg_from = hipchat_msg_from
        self.msg_color = hipchat_msg_color
        self.msg_format = hipchat_message_format

    @property
    def _url(self):
        return '/'.join([self.hipchat_base_url, self.hipchat_api_version, 'room', self.hipchat_room, 'notification'])

    @property
    def _headers(self):
        return {'Content-type': 'application/json', 'Authorization': 'Bearer {}'.format(self.auth_token)}

    def _request_body(self, record):
        _ret = {'color': self.msg_color, 'message_format': self.msg_format, 'message': self.format(record)}
        if self.msg_from:
            _ret.update({'from': self.msg_from})
        return json.dumps(_ret)

    def _hipchat_response_handler(self, response):
        pass

    def emit(self, record):
        _request = HTTPRequest(url=self._url, method='POST', headers=self._headers, body=self._request_body(record))
        _client = AsyncHTTPClient()
        _client.fetch(_request, callback=self._hipchat_response_handler)

def get_hipchat_logger(hipchat_room, hipchat_auth_token, hipchat_msg_from=None, hipchat_msg_color='yellow', 
                       hipchat_base_url='https://sulab.hipchat.com', hipchat_api_version='v2', 
                       hipchat_message_format='text', hipchat_log_format=None, *args, **kwargs):
    ''' hipchat_room is the room id, hipchat_auth_token is a notification level auth token for that room.
        returns a logger that will asynchronously log to hipchat (requires tornado and/or other
        asynchronous event loop). '''
    _logger = logging.getLogger('hipchat')
    _handler = AsyncHipchatLoggingHandler(hipchat_room=hipchat_room, hipchat_auth_token=hipchat_auth_token, 
        hipchat_msg_from=hipchat_msg_from, hipchat_msg_color=hipchat_msg_color, hipchat_base_url=hipchat_base_url, 
        hipchat_api_version=hipchat_api_version, hipchat_message_format=hipchat_message_format, *args, **kwargs)
    if hipchat_log_format:
        _formatter = logging.Formatter(hipchat_log_format)
        _handler.setFormatter(hipchat_log_format)
    _logger.addHandler(_handler)
    return _logger
