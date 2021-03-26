''' For Google Analytics tracking in web '''
from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from ipaddress import ip_address, IPv6Address, IPv4Address
import hashlib

from urllib.parse import quote_plus as _q
import time
import random
import uuid
from collections import OrderedDict

from typing import Union, Optional


class ExpiringDict:
    # assuming that I do not need to worry about threads
    # loosely based on https://github.com/mailgun/expiringdict
    # cleanup on contain/get, does not clean on set

    def __init__(self, max_size: int = 1000, ttl: int = 3600):
        """
        Initialize a self expiring Dictionary

        Configure a maximum allowed size and a time to live on items.
        When an item is accessed, the TTL is reset. Removes exired items
        on access (but not on set)
        Args:
            max_size: Maximum number of items that can be stored
            ttl: time to live for items, in seconds
        """
        self.od = OrderedDict()  # {key: (v, t)} eldest in the front
        self.max_size = max_size
        self.ttl = ttl

    def _cleanup(self, stop_key):
        while True:
            k, _ = self.od.popitem(last=False)
            if k == stop_key:
                return

    def __contains__(self, key):
        try:
            item = self.od[key]
            if time.time() - item[1] < self.ttl:
                return True
            else:
                self._cleanup(key)
        except KeyError:
            pass
        return False

    def __getitem__(self, key):
        item = self.od[key]
        if time.time() - item[1] < self.ttl:
            item[1] = time.time()
            self.od.move_to_end(key, last=True)
            return item[0]
        else:
            self._cleanup(key)
            raise KeyError

    def __setitem__(self, key, value):
        if len(self.od) == self.max_size:
            if key in self:
                pass
            else:
                try:
                    self.od.popitem()
                except KeyError:
                    pass
        if self.max_size > 0:
            self.od[key] = [value, time.time()]
        else:
            pass  # just fail silently

    def __len__(self):
        return len(self.od)


exp_dict_uid = ExpiringDict(max_size=1000, ttl=3600)


def generate_unique_id(remote_ip: str, user_agent: str,
                       tm: Optional[int] = None):
    """Generates a unique user ID

    Using the remote IP and client user agent, to produce a somewhat
    unique identifier for users. A UUID version 4 conforming to RFC 4122
    is produced which is generally acceptable. It is not entirely random,
    but looks random enough to the outside. Using a hash func. the user
    info is completely anonymized.

    Args:
        remote_ip: Client IP address, IPv4 or IPv6
        user_agent: User agent string of the client
        tm: Optional timestamp, as the generation is time dependent, setting
            this will make it as if the generation was done at the given time.

    """
    global exp_dict_uid

    try:
        rip: Union[IPv4Address, IPv6Address] = ip_address(remote_ip)
        ip_packed = rip.packed
        key = (ip_packed, user_agent)
    except ValueError:  # in the weird case I don't get an IP
        ip_packed = random.randint(0, 0xffffffff).to_bytes(4, 'big')
        key = (remote_ip, user_agent)
    if key in exp_dict_uid:
        return exp_dict_uid[key]
    # else, generate the thing
    if tm is None:
        tm = int(time.time())
    tm = tm >> 8  # one bucket every 256 seconds, err. < 1%
    h = hashlib.blake2b(digest_size=16, salt=b'biothings')
    h.update(ip_packed)
    h.update(user_agent.encode('utf-8', errors='replace'))
    h.update(tm.to_bytes(8, 'big'))
    d = bytearray(h.digest())
    # truncating hash is not that bad, fixing some bits should be okay, too
    d[6] = 0x40 | (d[6] & 0x0f)  # set version
    d[8] = 0x80 | (d[8] & 0x3f)  # set variant
    u = str(uuid.UUID(bytes=bytes(d)))
    exp_dict_uid[key] = u
    return u


# This is a mixin for biothing handlers, and references class variables from that class, cannot be used
# without mixing in
class GAMixIn:
    def ga_track(self, event={}):
        no_tracking = self.get_argument('no_tracking', None)
        is_prod = not self.settings.get('debug', False)
        if not no_tracking and is_prod and self.web_settings.GA_ACCOUNT:
            _req = self.request
            path = _req.path
            ln = _req.headers.get('Accept-Language', '')
            remote_ip = _req.headers.get("X-Real-Ip", _req.headers.get("X-Forwarded-For", _req.remote_ip))
            user_agent = _req.headers.get("User-Agent", "")
            host = _req.headers.get("Host", "N/A")
            this_user = generate_unique_id(remote_ip, user_agent)
            user_agent = _q(user_agent)
            # compile measurement protocol string for google
            # first do the pageview hit type
            request_body = 'v=1&t=pageview&tid={}&ds=web&cid={}&uip={}&ua={}&an={}&av={}&dh={}&dp={}'.format(
                self.web_settings.GA_ACCOUNT, this_user, remote_ip, user_agent,
                self.web_settings.GA_TRACKER_URL, self.web_settings.API_VERSION, host, path)
            # add the event, if applicable
            if event:
                request_body += '\nv=1&t=event&tid={}&ds=web&cid={}&uip={}&ua={}&an={}&av={}&dh={}&dp={}'.format(
                    self.web_settings.GA_ACCOUNT, this_user, remote_ip, user_agent,
                    self.web_settings.GA_TRACKER_URL, self.web_settings.API_VERSION, host, path)
                # add event information also
                request_body += '&ec={}&ea={}'.format(event['category'], event['action'])
                if event.get('label', False) and event.get('value', False):
                    request_body += '&el={}&ev={}'.format(event['label'], event['value'])

            req = HTTPRequest('http://www.google-analytics.com/batch', method='POST', body=request_body)

            #now send actual async requests
            http_client = AsyncHTTPClient()
            http_client.fetch(req)
