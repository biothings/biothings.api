''' For Google Analytics tracking in web '''
import hashlib
import re
import uuid
from ipaddress import ip_address, IPv4Address, IPv6Address
from operator import itemgetter
from random import randint
from typing import Union
from urllib.parse import quote_plus as _q

from tornado.httpclient import HTTPRequest, AsyncHTTPClient

RE_LOCALE = re.compile(r'(^|\s*,\s*)([a-zA-Z]{1,8}(-[a-zA-Z]{1,8})*)\s*(;\s*q\s*=\s*(1(\.0{0,3})?|0(\.[0-9]{0,3})))?', re.I)

def get_user_language(lang):
    user_locals = []
    matched_locales = RE_LOCALE.findall(str(lang))
    if matched_locales:
        lang_lst = map((lambda x: x.replace('-', '_')), (i[1] for i in matched_locales))
        quality_lst = map((lambda x: x and x or 1), (float(i[4] and i[4] or '0') for i in matched_locales))
        lang_quality_map = map((lambda x, y: (x, y)), lang_lst, quality_lst)
        user_locals = [x[0] for x in sorted(lang_quality_map, key=itemgetter(1), reverse=True)]

    if user_locals:
        return user_locals[0]
    else:
        return ''

def generate_hash(user_agent, screen_resolution, screen_color_depth):
    tmpstr = "%s%s%s" % (user_agent, screen_resolution, screen_color_depth)
    hash_val = 1

    if tmpstr:
        hash_val = 0
        for ordinal in map(ord, tmpstr[::-1]):
            hash_val = ((hash_val << 6) & 0xfffffff) + ordinal + (ordinal << 14)
            left_most_7 = hash_val & 0xfe00000
            if left_most_7 != 0:
                hash_val ^= left_most_7 >> 21

    return hash_val

def generate_unique_id(user_agent='', screen_resolution='', screen_color_depth=''):
    '''Generates a unique user ID from the current user-specific properties.'''
    return ((randint(0, 0x7fffffff) ^ generate_hash(user_agent, screen_resolution, screen_color_depth))
            & 0x7fffffff)


def generate_unique_id_v2(remote_ip: str, user_agent: str):
    """
    Generates a unique user ID

    Using the remote IP and client user agent, to produce a somewhat
    unique identifier for users. A UUID version 4 conforming to RFC 4122
    is produced which is generally acceptable. It is not entirely random,
    but looks random enough to the outside. Using a hash func. the user
    info is completely anonymized.

    Args:
        remote_ip: Client IP address as a string, IPv4 or IPv6
        user_agent: User agent string of the client
    """

    try:
        rip: Union[IPv4Address, IPv6Address] = ip_address(remote_ip)
        ip_packed = rip.packed
    except ValueError:  # in the weird case I don't get an IP
        ip_packed = randint(0, 0xffffffff).to_bytes(4, 'big')  # nosec

    h = hashlib.blake2b(digest_size=16, salt=b'biothings')
    h.update(ip_packed)
    h.update(user_agent.encode('utf-8', errors='replace'))

    d = bytearray(h.digest())
    # truncating hash is not that bad, fixing some bits should be okay, too
    d[6] = 0x40 | (d[6] & 0x0f)  # set version
    d[8] = 0x80 | (d[8] & 0x3f)  # set variant
    u = str(uuid.UUID(bytes=bytes(d)))
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
            this_user = generate_unique_id(user_agent=user_agent)
            if getattr(self.web_settings, 'GA_UID_GENERATOR_VERSION', 1) == 2:
                this_user = generate_unique_id_v2(remote_ip, user_agent)
            user_agent = _q(user_agent)
            langua = get_user_language(ln)
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
