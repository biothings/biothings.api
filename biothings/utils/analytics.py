from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from biothings.settings import BiothingSettings
from random import randint
import re
from urllib.parse import quote_plus as _q

bts = BiothingSettings()

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

class GAMixIn:
    def ga_track(self, event={}):
        no_tracking = self.get_argument('no_tracking', None)
        is_prod = bts.ga_is_prod
        if not no_tracking and is_prod and bts.ga_account:
            _req = self.request
            path = _req.path
            ln = _req.headers.get('Accept-Language', '')
            remote_ip = _req.headers.get("X-Real-Ip",
                        _req.headers.get("X-Forwarded-For",
                        _req.remote_ip))
            user_agent = _req.headers.get("User-Agent", None)
            host = _req.headers.get("Host", "N/A")
            # compile measurement protocol string for google
            # first do the pageview hit type
            request_body = 'v=1&t=pageview&tid={}&ds=web&cid={}&uip={}&ua={}&geoid={}&an={}&av={}&dh={}&dp={}'.format(
                bts.ga_account, generate_unique_id(user_agent=user_agent), remote_ip, _q(user_agent), 
                get_user_language(ln), bts.ga_tracker_url, bts._api_version, host, path)
            # add the event, if applicable
            if event:
                request_body += '\nv=1&t=event&tid={}&ds=web&cid={}&uip={}&ua={}&geoid={}&an={}&av={}&dh={}&dp={}'.format(
                bts.ga_account, generate_unique_id(user_agent=user_agent), remote_ip, _q(user_agent), 
                get_user_language(ln), bts.ga_tracker_url, bts._api_version, host, path)
                # add event information also
                request_body += '&ec={}&ea={}&el={}&ev={}'.format(event['category'], event['action'],
                    event['label'], event['value'])

            req = HTTPRequest('http://www.google-analytics.com/batch', method='POST', body=request_body)

            #now send actual async requests
            http_client = AsyncHTTPClient()
            http_client.fetch(req)
