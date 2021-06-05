# ----------------
#       Google
# ----------------

# https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide
# https://ga-dev-tools.appspot.com/hit-builder/?v=1&tid=UA-XXXXX-Y&cid=555&t=pageview&uip=1.2.3.4&ua=Opera/9.80
# https://developers.google.com/analytics/devguides/reporting/data/v1/quotas
#


''' For Google Analytics tracking in web '''

import hashlib
import re
import uuid
from ipaddress import IPv4Address, IPv6Address, ip_address
from operator import itemgetter
from random import randint
from typing import Union
from urllib.parse import quote_plus as _q

from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

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
        # to control UID generation behavior to use the new algorithm
        # explicitly set GA_UID_GENERATOR_VERSION = 2 in config
        no_tracking = self.get_argument('no_tracking', None)
        is_prod = not self.settings.get('debug', False)
        if not no_tracking and is_prod and self.biothings.config.GA_ACCOUNT:
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
            referrer = _req.headers.get("Referer", None)  # headers is c.i.
            # FIXME: in the case that the encoded value is actually more than
            #  2048 bytes (GA Limit), this truncate may break some things.
            # Typically we don't have to worry about it because most browsers
            # only send the host part now, not the full URL.
            # Legitimate requests from modern browsers is unlikely to be over
            # the limit, as domain names are limited to 255 chars. An attacker
            # might try to put really big headers here but we don't need to
            # worry about it.
            # Use 2047 here in case GA counts a \0 internally
            if referrer:
                referrer = url_escape(referrer)[:2047]
            # compile measurement protocol string for google
            # first do the pageview hit type
            request_body = 'v=1&t=pageview&tid={}&ds=web&cid={}&uip={}&ua={}&an={}&av={}&dh={}&dp={}'.format(
                self.biothings.config.GA_ACCOUNT, this_user, remote_ip, user_agent,
                self.biothings.config.GA_TRACKER_URL, self.biothings.config.API_VERSION, host, path)
            # add referrer
            if referrer:
                request_body += f'&dr={referrer}'
            # add the event, if applicable
            if event:
                request_body += '\nv=1&t=event&tid={}&ds=web&cid={}&uip={}&ua={}&an={}&av={}&dh={}&dp={}'.format(
                    self.biothings.config.GA_ACCOUNT, this_user, remote_ip, user_agent,
                    self.biothings.config.GA_TRACKER_URL, self.biothings.config.API_VERSION, host, path)
                # add event information also
                request_body += '&ec={}&ea={}'.format(event['category'], event['action'])
                if event.get('label', False) and event.get('value', False):
                    request_body += '&el={}&ev={}'.format(event['label'], event['value'])
                if referrer:
                    request_body += f'&dr={referrer}'

            req = HTTPRequest('http://www.google-analytics.com/batch', method='POST', body=request_body)

            #now send actual async requests
            http_client = AsyncHTTPClient()
            http_client.fetch(req)


# ----------------
#       AWS
# ----------------

''' For Standalone biothing tracking '''
import sys
import os
import base64
import datetime
import hashlib
import hmac
import json
import logging
from tornado.httpclient import HTTPRequest, AsyncHTTPClient

# Key derivation functions. See:
# http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def getSignatureKey(key, date_stamp, regionName, serviceName):
    kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, serviceName)
    kSigning = sign(kService, 'aws4_request')
    return kSigning

def tracking_callback(response):
    logging.debug("GA response: {}".format(str(response)))
    logging.debug("Body: {}".format(response.buffer.getvalue()))
    logging.debug("Response Headers: {}".format(str(response.headers)))
    logging.debug("Request Headers: {}".format(str(response.request.headers)))
    logging.debug("Request Body: {}".format(str(response.request.body)))
    return

# This is a mixin for biothing handlers, and references class variables from that class, cannot be used
# without mixing in
class StandaloneTrackingMixin:
    def self_track(self, data={}):
        no_tracking = self.get_argument('no_tracking', None)
        access_key = self.biothings.config.STANDALONE_AWS_CREDENTIALS.get('AWS_ACCESS_KEY_ID', False)
        secret_key = self.biothings.config.STANDALONE_AWS_CREDENTIALS.get('AWS_SECRET_ACCESS_KEY', False)
        if not no_tracking and self.biothings.config.STANDALONE_TRACKING_URL and access_key and secret_key:
            self.biothings.config.tracking_payload.append(json.dumps({
                "action": data.get('action', 'NA'),
                "biothing": self.biothings.config.ES_DOC_TYPE,
                "category": data.get('category', 'NA')
            }))
            logging.debug("tracking_payload size: {}".format(len(self.biothings.config.tracking_payload)))
            if (len(self.biothings.config.tracking_payload) == self.biothings.config.STANDALONE_TRACKING_BATCH_SIZE):

                # ************* REQUEST VALUES *************
                request_body = '\n'.join(self.biothings.config.tracking_payload)
                # logging.debug("Standalone Request Body: {}".format(request_body))
                # reset payload
                self.biothings.config.tracking_payload = []
                method = 'POST'
                service = 'execute-api'
                endpoint = self.biothings.config.STANDALONE_TRACKING_URL
                host = endpoint.split('://')[1].split('/')[0]
                canonical_uri = endpoint.split(host)[1]
                region = 'us-west-1'

                # POST requests use a content type header.
                content_type = 'application/x-amz-json-1.0'
                content_length = len(request_body)

                # Create a date for headers and the credential string
                t = datetime.datetime.utcnow()
                amz_date = t.strftime('%Y%m%dT%H%M%SZ')
                date_stamp = t.strftime('%Y%m%d')  # Date w/o time, used in credential scope

                # ************* TASK 1: CREATE A CANONICAL REQUEST *************
                # http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html

                # Step 1 is to define the verb (GET, POST, etc.)--already done.

                # Step 2: Create canonical URI--the part of the URI from domain to query
                # string (use '/' if no path) -- already done.

                ## Step 3: Create the canonical query string. In this example, request
                # parameters are passed in the body of the request and the query string
                # is blank.
                canonical_querystring = ''

                # Step 4: Create the canonical headers. Header names must be trimmed
                # and lowercase, and sorted in code point order from low to high.
                # Note that there is a trailing \n.
                canonical_headers = 'content-length:' + '{}'.format(content_length) + '\n' + 'content-type:' + content_type + '\n' + 'host:' + host + '\n' + 'x-amz-date:' + amz_date + '\n'

                # Step 5: Create the list of signed headers. This lists the headers
                # in the canonical_headers list, delimited with ";" and in alpha order.
                # Note: The request can include any headers; canonical_headers and
                # signed_headers include those that you want to be included in the
                # hash of the request. "Host" and "x-amz-date" are always required.
                signed_headers = 'content-length;content-type;host;x-amz-date'

                # Step 6: Create payload hash.
                payload_hash = hashlib.sha256(request_body.encode('utf-8')).hexdigest()

                # Step 7: Combine elements to create create canonical request
                canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash

                # ************* TASK 2: CREATE THE STRING TO SIGN*************
                # Match the algorithm to the hashing algorithm you use, either SHA-1 or
                # SHA-256 (recommended)
                algorithm = 'AWS4-HMAC-SHA256'
                credential_scope = date_stamp + '/' + region + '/' + service + '/' + 'aws4_request'
                string_to_sign = algorithm + '\n' + amz_date + '\n' + credential_scope + '\n' + hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()

                # ************* TASK 3: CALCULATE THE SIGNATURE *************
                # Create the signing key using the function defined above.
                signing_key = getSignatureKey(secret_key, date_stamp, region, service)

                # Sign the string_to_sign using the signing_key
                signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()

                # ************* TASK 4: ADD SIGNING INFORMATION TO THE REQUEST *************
                # Put the signature information in a header named Authorization.
                authorization_header = algorithm + ' ' + 'Credential=' + access_key + '/' + credential_scope + ', ' + 'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature

                req = HTTPRequest(url=self.biothings.config.STANDALONE_TRACKING_URL, method=method, body=request_body,
                                  headers={
                                      "Content-Type": content_type,
                                      "Content-Length": content_length,
                                      "X-Amz-Date": amz_date,
                                      "Authorization": authorization_header,
                                      "Host": host
                                  }
                                  )

                #now send actual async requests
                http_client = AsyncHTTPClient()
                http_client.fetch(req)  # , callback=tracking_callback)
