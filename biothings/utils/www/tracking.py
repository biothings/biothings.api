''' For Standalone biothing tracking '''
import sys, os, base64, datetime, hashlib, hmac, json, logging 
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

# This is a mixin for biothing handlers, and references class variables from that class, cannot be used
# without mixing in
class StandaloneTrackingMixin:
    def self_track(self, data={}):
        #logging.debug("In Standalone Request Tracking")
        no_tracking = self.get_argument('no_tracking', None)
        access_key = self.web_settings.STANDALONE_AWS_CREDENTIALS.get('AWS_ACCESS_KEY_ID', False)
        secret_key = self.web_settings.STANDALONE_AWS_CREDENTIALS.get('AWS_SECRET_ACCESS_KEY', False)
        if not no_tracking and self.web_settings.STANDALONE_TRACKING_URL and access_key and secret_key:
            request_body = json.dumps({
                "action": data.get('action', 'NA'),
                "biothing": self.web_settings.ES_DOC_TYPE,
                "category": data.get('category', 'NA')
            })
            #logging.debug("Standalone Request Body: {}".format(request_body))

            # ************* REQUEST VALUES *************
            method = 'POST'
            service = 'execute-api'
            endpoint = self.web_settings.STANDALONE_TRACKING_URL
            host = endpoint.split('://')[1].split('/')[0]
            canonical_uri = endpoint.split(host)[1]
            region = 'us-west-1'

            # POST requests use a content type header.
            content_type = 'application/x-amz-json-1.0'
            content_length = len(request_body)

            # Create a date for headers and the credential string
            t = datetime.datetime.utcnow()
            amz_date = t.strftime('%Y%m%dT%H%M%SZ')
            date_stamp = t.strftime('%Y%m%d') # Date w/o time, used in credential scope

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
            string_to_sign = algorithm + '\n' +  amz_date + '\n' +  credential_scope + '\n' +  hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()


            # ************* TASK 3: CALCULATE THE SIGNATURE *************
            # Create the signing key using the function defined above.
            signing_key = getSignatureKey(secret_key, date_stamp, region, service)

            # Sign the string_to_sign using the signing_key
            signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()


            # ************* TASK 4: ADD SIGNING INFORMATION TO THE REQUEST *************
            # Put the signature information in a header named Authorization.
            authorization_header = algorithm + ' ' + 'Credential=' + access_key + '/' + credential_scope + ', ' +  'SignedHeaders=' + signed_headers + ', ' + 'Signature=' + signature

            req = HTTPRequest(url=self.web_settings.STANDALONE_TRACKING_URL, method=method, body=request_body, 
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
            http_client.fetch(req)
