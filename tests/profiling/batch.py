import asyncio
import json
import os
import random
import string
import time
from urllib.parse import urlencode

import certifi
from tornado.httpclient import AsyncHTTPClient

URL = "http://localhost:8000/v1/query"

ES_URL = "https://<service endpoint>/<index pattern>/_msearch"
ES_USERNAME = os.environ.get("ES_USERNAME")
ES_PASSWORD = os.environ.get("ES_PASSWORD")

# FOR BIOTHINGS ENDPOINT
async def request_one_batch(batch_seq="!"):
    """
    Make a batch query request.
    Typically POST to query endpoint.
    """
    http_client = AsyncHTTPClient()
    body = urlencode({'q': get_random_terms()})
    response = await http_client.fetch(
        URL, method="POST", body=body, raise_error=False,
        connect_timeout=600, request_timeout=600)
    if response.code != 200:
        return response.code, response.body, str(batch_seq)
    return "OK " + str(batch_seq)

# FOR ELASTICSEARCH ENDPOINT
async def request_one_batch_elasticsearch(batch_seq="!"):
    """
    Make a batch query request.
    Typically POST to query endpoint.
    """
    http_client = AsyncHTTPClient()
    body = "\n".join((
        "{}\n" + json.dumps({
            "query": {
                "query_string": {
                    "query": get_random_term()
                }
            }
        }) for _ in range(1000)
    )) + '\n'
    response = await http_client.fetch(
        ES_URL, method="GET", body=body, raise_error=False,
        connect_timeout=600, request_timeout=600, ca_certs=certifi.where(),
        auth_username=ES_USERNAME, auth_password=ES_PASSWORD,
        headers={"Content-Type": "application/x-ndjson"},
        allow_nonstandard_methods=True)
    if response.code != 200:
        return response.code, response.body, str(batch_seq)
    return "OK " + str(batch_seq)

async def request_10_batch_elasticsearch(batch_seq="!"):
    for _ in range(10):
        res = await request_one_batch_elasticsearch()
        if not (isinstance(res, str) and res.startswith("OK")):
            raise RuntimeError()
    return "OK" + str(batch_seq)

def get_random_term():
    return ''.join(random.choices(string.ascii_letters, k=3)) + str(random.randrange(1000))

def get_random_terms(num=1000):
    """
    Generate strings with 3 letters and 1-3 numbers like:
    "abc123", "cdk2", "AAA22". Used for query terms or ids.
    """
    return ','.join(
        get_random_term()
        for _ in range(num)
    )


async def main(num=10):
    """
    Time and measure the performance of the web server.
    Use the num parameter to control concurrency.
    """
    success = 0
    start_time = time.time()
    # func = request_one_batch
    # func = request_one_batch_elasticsearch
    func = request_10_batch_elasticsearch  # result x 10
    tasks = [asyncio.create_task(func(seq)) for seq in range(num)]
    for task in tasks:
        await task
        result = task.result()
        print(result, end=" ", flush=True)
        if "OK" in result:
            success += 1
    print()
    seconds = time.time() - start_time
    print("--- %s seconds ---" % seconds)
    print("%s requests/second" % int(success*1000/seconds))


if __name__ == '__main__':
    while True:
        asyncio.run(main(40))
