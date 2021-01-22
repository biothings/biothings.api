import asyncio
import random
import string
import time
from urllib.parse import urlencode

from tornado.httpclient import AsyncHTTPClient

URL = "http://localhost:8000/v1/query"

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


def get_random_terms(num=1000):
    """
    Generate strings with 3 letters and 1-3 numbers like:
    "abc123", "cdk2", "AAA22". Used for query terms or ids.
    """
    return ','.join(
        ''.join(random.choices(string.ascii_letters, k=3)) + str(random.randrange(num))
        for _ in range(num)
    )


async def main(num=10):
    """
    Time and measure the performance of the web server.
    Use the num parameter to control concurrency.
    """
    success = 0
    start_time = time.time()
    tasks = [asyncio.create_task(request_one_batch(seq)) for seq in range(num)]
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
    asyncio.run(main(20))
