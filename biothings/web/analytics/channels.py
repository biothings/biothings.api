import aiohttp
import asyncio
import certifi
import logging
import orjson
import ssl

from biothings.web.analytics.events import Event, Message
from aiohttp import ClientConnectionError


class Channel:
    async def handles(self, event):
        raise NotImplementedError()

    async def send(self, event):
        raise NotImplementedError()


class SlackChannel(Channel):
    def __init__(self, hook_urls):
        self.hooks = hook_urls

    async def handles(self, event):
        return isinstance(event, Message)

    async def send(self, event):
        async with aiohttp.ClientSession() as session:
            tasks = [self.send_request(session, url, event) for url in self.hooks]
            await asyncio.gather(*tasks)

    async def send_request(self, session, url, event):
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with session.post(url, json=event.to_slack_payload(), ssl=ssl_context) as _:  # for Windows compatibility
            pass


class GAChannel(Channel):
    def __init__(self, tracking_id, uid_version=1):
        self.tracking_id = tracking_id
        self.uid_version = uid_version
        self.url = "http://www.google-analytics.com/batch"

    async def handles(self, event):
        return isinstance(event, Event)

    async def send(self, event):
        events = event.to_GA_payload(self.tracking_id, self.uid_version)
        async with aiohttp.ClientSession() as session:
            # The pagination of 20 is defined according to the context of the current application
            # Usually, each client request is going to make just 1 request to the GA API.
            # However, it's possible to collect data to GA in other parts of the application.
            for i in range(0, len(events), 20):
                data = "\n".join(events[i : i + 20])
                await self.send_request(session, self.url, data)

    async def send_request(self, session, url, data):
        async with session.post(url, data=data) as _:
            pass


class GA4Channel(Channel):
    def __init__(self, measurement_id, api_secret, uid_version=1):
        self.measurement_id = measurement_id
        self.api_secret = api_secret
        self.uid_version = uid_version
        self.max_retries = 1
        self.url = f"https://www.google-analytics.com/mp/collect?measurement_id={self.measurement_id}&api_secret={self.api_secret}"

    async def handles(self, event):
        return isinstance(event, Event)

    async def send(self, event):
        events = event.to_GA4_payload(self.measurement_id, self.uid_version)
        async with aiohttp.ClientSession() as session:
            # The pagination of 25 is defined according to the context of the current application
            # Usually, each client request is going to make just 1 request to the GA4 API.
            # However, it's possible to collect data to GA4 in other parts of the application.
            for i in range(0, len(events), 25):
                data = {
                    "client_id": str(event._cid(self.uid_version)),
                    "user_id": str(event._cid(1)),
                    "events": events[i : i + 25],
                }
                await self.send_request(session, self.url, orjson.dumps(data))

    async def send_request(self, session, url, data):
        retries = 0
        base_delay = 1  # Base delay in seconds
        while retries <= self.max_retries:
            try:
                async with session.post(url, data=data) as response:
                    if response.status >= 500:  # HTTP 5xx
                        logging.warning(
                            "GA4Channel: Received HTTP %d. Retrying (%d/%d)...",
                            response.status, retries + 1, self.max_retries
                        )
                        delay = base_delay * (2 ** retries)  # Exponential backoff (1s, 2s, 4s, 8s, etc.)
                        await asyncio.sleep(delay)  # Add a delay before retrying
                        retries += 1
                    else:
                        return  # Return if successful or not 502
            except ClientConnectionError as e:
                if "SSL shutdown timed out" in str(e):
                    logging.debug("GA4Channel: Ignored SSL shutdown timeout.")
                    return
                else:
                    logging.warning("GA4Channel: Connection error: %s", e)
                    retries += 1
                    await asyncio.sleep(base_delay)

        # If max retries reached without success, raise an exception
        logging.error("GA4Channel: Maximum retries reached. Unable to complete request.")
        raise Exception("GA4Channel: Maximum retries reached. Unable to complete request.")
