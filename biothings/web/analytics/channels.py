import aiohttp
import asyncio
import certifi
import logging
import orjson

from biothings.web.analytics.events import Event, Message

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
        async with session.post(
                url,
                json=event.to_slack_payload(),
                verify=certifi.where()  # for Windows compatibility
        ) as _:
            pass


class GAChannel(Channel):
    def __init__(self, tracking_id, uid_version=1):
        self.tracking_id = tracking_id
        self.uid_version = uid_version

    async def handles(self, event):
        return isinstance(event, Event)

    async def send(self, payload):
        events = payload.to_GA_payload(self.tracking_id, self.uid_version)
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(events), 20):
                data = "\n".join(events[i:i + 20])
                url = "http://www.google-analytics.com/batch"
                await self.send_request(session, url, data)

    async def send_request(self, session, url, data):
        async with session.post(url, data=data) as _:
            pass


class GA4Channel(Channel):
    def __init__(self, measurement_id, api_secret, uid_version=1):
        self.measurement_id = measurement_id
        self.api_secret = api_secret
        self.uid_version = uid_version
        self.max_retries = 1

    async def handles(self, event):
        return isinstance(event, Event)

    async def send(self, payload):
        events = payload.to_GA4_payload(self.measurement_id, self.uid_version)
        url = f"https://www.google-analytics.com/mp/collect?measurement_id={self.measurement_id}&api_secret={self.api_secret}"
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(events), 25): # TODO: Add reference to the page size
                data = {
                    "client_id": str(payload._cid(self.uid_version)),
                    "user_id": str(payload._cid(1)),
                    "events": events[i:i + 25],
                }
                await self.send_request(session, url, orjson.dumps(data))

    async def send_request(self, session, url, data):
        retries = 0
        base_delay = 1  # Base delay in seconds
        while retries <= self.max_retries:
            async with session.post(url, data=data) as response:
                if response.status == 502:  # HTTP 502 - Bad Gateway
                    logging.warning(f"GA4Channel: Received HTTP 502. Retrying ({retries+1}/{self.max_retries})...")
                    delay = base_delay * (2 ** (retries - 1))  # Exponential backoff (1s, 2s, 4s, 8s, etc.)
                    await asyncio.sleep(delay)  # Add a delay before retrying
                    retries += 1
                else:
                    return  # Return if successful or not 502

        # If max retries reached without success, raise an exception
        logging.error("GA4Channel: Maximum retries reached. Unable to complete request.")
        raise Exception("GA4Channel: Maximum retries reached. Unable to complete request.")
