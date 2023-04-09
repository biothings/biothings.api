from biothings.web.handlers import BaseAPIHandler


class CustomCacheHandler(BaseAPIHandler):
    cache = 999

    async def get(self):
        res = {
            "success": True,
            "status": "yellow",
        }
        self.finish(res)


class DefautlAPIHandler(BaseAPIHandler):
    async def get(self):
        res = {
            "success": True,
            "status": "yellow",
        }
        self.finish(res)
