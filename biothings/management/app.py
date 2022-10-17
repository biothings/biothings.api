import json

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.locks
import tornado.options
import tornado.web


class NoResultError(Exception):
    pass


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")


class HomeHandler(BaseHandler):
    async def get(self):
        collection_names = self.application.db.collection_names()
        list_routes = [
            f"/{item}/" for item in collection_names if item in self.application.table_space
        ]
        detail_routes = [
            f"/{item}/([^/]+)/"
            for item in collection_names
            if item in self.application.table_space
        ]
        routes = list_routes + detail_routes
        self.write(json.dumps(routes))


class EntryHandler(BaseHandler):
    async def get(self, slug, item_id):
        src_cols = self.application.db[slug]
        entries = src_cols.find({"_id": item_id})
        if not entries:
            raise tornado.web.HTTPError(404)

        r = json.dumps(entries)
        self.write(r)


class EntriesHandler(BaseHandler):
    async def get(self, slug):
        src_cols = self.application.db[slug]

        start = self.get_argument("start", "0", True)
        limit = self.get_argument("limit", None, True)
        query_params = {
            key: self.get_argument(key)
            for key in self.request.arguments
            if key not in ["start", "limit"]
        }
        if limit:
            limit = int(limit)
            start = int(start)
            entries = src_cols.find(query_params, start=start, limit=limit)
        else:
            entries = src_cols.find(query_params)
        if not entries:
            entries = []

        r = json.dumps(entries)
        self.write(r)


class Application(tornado.web.Application):
    def __init__(self, db, table_space):
        self.db = db
        self.table_space = table_space
        handlers = [
            (r"/", HomeHandler),
            (r"/([^/]+)/", EntriesHandler),
            (r"/([^/]+)/([^/]+)/", EntryHandler),
        ]
        settings = dict(
            debug=True,
        )
        super().__init__(handlers, **settings)


async def main(port, db, table_space):
    app = Application(db, table_space)
    app.listen(port)
    shutdown_event = tornado.locks.Event()
    await shutdown_event.wait()
