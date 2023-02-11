import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.locks
import tornado.options
import tornado.web

from biothings.utils.serializer import to_json


class NoResultError(Exception):
    pass


async def get_available_routes(db, table_space):
    collection_names = db.collection_names()
    list_routes = [f"/{item}/" for item in collection_names if item in table_space]
    detail_routes = [f"/{item}/([^/]+)/" for item in collection_names if item in table_space]
    return list_routes, detail_routes


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")


class HomeHandler(BaseHandler):
    async def get(self):
        list_routes, detail_routes = await get_available_routes(
            self.application.db, self.application.table_space
        )
        self.write(to_json(list_routes + detail_routes))


class EntryHandler(BaseHandler):
    async def get(self, slug, item_id):
        src_cols = self.application.db[slug]
        entries = src_cols.find({"_id": item_id})
        if not entries:
            raise tornado.web.HTTPError(404)
        self.write(to_json(entries))


class EntriesHandler(BaseHandler):
    async def get(self, slug):
        src_cols = self.application.db[slug]

        start = self.get_argument("from", 0, True)
        limit = self.get_argument("size", 10, True)
        query_string = self.get_argument("q", "", True)
        query_params = {
            key_value.split(":", 1)[0]
            .strip()
            .strip('"')
            .strip("'"): key_value.split(":", 1)[1]
            .strip()
            .strip('"')
            .strip("'")
            for key_value in query_string.split("AND")
            if key_value and len(key_value.split(":", 1)) == 2
        }
        if limit:
            limit = int(limit)
            start = int(start)
            entries, total_hit = src_cols.find_with_count(query_params, start=start, limit=limit)
        else:
            entries, total_hit = src_cols.find_with_count(query_params)
        if not entries:
            entries = []

        self.write(
            to_json(
                {
                    "from": start,
                    "end": start + len(entries),
                    "total_hit": total_hit,
                    "entries": entries,
                }
            )
        )


class Application(tornado.web.Application):
    def __init__(self, db, table_space, **settings):
        self.db = db
        self.table_space = table_space
        handlers = [
            (r"/?", HomeHandler),
            (r"/([^/]+)/?", EntriesHandler),
            (r"/([^/]+)/([^/]+)/?", EntryHandler),
        ]
        settings.update({"debug": True})
        super().__init__(handlers, **settings)


async def main(host, port, db, table_space):
    app = Application(db, table_space, **{"static_path": "static"})
    print(f"Listening on http://{host}:{port}")
    print(f"There are all available routes:\nhttp://{host}:{port}/")
    list_routes, detail_routes = await get_available_routes(db, table_space)
    for route in list_routes:
        print(f"http://{host}:{port}/{route.strip('/')}/")
        print(f"http://{host}:{port}/{route.strip('/')}?from=0&size=10")
        print(
            f"http://{host}:{port}/{route.strip('/')}?q='field1_name:value1 AND field2_name:value2'"
        )
        print(f"http://{host}:{port}/{route.strip('/')}/<doc_id>")
    app.listen(port, address=host)
    shutdown_event = tornado.locks.Event()
    await shutdown_event.wait()
