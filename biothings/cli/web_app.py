import random
from itertools import chain

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.locks
import tornado.options
import tornado.web
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel

from biothings.utils.common import traverse
from biothings.utils.serializer import load_json, to_json


class NoResultError(Exception):
    pass


async def get_available_routes(db, table_space):
    """return a list available URLs/routes based on the table_space and the actual collections in the database"""
    collection_names = set(db.collection_names())
    list_routes = []
    detail_routes = []
    for table in table_space:
        if table in collection_names:
            rprint(f"[green]Found collection: [/green]{table}[green]; counting...[/green]", end="")
            tbl_cnt = db[table].count()
            rprint(f"[green]Done ([/green]{tbl_cnt}[green] documents)[/green]")
            if tbl_cnt > 0:
                list_routes.append(f"/{table}/")
                detail_routes.append(f"/{table}/([^/]+)/")
                db._count = getattr(db, "_count", {})
                db._count[table] = tbl_cnt  # save table count for later use
                if tbl_cnt > 100000:
                    rprint(
                        f"[yellow]WARNING: collection [/yellow]{table}[yellow] has more than 100,000 documents. \n[/yellow]"
                        f'[yellow]Queries below can be slow since the data are not indexed, except the "_id" field.[/yellow]'
                    )
    return list_routes, detail_routes


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")


class HomeHandler(BaseHandler):
    """the handler for the landing page, which lists all available routes"""

    async def get(self):
        list_routes, detail_routes = await get_available_routes(self.application.db, self.application.table_space)
        self.write(to_json(list_routes + detail_routes))


class DocHandler(BaseHandler):
    """The handler for the detail view of a document, e.g. /<source>/<doc_id/"""

    async def get(self, slug, item_id):
        src_cols = self.application.db[slug]
        doc = src_cols.find_one({"_id": item_id})
        if not doc:
            raise tornado.web.HTTPError(404)
        self.write(to_json(doc))


class QueryHandler(BaseHandler):
    """The handler for return a list of docs matching the query terms passed to "q" parameter e.g. /<source>/?q=<query>"""

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
            # entries, total_hit = src_cols.find_with_count(query_params, start=start, limit=limit)
            entries, total_hit = src_cols.findv2(
                query_params, start=start, limit=limit, return_total=True, return_list=True
            )
        else:
            # entries, total_hit = src_cols.find_with_count(query_params)
            entries, total_hit = src_cols.findv2(query_params, return_total=True)
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


def get_example_queries(db, table_space):
    """Populate example queries for a given table_space"""
    out = {}
    for table in table_space:
        col = db[table]
        total_cnt = getattr(db, "_count", {}).get(table, col.count())
        n = 5
        i = random.randint(0, min(1000, total_cnt - n))
        random_docs = [
            load_json(row[0])
            for row in (
                col.get_conn()
                .execute(
                    # f"SELECT document FROM {table} WHERE _id IN (SELECT _id FROM {table} ORDER BY RANDOM() LIMIT 10)"
                    f"SELECT document FROM {table} LIMIT {n} OFFSET {i}"
                )
                .fetchall()
            )
        ]
        key_value_list = list(chain(*[traverse(doc, leaf_node=True) for doc in random_docs]))
        selected_fields = []
        while len(selected_fields) < n:
            key, value = random.choice(key_value_list)
            if key == "_id" or not value or (isinstance(value, str) and (len(value) > 50 or " " in value)):
                continue
            selected_fields.append((key, value))
        out[table] = {"ids": [doc["_id"] for doc in random_docs], "fields": selected_fields}
    return out


class Application(tornado.web.Application):
    """The main application class, which defines the routes and handlers."""

    def __init__(self, db, table_space, **settings):
        self.db = db
        self.table_space = table_space
        handlers = [
            (r"/?", HomeHandler),
            (r"/([^/]+)/?", QueryHandler),
            (r"/([^/]+)/([^/]+)/?", DocHandler),
        ]
        settings.update({"debug": True})
        super().__init__(handlers, **settings)


async def main(host, port, db, table_space):
    """The main function, which starts the server."""
    list_routes, detail_routes = await get_available_routes(db, table_space)
    del detail_routes
    if not list_routes:
        rprint('[red]Error: Source data do not exist or are empty. Was "upload" runned successfully yet?[/red]')
        return

    app = Application(db, table_space, **{"static_path": "static"})
    app.listen(port, address=host)

    rprint(f"[green]Listening on http://{host}:{port}[/green]")
    rprint(f"[green]View all available routes: http://{host}:{port}/[/green]")
    example_queries = get_example_queries(db, table_space)
    console = Console()
    for route in list_routes:
        route = route.strip("/")
        example_ids = example_queries[route]["ids"]
        example_fields = [(k, str(v)) for k, v in example_queries[route]["fields"]]
        console.print(
            Panel(
                "\n"
                + ":link: Get a document by id:\n"
                + f"    [green]http://{host}:{port}/{route}/<doc_id>[/green]\n"
                + "    [green]Examples:[/green]\n"
                + f"     [green]http://{host}:{port}/{route}/{example_ids[0]}[/green]\n"
                + f"     [green]http://{host}:{port}/{route}/{example_ids[1]}[/green]\n"
                + ":link: Query documents by fields:\n"
                + f"    [green]http://{host}:{port}/{route}?q=<query>[/green]\n"
                + "    [green]Examples:[/green]\n"
                + f"     [green]http://{host}:{port}/{route}?from=0&size=10[/green]\n"
                + f"     [green]http://{host}:{port}/{route}?q={':'.join(example_fields[0])}[/green]\n"
                + f"     [green]http://{host}:{port}/{route}?q={':'.join(example_fields[1])} AND {':'.join(example_fields[-1])}[/green]\n",
                title=f"[bold]http://{host}:{port}/{route}[/bold]",
                title_align="left",
            )
        )

    shutdown_event = tornado.locks.Event()
    await shutdown_event.wait()
