import tornado.web
from biothings.hub.api.handlers.hub import HubHandler, StatsHandler
from biothings.hub.api.handlers.manager import ManagerHandler
from biothings.hub.api.handlers.source import SourceHandler, DumpSourceHandler, \
                                              UploadSourceHandler


def get_api_app(managers, settings={}):

    routes = [
            (r"/", HubHandler, {"managers":managers}),
            (r"/stats", StatsHandler, {"managers":managers}),
            # source
            (r"/source/?", SourceHandler, {"managers":managers}),
            (r"/source/(\w+)", SourceHandler, {"managers":managers}),
            (r"/source/(\w+)/dump", DumpSourceHandler, {"managers":managers}),
            (r"/source/(\w+)/upload", UploadSourceHandler, {"managers":managers}),
            # manager
            (r"/manager/?", ManagerHandler, {"managers":managers}),
            (r"/manager/(\w+)", ManagerHandler, {"managers":managers}),
            # misc/static
            (r"/static/(.*)", tornado.web.StaticFileHandler,{"path":"biothings/hub/app/static"}),
            (r"/home()",tornado.web.StaticFileHandler,{"path":"biothings/hub/app/html/index.html"})]

    app = tornado.web.Application(routes,settings=settings)

    return app
