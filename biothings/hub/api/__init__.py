import tornado.web
from biothings.hub.api.handlers.hub import HubHandler
from biothings.hub.api.handlers.source import SourceHandler
from biothings.hub.api.handlers.manager import ManagerHandler

def get_api_app(managers, settings={}):

    routes = [(r"/", HubHandler, {"managers":managers}),
              # source
              (r"/source/?", SourceHandler, {"managers":managers}),
              (r"/source/(\w+)", SourceHandler, {"managers":managers}),
              # manager
              (r"/manager/?", ManagerHandler, {"managers":managers}),
              (r"/manager/(\w+)", ManagerHandler, {"managers":managers}),
              # misc/static
              (r"/static/(.*)", tornado.web.StaticFileHandler,{"path":"biothings/hub/app/static"}),
              (r"/home()",tornado.web.StaticFileHandler,{"path":"biothings/hub/app/html/index.html"})]

    app = tornado.web.Application(routes,settings=settings)

    return app
