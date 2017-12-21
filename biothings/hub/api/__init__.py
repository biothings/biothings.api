import inspect, types
import asyncio

import tornado.web
from biothings.hub.api.handlers.base import GenericHandler
from biothings.hub.api.handlers.hub import HubHandler, StatsHandler
from biothings.hub.api.handlers.manager import ManagerHandler
from biothings.hub.api.handlers.source import SourceHandler, DumpSourceHandler, \
                                              UploadSourceHandler

def generate_handler(name, command, method):
    method = method.lower()
    try:
        specs = inspect.getfullargspec(command)
    except TypeError as e:
        # TODO: generate a display handler
        raise TypeError("Can't determine arguments for command '%s': %s" % (command,e))

    argfrom = 0
    if type(command) == types.MethodType:
        # skip "self" arg
        argfrom = 1
    defaultargs = {}
    args = specs.args[argfrom:]
    # defaults are listed from the end ("n latest args")
    if specs.defaults:
        for i in range(-len(specs.defaults),0):
            defaultargs[args[i]] = specs.defaults[i]
    # generate a wrapper over the passed command
    strcode = """
@asyncio.coroutine
def %s(self):
    cmdargs = {}
    for arg in %s:
        try:
            defarg = %s[arg]
            cmdargs[arg] = self.get_argument(arg,defarg)
        except KeyError:
            cmdargs[arg] = self.get_argument(arg)
    #print(cmdargs)
    res = command(**cmdargs)
    self.write(res)
""" % (method,args,defaultargs)
    print(strcode)
    # compile the code string and eval (link) to a globals() dict
    code = compile(strcode,"<string>","exec")
    command_globals = {}
    eval(code,{"command":command,"asyncio":asyncio},command_globals)
    methodfunc = command_globals[method]
    confdict = {method:methodfunc}
    klass = type("%s_handler" % name,(GenericHandler,),confdict)
    return klass

def create_handlers(commands,command_methods,prefixes={}):
    routes = []
    for cmdname, command in commands.items():
        method = command_methods.get(cmdname,"GET")
        prefix = prefixes.get(cmdname,"")
        try:
            handler_class = generate_handler(cmdname,command,method)
        except TypeError:
            continue
        routes.append((r"%s/%s" % (prefix,cmdname),handler_class,{"managers":{}}))

    return routes

def generate_api_app(commands,command_methods,command_prefixes={},settings={}):
    routes = create_handlers(commands,command_methods,command_prefixes)
    app = tornado.web.Application(routes,settings=settings)
    print(routes)
    return app

#def get_api_app(managers, settings={}):
#
#    routes = [
#            (r"/", HubHandler, {"managers":managers}),
#            (r"/stats", StatsHandler, {"managers":managers}),
#            # source
#            (r"/source/?", SourceHandler, {"managers":managers}),
#            (r"/source/(\w+)", SourceHandler, {"managers":managers}),
#            (r"/source/(\w+)/dump", DumpSourceHandler, {"managers":managers}),
#            (r"/source/(\w+)/upload", UploadSourceHandler, {"managers":managers}),
#            # manager
#            (r"/manager/?", ManagerHandler, {"managers":managers}),
#            (r"/manager/(\w+)", ManagerHandler, {"managers":managers}),
#            # misc/static
#            (r"/static/(.*)", tornado.web.StaticFileHandler,{"path":"biothings/hub/app/static"}),
#            (r"/home()",tornado.web.StaticFileHandler,{"path":"biothings/hub/app/html/index.html"}),
#            (r"/whatsnew",generate_handler("whatnew",managers["build_manager"].whatsnew,"GET"),{"managers":managers}),
#            ]
#
#    app = tornado.web.Application(routes,settings=settings)
#
#    return app
