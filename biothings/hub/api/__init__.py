import inspect, types, logging
import asyncio

import tornado.web
from biothings.hub.api.handlers.base import GenericHandler
from biothings.hub.api.handlers.hub import HubHandler, StatsHandler
from biothings.hub.api.handlers.manager import ManagerHandler
from biothings.hub.api.handlers.source import SourceHandler, DumpSourceHandler, \
                                              UploadSourceHandler
from biothings.utils.hub import CompositeCommand, CommandInformation


def generate_endpoint_for_callable(name, command, method):
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
def %(method)s(self):
    cmdargs = {}
    for arg in %(args)s:
        try:
            defarg = %(defaultargs)s[arg]
            cmdargs[arg] = self.get_argument(arg,defarg)
        except KeyError:
            cmdargs[arg] = self.get_argument(arg)
    # we don't pass though shell evaluation there
    # to prevent security issue (injection)...
    res = command(**cmdargs)
    # ... but we register the command in the shell to track it
    cmdres = shell.register_command('''%(name)s''',res)
    if type(cmdres) == CommandInformation:
        # asyncio tasks unserializable
        # but keep original one
        cmdres = CommandInformation([(k,v) for k,v in cmdres.items() if k != 'jobs'])
    self.write(cmdres)
""" % {"method":method,"args":args,"defaultargs":defaultargs,"name":name}
    return strcode

def generate_endpoint_for_composite_command(name, command, method):
    strcode = """
@asyncio.coroutine
def %(method)s(self):
    # composite commands never take arguments
    cmdres = shell.eval('''%(name)s()''',return_cmdinfo=True)
    if type(cmdres) == CommandInformation:
        # asyncio tasks unserializable
        # but keep original one
        cmdres = CommandInformation([(k,v) for k,v in cmdres.items() if k != 'jobs'])
    self.write(cmdres)
""" % {"method":method,"name":name}
    return strcode

def generate_endpoint_for_display(name, command, method):
    strcode = """
@asyncio.coroutine
def %(method)s(self):
    self.write(command)
""" % {"method":method}
    return strcode

def generate_handler(shell, name, command, method):
    method = method.lower()
    if callable(command):
        strcode = generate_endpoint_for_callable(name,command,method)
    elif type(command) == CompositeCommand:
        strcode = generate_endpoint_for_composite_command(name,command,method)
    else:
        strcode = generate_endpoint_for_display(name,command,method)
    # compile the code string and eval (link) to a globals() dict
    code = compile(strcode,"<string>","exec")
    command_globals = {}
    endpoint_ns = {"command":command,"asyncio":asyncio,"shell":shell,"CommandInformation":CommandInformation}
    eval(code,endpoint_ns,command_globals)
    methodfunc = command_globals[method]
    confdict = {method:methodfunc}
    klass = type("%s_handler" % name,(GenericHandler,),confdict)
    return klass

def create_handlers(shell, commands, command_methods, command_prefixes={}):
    routes = []
    for cmdname, command in commands.items():
        method = command_methods.get(cmdname,"GET")
        prefix = command_prefixes.get(cmdname,"")
        try:
            handler_class = generate_handler(shell,cmdname,command,method)
        except TypeError as e:
            logging.warning("Can't generate handler for '%s': %s" % (cmdname,e))
            continue
        routes.append((r"%s/%s" % (prefix,cmdname),handler_class,{"shell":shell}))

    return routes

def generate_api_routes(shell, commands, command_methods, command_prefixes={},settings={}):
    routes = create_handlers(shell,commands,command_methods,command_prefixes)
    return routes

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
