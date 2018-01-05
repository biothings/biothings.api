import inspect, types, logging
import asyncio
from functools import partial

import tornado.web
from biothings.hub.api.handlers.base import GenericHandler
from biothings.hub.api.handlers.hub import HubHandler, StatsHandler
from biothings.utils.hub import CompositeCommand, CommandInformation


def generate_endpoint_for_callable(name, command, method):
    try:
        specs = inspect.getfullargspec(command)
    except TypeError as e:
        # TODO: generate a display handler
        raise TypeError("Can't determine arguments for command '%s': %s" % (command,e))

    argfrom = 0
    if type(command) == types.MethodType or \
            type(command) == partial and type(command.func) == types.MethodType:
        # skip "self" arg
        argfrom = 1
    defaultargs = {}
    args = specs.args[argfrom:]
    # defaults are listed from the end ("n latest args")
    if specs.defaults:
        for i in range(-len(specs.defaults),0):
            defaultargs[args[i]] = specs.defaults[i]
    # ignore "self" args, assuming it's the one used when dealing with method
    #mandatargs = set(args).difference({"self"}).difference(defaultargs) or ""
    mandatargs = set(args).difference(defaultargs) or ""
    cmdargs = "{}"
    if mandatargs:
        # this is for cmd args building before submitting to the shell 
        cmdargs = "{" + ",".join(["'''%s''':%s" % (v,v) for v in mandatargs]) + "}"
        # this is for signature
        mandatargs = "," + ",".join(["%s" % v for v in mandatargs])
    # generate a wrapper over the passed command
    strcode = """
@asyncio.coroutine
def %(method)s(self%(mandatargs)s):
    cmdargs = %(cmdargs)s
    for k in cmdargs:
        if cmdargs[k] is None:
            raise tornado.web.HTTPError(400,reason="Bad Request (Missing argument " + k + ")")

    if "%(method)s" != "get":
        # allow to have no body at all, defaulting to empty dict (no args)
        bodyargs = tornado.escape.json_decode(self.request.body or '{}')
    for arg in %(args)s:
        mandatory = False
        try:
            defarg = %(defaultargs)s[arg]
        except KeyError:
            mandatory = True
            defarg = None
        if "%(method)s" != "get":
            try:
                if mandatory:
                    cmdargs[arg] # just check the key
                else:
                    val = bodyargs.get(arg,defarg)
                    cmdargs[arg] = val
            except KeyError:
                raise tornado.web.HTTPError(400,reason="Bad Request (Missing argument " + arg + ")")
        else:
            # if not default arg and arg not passed, this will raise a 400 (by tornado)
            if mandatory:
                    cmdargs[arg] # check key
            else:
                val = self.get_argument(arg,defarg)
                cmdargs[arg] = val
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
""" % {"method":method,"args":args,"defaultargs":defaultargs,"name":name,
        "mandatargs":mandatargs,"cmdargs":cmdargs}
    #print(strcode)
    return strcode, mandatargs != ""

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
    num_mandatory = 0
    if callable(command):
        strcode,num_mandatory = generate_endpoint_for_callable(name,command,method)
    elif type(command) == CompositeCommand:
        strcode = generate_endpoint_for_composite_command(name,command,method)
    else:
        assert method == "get", "display endpoint needs a GET method for command '%s'" % name
        strcode = generate_endpoint_for_display(name,command,method)
    # compile the code string and eval (link) to a globals() dict
    code = compile(strcode,"<string>","exec")
    command_globals = {}
    endpoint_ns = {"command":command,"asyncio":asyncio,
                   "shell":shell,"CommandInformation":CommandInformation,
                   "tornado":tornado}
    eval(code,endpoint_ns,command_globals)
    methodfunc = command_globals[method]
    confdict = {method:methodfunc}
    klass = type("%s_handler" % name,(GenericHandler,),confdict)
    return klass,num_mandatory

def create_handlers(shell, commands, command_methods, command_prefixes={}):
    routes = []
    for cmdname, command in commands.items():
        method = command_methods.get(cmdname,"GET")
        prefix = command_prefixes.get(cmdname,"")
        try:
            handler_class,num_mandatory= generate_handler(shell,cmdname,command,method)
        except TypeError as e:
            logging.warning("Can't generate handler for '%s': %s" % (cmdname,e))
            continue
        if num_mandatory:
            # this is for more REST alike URLs (eg. /info/clinvar == /info?src=clinvar
            routes.append((r"%s/%s%s" % (prefix,cmdname,"/(\w+)?"*num_mandatory),handler_class,{"shell":shell}))
        else:
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
#            # misc/static
#            (r"/static/(.*)", tornado.web.StaticFileHandler,{"path":"biothings/hub/app/static"}),
#            (r"/home()",tornado.web.StaticFileHandler,{"path":"biothings/hub/app/html/index.html"}),
#            ]
#
#    app = tornado.web.Application(routes,settings=settings)
#
#    return app
