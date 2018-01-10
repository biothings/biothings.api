import inspect, types, logging
import asyncio
from functools import partial

import tornado.web
from biothings.hub.api.handlers.base import GenericHandler
from biothings.hub.api.handlers.hub import HubHandler, StatsHandler
from biothings.utils.hub import CompositeCommand, CommandInformation


def generate_endpoint_for_callable(name, command, method, force_bodyargs):
    if force_bodyargs is True:
        assert method != "get", \
            "Can't have force_bodyargs=True with method '%s' for command '%s'" % (method,command)
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
    mandatargs = force_bodyargs is False and set(args).difference(defaultargs) or ""
    cmdargs = "{}"
    if mandatargs:
        # this is for cmd args building before submitting to the shell 
        cmdargs = "{" + ",".join(["'''%s''':%s" % (v,v) for v in mandatargs]) + "}"
        # this is for signature
        mandatargs = "," + ",".join(["%s" % v for v in mandatargs])
    # generate a wrapper over the passed command
    #print({"method":method,"args":args,"defaultargs":defaultargs,"name":name,
    #    "mandatargs":mandatargs,"cmdargs":cmdargs})
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
                    # part of signature (in URL) or body args ?
                    try:
                        cmdargs[arg] # just check key exists
                    except KeyError:
                        cmdargs[arg] = bodyargs[arg]
                else:
                    # check if optional has been passed or if value is taken from default
                    # (used to display/build command line with minimal info,
                    # ie. what's been passed by user)
                    try:
                        val = bodyargs[arg]
                        cmdargs[arg] = val
                    except KeyError:
                        pass
            except KeyError:
                raise tornado.web.HTTPError(400,reason="Bad Request (Missing argument " + arg + ")")
        else:
            # if not default arg and arg not passed, this will raise a 400 (by tornado)
            if mandatory:
                    cmdargs[arg] # check key
            else:
                try:
                    val = self.get_argument(arg)
                    cmdargs[arg] = val
                except tornado.web.MissingArgumentError:
                    pass
    # we don't pass though shell evaluation there
    # to prevent security issue (injection)...
    strcmd = '''%(name)s''' + "("
    strcmd += ",".join([str(k) + "=" + repr(v) for k,v in cmdargs.items()])
    strcmd += ")"
    res = command(**cmdargs)
    # ... but we register the command in the shell to track it
    cmdres = shell.register_command(strcmd,res)
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

def generate_handler(shell, name, command, method, force_bodyargs):
    method = method.lower()
    num_mandatory = 0
    if callable(command):
        strcode,num_mandatory = generate_endpoint_for_callable(name,command,method,force_bodyargs)
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

def create_handlers(shell, commands, command_prefixes={}):
    routes = []
    for cmdname, config in commands.items():
        command = config["command"]
        method = config.get("method","GET")
        prefix = config.get("prefix","")
        force_bodyargs = config.get("force_bodyargs",False)
        try:
            handler_class,num_mandatory= generate_handler(shell,cmdname,command,method,force_bodyargs)
        except TypeError as e:
            logging.warning("Can't generate handler for '%s': %s" % (cmdname,e))
            continue
        if num_mandatory:
            # this is for more REST alike URLs (eg. /info/clinvar == /info?src=clinvar
            routes.append((r"%s/%s%s" % (prefix,cmdname,"/(\w+)?"*num_mandatory),handler_class,{"shell":shell}))
        else:
            routes.append((r"%s/%s" % (prefix,cmdname),handler_class,{"shell":shell}))

    return routes

def generate_api_routes(shell, commands, command_prefixes={},settings={}):
    routes = create_handlers(shell,commands,command_prefixes)
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
