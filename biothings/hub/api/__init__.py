import asyncio
import contextlib
import inspect
import logging
import socket
import time
import types
from functools import partial

import tornado.web

from biothings.hub.api.handlers.base import GenericHandler
from biothings.utils.hub import CommandDefinition, CommandError, CommandInformation


class EndpointDefinition(dict):
    pass


def generate_endpoint_for_callable(name, command, method, force_bodyargs):
    if force_bodyargs is True:
        pass
        # assert method != "get", \
        #    "Can't have force_bodyargs=True with method '%s' for command '%s'" % (method,command)
    try:
        specs = inspect.getfullargspec(command)
    except TypeError as e:
        # TODO: generate a display handler
        raise TypeError("Can't determine arguments for command '%s': %s" % (command, e))

    argfrom = 0
    if (
        isinstance(command, types.MethodType)
        or isinstance(command, partial)
        and isinstance(command.func, types.MethodType)
    ):
        # skip "self" arg
        argfrom = 1
    defaultargs = {}
    args = specs.args[argfrom:]
    # defaults are listed from the end ("n latest args")
    if specs.defaults:
        for i in range(-len(specs.defaults), 0):
            defaultargs[args[i]] = specs.defaults[i]
    # ignore "self" args, assuming it's the one used when dealing with method
    # mandatargs = set(args).difference({"self"}).difference(defaultargs) or ""
    mandatargs = force_bodyargs is False and set(args).difference(defaultargs) or ""
    cmdargs = "{}"
    if mandatargs:
        # this is for cmd args building before submitting to the shell
        cmdargs = "{" + ",".join(["'''%s''':%s" % (v, v) for v in mandatargs]) + "}"
        # this is for signature
        mandatargs = "," + ",".join(["%s" % v for v in mandatargs])
    # generate a wrapper over the passed command
    # print({"method":method,"args":args,"defaultargs":defaultargs,"name":name,
    #       "mandatargs":mandatargs,"cmdargs":cmdargs})
    strcode = """
async def %(method)s(self%(mandatargs)s):
    '''%(name)s => %(command)s'''
    cmdargs = %(cmdargs)s
    reqargs = {} # holds either body or query string args
    qkwargs = {} # holds kwargs (either from body or query string)
    for k in cmdargs:
        if cmdargs[k] is None:
            raise tornado.web.HTTPError(400,reason="Bad Request (Missing argument " + k + ")")

    if "%(method)s" != "get":
        # allow to have no body at all, defaulting to empty dict (no args)
        reqargs = tornado.escape.json_decode(self.request.body or '{}')
    elif %(force_bodyargs)s == True: # force_bodyargs
        for arg in %(args)s:
            qarg = self.get_query_argument(arg,None)
            if qarg:
                reqargs[arg] = qarg;
    else:
        # extract optional args
        for arg in self.request.arguments:
            if not arg in reqargs:
                qkwargs[arg] = self.get_argument(arg)
    #print("arguments:")
    #print(%(args)s)
    #print(%(defaultargs)s)
    #print(reqargs)
    #print(qkwargs)
    for arg in %(args)s + list(reqargs.keys()) + list(qkwargs.keys()):
        if arg in %(defaultargs)s or arg in qkwargs:
            mandatory = False
        else:
            mandatory = True
        if %(force_bodyargs)s or "%(method)s" != "get":
            try:
                if mandatory:
                    # part of signature (in URL) or body args ?
                    try:
                        cmdargs[arg] # just check key exists
                    except KeyError:
                        cmdargs[arg] = reqargs[arg]
                else:
                    # check if optional has been passed or if value is taken from default
                    # (used to display/build command line with minimal info,
                    # ie. what's been passed by user)
                    try:
                        val = reqargs[arg]
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
                    val = qkwargs[arg]
                    cmdargs[arg] = val
                except KeyError:
                    pass
    # we don't pass though shell evaluation there
    # to prevent security issue (injection)...
    strcmd = '''%(name)s''' + "("
    strcmd += ",".join([str(k) + "=" + repr(v) for k,v in cmdargs.items()])
    strcmd += ")"
    res = command(**cmdargs)
    # ... but we register the command in the shell to track it
    cmdres = shell.register_command(strcmd,res)
    if type(cmdres) == CommandInformation:# or type(cmdres) == list and type(:
        # asyncio tasks unserializable
        # but keep original one
        cmdres = CommandInformation([(k,v) for k,v in cmdres.items() if k != 'jobs'])
    from inspect import isawaitable
    if isawaitable(cmdres):
        self.write(await cmdres)
    else:
        self.write(cmdres)
""" % {
        "method": method,
        "args": args,
        "defaultargs": defaultargs,
        "name": name,
        "mandatargs": mandatargs,
        "cmdargs": cmdargs,
        "command": repr(command),
        "force_bodyargs": force_bodyargs,
    }
    # if name == "info" or name == "builds":
    #    print(strcode)
    return strcode, mandatargs != ""


def generate_endpoint_for_composite_command(name, command, method):
    strcode = """
async def %(method)s(self):
    # composite commands never take arguments
    cmdres = shell.eval('''%(name)s()''',return_cmdinfo=True)
    if type(cmdres) == CommandInformation:
        # asyncio tasks unserializable
        # but keep original one
        cmdres = CommandInformation([(k,v) for k,v in cmdres.items() if k != 'jobs'])
    self.write(cmdres)
""" % {
        "method": method,
        "name": name,
    }
    return strcode


def generate_endpoint_for_display(name, command, method):
    strcode = """
async def %(method)s(self):
    self.write(command)
""" % {
        "method": method
    }
    return strcode


def generate_handler(shell, name, command_defs):
    if not type(command_defs) == list:
        command_defs = [command_defs]

    by_suffix = {}
    for commanddef in command_defs:
        confdict = {}
        method = commanddef["method"].lower()
        cmdname = commanddef["name"]
        try:
            # retrieve the actual command from the shell
            command = shell.commands.get(cmdname)  # first try public commands
            if command is None:
                command = shell.extra_ns[cmdname]  # then private/hidden commands
            # could be directly a callable or an encapsulating CommandDefinition
            if type(command) == CommandDefinition:
                command = command["command"]
        except KeyError as e:
            raise CommandError("Command '%s' can't be found in hub shell" % e)
        force_bodyargs = commanddef.get("force_bodyargs", False)
        suffix = commanddef.get("suffix", "")
        num_mandatory = 0
        # if callable(command):
        strcode, num_mandatory = generate_endpoint_for_callable(cmdname, command, method, force_bodyargs)
        # elif type(command) == CompositeCommand:
        #    strcode = generate_endpoint_for_composite_command(name,command,method)
        # else:
        #    assert method == "get", "display endpoint needs a GET method for command '%s'" % name
        #    strcode = generate_endpoint_for_display(name,command,method)
        # compile the code string and eval (link) to a globals() dict
        code = compile(strcode, "<string>", "exec")
        command_globals = {}
        endpoint_ns = {
            "command": command,
            "asyncio": asyncio,
            "shell": shell,
            "CommandInformation": CommandInformation,
            "tornado": tornado,
        }
        eval(code, endpoint_ns, command_globals)
        methodfunc = command_globals[method]
        confdict[method] = methodfunc

        by_suffix.setdefault(suffix, {})
        by_suffix[suffix].setdefault(method, {})["confdict"] = confdict
        by_suffix[suffix][method]["num_mandatory"] = num_mandatory
        by_suffix[suffix][method]["strcode"] = strcode

    routes = []
    # when suffix present, it gives a new handler
    # so method don't get mixed (actually overwritten) together with non-suffixed
    # for i, (suffix, by_method) in enumerate(by_suffix.items()):
    for suffix, by_method in by_suffix.items():
        confdict = {}
        methods = []
        # merge all method into same handler
        for method, dat in by_method.items():
            confdict.update(dat["confdict"])
            methods.append(method)
        handler_class = type("%s%s_handler" % (name, suffix), (GenericHandler,), confdict)
        num_mandatory = dat["num_mandatory"]
        if suffix and not suffix.startswith("/"):
            suffix = "/" + suffix
        if num_mandatory:
            # this is for more REST alike URLs (eg. /info/clinvar == /info?src=clinvar
            url = r"/%s%s%s" % (name, r"/([\w\.-]+)?" * num_mandatory, suffix)
        else:
            url = r"/%s%s" % (name, suffix)
        routes.append((url, handler_class, {"shell": shell}))
        logging.info("route: %s %s => %s" % (repr([m.upper() for m in methods]), url, handler_class))

    return routes


def create_handlers(shell, command_defs):
    routes = []
    for cmdname, config in command_defs.items():
        if type(config) == list:
            # multiple endpoints per handler
            commands = config
        else:
            # normalized as a list
            commands = [
                EndpointDefinition(
                    name=config["name"],
                    method=config.get("method", "GET"),
                    force_bodyargs=config.get("force_bodyargs", False),
                )
            ]
        try:
            routes += generate_handler(shell, cmdname, commands)
        except TypeError as e:
            logging.exception("Can't generate handler for '%s': %s" % (cmdname, e))
            continue

    return routes


# def generate_api_routes(shell, commands, settings={}):
def generate_api_routes(shell, commands):
    routes = create_handlers(shell, commands)
    return routes


def start_api(app, port, check=True, wait=5, retry=5, settings=None):
    settings = settings or {}
    if check:
        # check if port is used
        def check_socket(host, port):
            num = 1
            with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                if sock.connect_ex((host, port)) == 0:
                    if num >= retry:
                        raise Exception(f"Can't start API, port {port} is already used and already tried {retry} times")
                    logging.info("Port %s is already used, sleep and retry (%s/%s)", port, num, retry)
                    time.sleep(wait)
                    num += 1
                else:
                    return

        check_socket("localhost", port)
    app_server = tornado.httpserver.HTTPServer(app, **settings)
    app_server.listen(port)
    app_server.start()
    return app_server
