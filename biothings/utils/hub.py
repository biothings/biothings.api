# from http://asyncssh.readthedocs.io/en/latest/#id13

# To run this program, the file ``ssh_host_key`` must exist with an SSH
# private key in it to use as a server host key.

import os, glob, re, pickle, datetime, json, pydoc, copy
import asyncio, asyncssh, crypt, sys, io, inspect
import types, aiocron, time
from functools import partial
from IPython import InteractiveShell
from pprint import pprint, pformat
from collections import OrderedDict
import pymongo
import pyinotify

from biothings import config
from biothings.utils.dataload import to_boolean
logging = config.logger
from biothings.utils.common import timesofar, sizeof_fmt
import biothings.utils.aws as aws
from biothings.utils.hub_db import get_cmd, get_src_dump, get_src_build, get_src_build_config, \
                                   get_last_command, backup, restore
from biothings.utils.loggers import get_logger
from biothings.utils.jsondiff import make as jsondiff

# useful variables to bring into hub namespace
pending = "pending"
done = "done"

HUB_ENV = hasattr(config,"HUB_ENV") and config.HUB_ENV or "" # default: prod (or "normal")
VERSIONS = HUB_ENV and "%s-versions" % HUB_ENV or "versions"
LATEST = HUB_ENV and "%s-latest" % HUB_ENV or "latest"
HUB_REFRESH_COMMANDS = hasattr(config,"HUB_REFRESH_COMMANDS") and config.HUB_REFRESH_COMMANDS or "* * * * * *" # every sec


def jsonreadify(cmd):
    newcmd = copy.copy(cmd)
    newcmd.pop("jobs")
    return newcmd

##############
# HUB SERVER #
##############

class AlreadyRunningException(Exception):pass
class CommandError(Exception):pass

class CommandInformation(dict): pass
class CommandDefinition(dict): pass

class HubShell(InteractiveShell):

    launched_commands = {}
    pending_outputs = {}
    cmd_cnt = None
    cmd = None # "cmd" collection

    def __init__(self, job_manager):
        self.job_manager = job_manager
        self.commands = OrderedDict()
        self.managers = {}
        self.extra_ns = OrderedDict()
        self.tracked = {} # command calls kept in history or not
        self.hidden = {} # not returned by help()
        self.origout = sys.stdout
        self.buf = io.StringIO()
        # there should be only one shell instance (todo: singleton)
        self.__class__.cmd = get_cmd()
        self.__class__.set_command_counter()
        super(HubShell,self).__init__(user_ns=self.extra_ns)

    @classmethod
    def set_command_counter(klass):
        assert klass.cmd, "No cmd collection set"
        try:
            res = get_last_command()
            if res:
                logging.debug("Last launched command ID: %s" % res["_id"])
                klass.cmd_cnt = int(res["_id"]) + 1
            else:
                logging.info("No previously stored command found, set counter to 1")
                klass.cmd_cnt = 1

        except StopIteration:
            logging.info("Can't find highest command number, assuming starting from scratch")
            klass.cmd_cnt = 1

    def set_commands(self, basic_commands, *extra_ns):
        def register(commands,hidden=False):
            for name,cmd in commands.items():
                if name in self.commands:
                    raise CommandError("Command defined multiple time: %s" % name)
                if type(cmd) == CommandDefinition:
                    try:
                        self.commands[name] = cmd["command"]
                        self.tracked[name] = cmd.get("tracked",True)
                        self.hidden[name] = cmd.get("hidden",False) or hidden
                    except KeyError as e:
                        raise CommandError("Could not register command because missing '%s' in definition: %s" % (e,cmd))
                else:
                    self.commands[name] = cmd
                    self.hidden[name] = hidden
                # update original passed commands to caller knows what's been done therz
                if not name in basic_commands:
                    basic_commands[name] = cmd
        # update with ssh server default commands
        register(basic_commands)
        # don't track this calls
        register({"restart":CommandDefinition(command=self.restart,track=True)})
        register({"stop":CommandDefinition(command=self.stop,track=True)})
        register({"backup":CommandDefinition(command=backup,track=True)})
        register({"restore":CommandDefinition(command=restore,track=True)})
        register({"help":CommandDefinition(command=self.help,track=False)})
        register({"commands":CommandDefinition(command=self.command_info,tracked=False)})
        register({"command":CommandDefinition(command=lambda id,*args,**kwargs:
            self.command_info(id=id,*args,**kwargs),tracked=False)})

        for extra in extra_ns:
            # don't expose extra commands, they're kind of private/advanced
            register(extra,hidden=True)

        #self.extra_ns["cancel"] = self.__class__.cancel
        # for boolean calls
        self.extra_ns["_and"] = _and
        self.extra_ns["partial"] = partial
        self.extra_ns["hub"] = self
        # merge official/public commands with hidden/private to
        # make the whole available in shell's namespace
        self.extra_ns.update(self.commands)
        # Note: there's no need to update shell namespace as self.extra_ns
        # has been passed by ref in __init__() so things get updated automagically
        # (self.user_ns.update(...) can be used otherwise, self.user_ns is IPython
        # internal namespace dict

    def stop(self,force=False):
        return self.restart(force=force,stop=True)

    def restart(self, force=False, stop=False):

        @asyncio.coroutine
        def do():
            try:
                if stop:
                    event = "hub_stop"
                    msg = "Hub is stopping"
                else:
                    event = "hub_restart"
                    msg = "Hub is restarting"
                logging.critical(json.dumps({"type":"alert",
                                  "msg":msg,
                                  "event" : event}),
                                  extra={"event":True})
                logging.info("Stopping job manager...")
                j = self.job_manager.stop(force=force)
                def ok(f):
                    f.result() # consume
                    logging.error("Job manager stopped")
                j.add_done_callback(ok)
                yield from j
            except Exception as e:
                logging.error("Error while recycling the process queue: %s" % e)
                raise

        def start(f):
            f.result() # consume future's result to potentially raise exception
            logging.debug("%s %s" % ([sys.executable] ,sys.argv))
            import subprocess
            p = subprocess.Popen([sys.executable] + sys.argv)
            self.job_manager.hub_process.kill()
            sys.exit(0)

        def autokill(f):
            f.result()
            self.job_manager.hub_process.kill()

        fut = asyncio.ensure_future(do())

        if stop:
            logging.warning("Stopping hub")
            fut.add_done_callback(autokill)
        else:
            logging.warning("Restarting hub")
            fut.add_done_callback(start)

        return fut

    def help(self, func=None):
        """
        Display help on given function/object or list all available commands
        """
        if not func:
            cmds = "\nAvailable commands:\n\n"
            for k in self.commands:
                if self.hidden[k]:
                    continue
                cmds += "\t%s\n" % k
            cmds += "\nType: 'help(command)' for more\n"
            return cmds
        elif isinstance(func,partial):
            docstr = "\n" + pydoc.render_doc(func.func,title="Hub documentation: %s")
            docstr += "\nDefined et as a partial, with:\nargs:%s\nkwargs:%s\n" % (repr(func.args),repr(func.keywords))
            return docstr
        elif isinstance(func,CompositeCommand):
            docstr = "\nComposite command:\n\n%s\n" % func
            return docstr
        else:
            try:
                return "\n" + pydoc.render_doc(func,title="Hub documentation: %s")
            except ImportError:
                return "\nHelp not available for this command\n"

    def launch(self,pfunc):
        """
        Helper to run a command and register it
        pfunc is partial taking no argument. Command name
        is generated from partial's func and arguments
        """
        res = pfunc()
        # rebuild a command as string
        strcmd = pfunc.func.__name__
        strcmd += "("
        strargs = []
        if pfunc.args:
            strargs.append(",".join([repr(a) for a in pfunc.args]))
        if pfunc.keywords:
            strargs.append(",".join(["%s=%s" % (k,repr(v)) for (k,v) in pfunc.keywords.items()]))
        strcmd += ",".join(strargs)
        strcmd += ")"
        # we use force here because, very likely, the command from strcmd we generated
        # isn't part of shell's known commands (and there's a check about that when force=False)
        self.register_command(strcmd,res,force=True)
        return pfunc

    def extract_command_name(self,cmd):
        try:
            # extract before () (non-callable are never tracked)
            grps = re.fullmatch("([\w\.]+)(\(.*\))",cmd.strip()).groups()
            return grps[0]
        except AttributeError:
            raise CommandError("Can't extract command name from '%s'" % repr(cmd))

    @classmethod
    def save_cmd(klass,_id,cmd):
        newcmd = jsonreadify(cmd)
        klass.cmd.replace_one({"_id":_id},newcmd,upsert=True)

    def register_managers(self,managers):
        self.managers = managers

    def register_command(self, cmd, result, force=False):
        """
        Register a command 'cmd' inside the shell (so we can keep track of it).
        'result' is the original value that was returned when cmd was submitted.
        Depending on the type, returns a cmd number (ie. result was an asyncio task
        and we need to wait before getting the result) or directly the result of
        'cmd' execution, returning, in that case, the output.
        """
        # see if command should actually be registered
        try:
            cmdname = self.extract_command_name(cmd)
        except CommandError:
            # if can't extract command name, then don't even try to register
            # (could be, for instance, "pure" python code typed from the console)
            logging.debug("Can't extract command from %s, can't register" % cmd)
            return result
        # also, never register non-callable command
        if not force and (not callable(self.extra_ns.get(cmdname)) or \
                self.tracked.get(cmdname,True) == False):
            return result

        cmdnum = self.__class__.cmd_cnt
        cmdinfo = CommandInformation(cmd=cmd,jobs=result,started_at=time.time(),
                                     id=cmdnum,is_done=False)
        assert not cmdnum in self.__class__.launched_commands
        # register
        self.__class__.launched_commands[cmdnum] = cmdinfo
        self.__class__.save_cmd(cmdnum,cmdinfo)
        self.__class__.cmd_cnt += 1

        if type(result) == asyncio.tasks.Task or type(result) == asyncio.tasks._GatheringFuture or \
                type(result) == asyncio.Future or \
                type(result) == list and len(result) > 0 and type(result[0]) == asyncio.tasks.Task:
            # it's asyncio related
            result = type(result) != list and [result] or result
            cmdinfo["jobs"] = result
            return cmdinfo
        else:
            # ... and it's not asyncio related, we can display it directly
            cmdinfo["is_done"] = True
            cmdinfo["failed"] = False
            cmdinfo["started_at"] = time.time()
            cmdinfo["finished_at"] = time.time()
            cmdinfo["duration"] = "0s"
            return result

    def eval(self, line, return_cmdinfo=False):
        line = line.strip()
        origline = line # keep what's been originally entered
        # poor man's singleton...
        if line in [j["cmd"] for j in self.__class__.launched_commands.values() if not j.get("is_done")]:
            raise AlreadyRunningException("Command '%s' is already running\n" % repr(line))
        # is it a hub command, in which case, intercept and run the actual declared cmd
        m = re.match("(.*)\(.*\)",line)
        if m:
            cmd = m.groups()[0].strip()
            if cmd in self.commands and \
                    isinstance(self.commands[cmd],CompositeCommand):
                line = self.commands[cmd]
        # cmdline is the actual command sent to shell, line is the one displayed
        # they can be different if there's a preprocessing
        cmdline = line
        # && cmds ? ie. chained cmds
        if "&&" in line:
            chained_cmds = [cmd for cmd in map(str.strip,line.split("&&")) if cmd]
            if len(chained_cmds) > 1:
                # need to build a command with _and and using partial, meaning passing original func param
                # to the partials
                strcmds = []
                for one_cmd in chained_cmds:
                    func,args = re.match("(.*)\((.*)\)",one_cmd).groups()
                    if args:
                        strcmds.append("partial(%s,%s)" % (func,args))
                    else:
                        strcmds.append("partial(%s)" % func)
                cmdline = "_and(%s)" % ",".join(strcmds)
            else:
                raise CommandError("Using '&&' operator required two operands\n")
        logging.info("Run: %s " % repr(cmdline))
        r = self.run_cell(cmdline,store_history=True)
        outputs = []
        if not r.success:
            raise CommandError("%s\n" % repr(r.error_in_exec))
        else:
            # command was a success, now get the results:
            if r.result is None:
                # -> nothing special was returned, grab the stdout
                self.buf.seek(0)
                # from print stdout ?
                b = self.buf.read()
                outputs.append(b)
                # clear buffer
                self.buf.seek(0)
                self.buf.truncate()
            else:
                # -> we have something returned...
                res = self.register_command(cmd=origline,result=r.result)
                if type(res) != CommandInformation:
                    if type(res) != str:
                        outputs.append(pformat(res))
                    else:
                        outputs.append(res)
                else:
                    if return_cmdinfo:
                        return res

        # Note: this will cause all outputs to go to one SSH session, ie. if multiple users
        # are logged, only one will the results
        if self.__class__.pending_outputs:
            outputs.extend(self.__class__.pending_outputs.values())
            self.__class__.pending_outputs = {}

        return outputs

    #@classmethod
    #def cancel(klass,jobnum):
    #    return klass.launched_commands.get(jobnum)

    @classmethod
    def refresh_commands(klass):
        for num,info in sorted(klass.launched_commands.items()):
            # already process, this current command is now history
            # Note: if we have millions of commands there, it could last quite a while,
            # but IRL we only have a few
            if info.get("is_done") == True:
                continue
            is_done = set([j.done() for j in info["jobs"]]) == set([True])
            has_err = is_done and  [True for j in info["jobs"] if j.exception()] or None
            localoutputs = is_done and ([str(j.exception()) for j in info["jobs"] if j.exception()] or \
                        [j.result() for j in info["jobs"]]) or None
            if is_done:
                klass.launched_commands[num]["is_done"] = True
                klass.launched_commands[num]["failed"] = has_err and has_err[0] or False
                klass.launched_commands[num]["results"] = localoutputs
                klass.launched_commands[num]["finished_at"] = time.time()
                klass.launched_commands[num]["duration"] = timesofar(t0=klass.launched_commands[num]["started_at"],
                                                                    t1=klass.launched_commands[num]["finished_at"])
                klass.save_cmd(num,klass.launched_commands[num])
                if not has_err and localoutputs and set(map(type,localoutputs)) == {str}:
                    localoutputs = "\n" + "".join(localoutputs)
                klass.pending_outputs[num] = "[%s] %s %s: finished %s" % (num,has_err and "ERR" or "OK ",info["cmd"], localoutputs)
            else:
                klass.pending_outputs[num] = "[%s] RUN {%s} %s" % (num,timesofar(info["started_at"]),info["cmd"])

    @classmethod
    def command_info(klass, id=None, running=None, failed=None):
        cmds = {}
        if not id is None:
            try:
                id = int(id)
                return jsonreadify(klass.launched_commands[id])
            except KeyError:
                raise CommandError("No such command with ID %s" % repr(id))
            except ValueError:
                raise CommandError("Invalid ID %s" % repr(id))
        if not running is None:
            is_done = not to_boolean(running)
        else:
            is_done = None
        if not failed is None:
            failed = to_boolean(failed)
        for _id,cmd in klass.launched_commands.items():
            if not is_done is None:
                # running or done commands (not both)
                if cmd.get("is_done") == is_done:
                    # done + failed (a failed command is always done btw)
                    if not failed is None and cmd.get("is_done") == True:
                        if cmd.get("failed") == failed:
                            cmds[_id] = jsonreadify(cmd)
                    else:
                        # don't care if failed or not
                        cmds[_id] = jsonreadify(cmd)
                else:
                    # If asked is_done=true, it means command _id has is_done=false
                    # if we get there. So the command is sill running, so we don't
                    # know if it failed or not, so no need to check failed there,
                    # it's been handled above.
                    # If asksed is_done=false, we don't need to check failed, 
                    # same logic applies
                    continue
            else:
                # either running or done commands (both)
                if not failed is None and cmd.get("is_done") == True:
                    if cmd.get("failed") == failed:
                        cmds[_id] = jsonreadify(cmd)
                else:
                    # don't care if failed or not
                    cmds[_id] = jsonreadify(cmd)

        return cmds


class HubSSHServerSession(asyncssh.SSHServerSession):

    def __init__(self, name, shell):
        self.name = name
        self.shell = shell
        self._input = ''

    def connection_made(self, chan):
        self._chan = chan

    def shell_requested(self):
        return True

    def exec_requested(self,command):
        self.eval_lines(["%s" % command,"\n"])
        return True

    def session_started(self):
        self._chan.write('\nWelcome to %s, %s!\n' % (self.name,self._chan.get_extra_info('username')))
        self._chan.write('hub> ')

    def data_received(self, data, datatype):
        self._input += data
        return self.eval_lines(self._input.split('\n'))

    def eval_lines(self, lines):
        for line in lines[:-1]:
            try:
                outs = [out for out in self.shell.eval(line) if out]
                # trailing \n if not already there
                if outs:
                    self._chan.write("\n".join(outs).strip("\n") + "\n")
            except AlreadyRunningException as e:
                self._chan.write("AlreadyRunningException: %s" % e)
            except CommandError as e:
                self._chan.write("CommandError: %s" % e)
        self._chan.write('hub> ')
        # consume passed commands
        self._input = lines[-1]

    def eof_received(self):
        self._chan.write('Have a good one...\n')
        self._chan.exit(0)

    def break_received(self, msec):
        # simulate CR
        self._chan.write('\n')
        self.data_received("\n",None)


class HubSSHServer(asyncssh.SSHServer):

    COMMANDS = OrderedDict() # public hub commands
    EXTRA_NS = {} # extra commands, kind-of of hidden/private
    PASSWORDS = {}
    SHELL = None

    def session_requested(self):
        return HubSSHServerSession(self.__class__.NAME,self.__class__.SHELL)

    def connection_made(self, conn):
         self._conn = conn
         print('SSH connection received from %s.' %
         conn.get_extra_info('peername')[0])

    def connection_lost(self, exc):
        if exc:
            print('SSH connection error: ' + str(exc), file=sys.stderr)
        else:
            print('SSH connection closed.')

    def begin_auth(self, username):
        try:
            self._conn.set_authorized_keys('bin/authorized_keys/%s.pub' % username)
        except IOError:
            pass
        return True

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        if self.password_auth_supported():
            pw = self.__class__.PASSWORDS.get(username, '*')
            return crypt.crypt(password, pw) == pw
        else:
            return False


@asyncio.coroutine
def start_server(loop,name,passwords,keys=['bin/ssh_host_key'],shell=None,
                 host='',port=8022):
    for key in keys:
        assert os.path.exists(key),"Missing key '%s' (use: 'ssh-keygen -f %s' to generate it" % (key,key)
    HubSSHServer.PASSWORDS = passwords
    HubSSHServer.NAME = name
    HubSSHServer.SHELL = shell
    cron = aiocron.crontab(HUB_REFRESH_COMMANDS,func=shell.__class__.refresh_commands,
                           start=True, loop=loop)
    yield from asyncssh.create_server(HubSSHServer, host, port, loop=loop,
                                 server_host_keys=keys)


####################
# DEFAULT HUB CMDS #
####################
# these can be used in client code to define
# commands. partial should be used to pass the
# required arguments, eg.:
# {"schedule" ; partial(schedule,loop)}

class JobRenderer(object):

    def __init__(self):
        self.rendered = {
                types.FunctionType : self.render_func,
                types.MethodType : self.render_method,
                partial : self.render_partial,
                types.LambdaType: self.render_lambda,
        }

    def render(self,job):
        r = self.rendered.get(type(job._callback))
        rstr = r(job._callback)
        delta = job._when - job._loop.time()
        days = None
        if delta > 86400:
            days = int(delta/86400)
            delta = delta - 86400
        strdelta = time.strftime("%Hh:%Mm:%Ss", time.gmtime(int(delta)))
        if days:
            strdelta = "%d day(s) %s" % (days,strdelta)
        return "%s {run in %s}" % (rstr,strdelta)

    def render_partial(self,p):
        # class.method(args)
        return self.rendered[type(p.func)](p.func) + "%s" % str(p.args)

    def render_cron(self,c):
        # func type associated to cron can vary
        return self.rendered[type(c.func)](c.func) + " [%s]" % c.spec

    def render_func(self,f):
        return f.__name__

    def render_method(self,m):
        # what is self ? cron ?
        if type(m.__self__) == aiocron.Cron:
            return self.render_cron(m.__self__)
        else:
            return "%s.%s" % (m.__self__.__class__.__name__,
                              m.__name__)

    def render_lambda(self,l):
        return l.__name__

renderer = JobRenderer()

def schedule(loop):
    jobs = {}
    # try to render job in a human-readable way...
    out = []
    for sch in loop._scheduled:
        if type(sch) != asyncio.events.TimerHandle:
            continue
        if sch._cancelled:
            continue
        try:
            info = renderer.render(sch)
            out.append(info)
        except Exception as e:
            import traceback
            traceback.print_exc()
            out.append(sch)

    return "\n".join(out)


def stats(src_dump):
    pass


def publish_data_version(s3_folder,version_info,env=None,update_latest=True):
    """
    Update remote files:
    - versions.json: add version_info to the JSON list
                     or replace if arg version_info is a list
    - latest.json: update redirect so it points to latest version url
    "versions" is dict such as:
        {"build_version":"...",         # version name for this release/build
         "require_version":"...",       # version required for incremental update
         "target_version": "...",       # version reached once update is applied
         "type" : "incremental|full"    # release type
         "release_date" : "...",        # ISO 8601 timestamp, release date/time
         "url": "http...."}             # url pointing to release metadata
    """
    # register version
    versionskey = os.path.join(s3_folder,"%s.json" % VERSIONS)
    try:
        versions = aws.get_s3_file(versionskey,return_what="content",
                aws_key=config.AWS_KEY,aws_secret=config.AWS_SECRET,
                s3_bucket=config.S3_RELEASE_BUCKET)
        versions = json.loads(versions.decode()) # S3 returns bytes
    except (FileNotFoundError,json.JSONDecodeError):
        versions = {"format" : "1.0","versions" : []}
    if type(version_info) == list:
        versions["versions"] = version_info
    else:
        # used to check duplicates
        tmp = {}
        [tmp.setdefault(e["build_version"],e) for e in versions["versions"]]
        tmp[version_info["build_version"]] = version_info
        # order by build_version
        versions["versions"] = sorted(tmp.values(),key=lambda e: e["build_version"])

    aws.send_s3_file(None,versionskey,content=json.dumps(versions,indent=True),
            aws_key=config.AWS_KEY,aws_secret=config.AWS_SECRET,s3_bucket=config.S3_RELEASE_BUCKET,
            content_type="application/json",overwrite=True)

    # update latest
    if type(version_info) != list and update_latest:
        latestkey = os.path.join(s3_folder,"%s.json" % LATEST)
        key = None
        try:
            key = aws.get_s3_file(latestkey,return_what="key",
                    aws_key=config.AWS_KEY,aws_secret=config.AWS_SECRET,
                    s3_bucket=config.S3_RELEASE_BUCKET)
        except FileNotFoundError:
            pass
        aws.send_s3_file(None,latestkey,content=json.dumps(version_info["build_version"],indent=True),
                content_type="application/json",aws_key=config.AWS_KEY,aws_secret=config.AWS_SECRET,
                s3_bucket=config.S3_RELEASE_BUCKET,overwrite=True)
        if not key:
            key = aws.get_s3_file(latestkey,return_what="key",
                    aws_key=config.AWS_KEY,aws_secret=config.AWS_SECRET,
                    s3_bucket=config.S3_RELEASE_BUCKET)
        newredir = os.path.join("/",s3_folder,"%s.json" % version_info["build_version"])
        key.set_redirect(newredir)


def _and(*funcs):
    """
    Calls passed functions, one by one. If one fails, then it stops.
    Function should return a asyncio Task. List of one Task only are also permitted.
    Partial can be used to pass arguments to functions.
    Ex: _and(f1,f2,partial(f3,arg1,kw=arg2))
    """
    all_res = []
    func1 = funcs[0]
    func2 = None
    fut1 = func1()
    if type(fut1) == list:
        assert len(fut1) == 1, "Can't deal with list of more than 1 task: %s" % fut1
        fut1 = fut1.pop()
    all_res.append(fut1)
    err = None
    def do(f,cb):
        res = f.result() # consume exception if any
        if cb:
            all_res.extend(_and(cb,*funcs))
    if len(funcs) > 1:
        func2 = funcs[1]
        if len(funcs) > 2:
            funcs = funcs[2:]
        else:
            funcs = []
    fut1.add_done_callback(partial(do,cb=func2))
    return all_res


class CompositeCommand(str):
    """
    Defines a composite hub commands, that is,
    a new command made of other commands. Useful to define
    shortcuts when typing commands in hub console.
    """
    def __init__(self,cmd):
        self.cmd = cmd
    def __str__(self):
        return "<CompositeCommand: '%s'>" % self.cmd

############
# RELOADER #
############

def exclude_from_reloader(path):
    # exlucde cached, git and hidden files
    return path.endswith("__pycache__") or ".git" in path or \
           os.path.basename(path).startswith(".")

class ReloadListener(pyinotify.ProcessEvent):

    def my_init(self,managers,watcher_manager,reload_func):
        logging.debug("for managers %s" % managers)
        self.managers = managers
        self.watcher_manager = watcher_manager
        self.reload_func = reload_func

    def process_default(self, event):
        if exclude_from_reloader(event.pathname):
            return
        if event.dir:
            if event.mask & pyinotify.IN_CREATE:
                # add to watcher. no need to check if already watched, manager knows
                # how to deal with that
                logging.info("Add '%s' to watcher" % event.pathname)
                self.watcher_manager.add_watch(event.pathname,self.notifier.mask,rec=1)
            elif event.mask & pyinotify.IN_DELETE:
                logging.info("Remove '%s' from watcher" % event.pathname)
                # watcher knows when directory is deleted (file descriptor become invalid),
                # so no need to do it manually
        logging.error("Need to reload manager because of %s" % event)
        self.reload_func()


class HubReloader(object):
    """
    Monitor sources' code and reload managers accordingly to update running code
    """

    def __init__(self,paths,managers,reload_func,wait=5,mask=None):
        """
        Monitor given paths for directory deletion/creation (which will update pyinotify watchers)
        and for file deletion/creation. Reload given managers accordingly.
        Poll for events every 'wait' seconds.
        """
        if type(paths) == str:
            paths = [paths]
        paths = set(paths) # get rid of duplicated, just in case
        self.mask = mask or pyinotify.IN_CREATE|pyinotify.IN_DELETE|pyinotify.IN_CLOSE_WRITE
        self.reload_func = reload_func
        # only listen to these events. Note: directory detection is done via a flag so
        # no need to use IS_DIR
        self.watcher_manager = pyinotify.WatchManager(exclude_filter=exclude_from_reloader)
        _paths = [] # cleaned
        for path in paths:
            if not os.path.isabs(path):
                path = os.path.abspath(path)
            _paths.append(path)
            self.watcher_manager.add_watch(path,self.mask,rec=True,exclude_filter=exclude_from_reloader) # recursive
        self.listener = ReloadListener(managers=managers,
                                       watcher_manager=self.watcher_manager,
                                       reload_func=self.reload_func)
        self.notifier = pyinotify.Notifier(
                self.watcher_manager,
                default_proc_fun=self.listener)
        # propagate notifier so notifer itself can be reloaded (when new directory/source)
        self.listener.notifier = self
        self.paths = _paths
        self.wait = wait
        self.do_monitor = False

    def monitor(self):

        @asyncio.coroutine
        def do():
            logging.info("Monitoring source code in, %s:\n%s" % \
                    (repr(self.paths),pformat([v.path for v in self.watcher_manager.watches.values()])))

            while self.do_monitor:
                try:
                    yield from asyncio.sleep(self.wait)
                    # this reads events from OS
                    if self.notifier.check_events(0.1):
                        self.notifier.read_events()
                    # Listener gets called there if event avail
                    self.notifier.process_events()
                except KeyboardInterrupt:
                    logging.warning("Stop monitoring code")
                    break
        if getattr(config,"USE_RELOADER",False) and config.USE_RELOADER:
            self.do_monitor = True
            return asyncio.ensure_future(do())
        else:
            logging.info("USE_RELOADER not set (or False), won't monitor for changes")
            return None

    def watches(self):
        return [v.path for v in self.watcher_manager.watches.values()]


def status(managers):
    """
    Return a global hub status (number or sources, documents, etc...)
    according to available managers
    """
    total_srcs = None
    total_docs = None
    total_confs = None
    total_builds = None
    total_apis = None
    total_running_apis = None
    if managers.get("source_manager"):
        try:
            srcm = managers["source_manager"]
            srcs = srcm.get_sources()
            total_srcs = len(srcs)
            total_docs = sum([s["upload"]["sources"][subs].get("count",0) or 0 \
                            for s in srcs
                            for subs in s.get("upload",{}).get("sources",{}) \
                            if s.get("upload")])
        except Exception as e:
            logging.error("Can't get stats for sources: %s" % e)

    try:
        bm = managers["build_manager"]
        total_confs = len(bm.build_config_info())
    except Exception as e:
        logging.error("Can't get total number of build configurations: %s" % e)
    try:
        total_builds = len(bm.build_info())
    except Exception as e:
        logging.error("Can't get total number of builds: %s" % e)

    try:
        am = managers["api_manager"]
        apis = am.get_apis()
        total_apis = len(apis)
        total_running_apis = len([a for a in apis if a.get("status") == "running"])
    except Exception as e:
        logging.error("Can't get stats for APIs: %s" % e)

    return {
            "source" : {"total" : total_srcs,
                        "documents" : total_docs},
            "build" : {"total" : total_builds},
            "build_conf" : {"total" : total_confs},
            "api" : {"total" : total_apis,
                     "running" : total_running_apis},
            }


class HubServer(object):

    DEFAULT_FEATURES = ["dump","upload","build","diff", "index",
                        "dataplugin","inspect","sync","api","job"]
    DEFAULT_MANAGERS_ARGS = {"upload" : {"poll_schedule" : "* * * * * */10"}}
    DEFAULT_RELOADER_CONFIG = {"folders": None, # will use default one
                               "managers" : ["source_manager","assistant_manager"],
                               "reload_func" : None} # will use default one
    DEFAULT_DATAUPLOAD_CONFIG = {"upload_root" : getattr(config,"DATA_UPLOAD_FOLDER",None)}
    DEFAULT_WEBSOCKET_CONFIG = {}
    DEFAULT_API_CONFIG = {}

    def __init__(self, source_list, features=None, name="BioThings Hub",
                 managers_custom_args={}, api_config=None, reloader_config=None,
                 dataupload_config=None, websocket_config=None):
        """
        Helper to setup and instantiate common managers usually used in a hub
        (eg. dumper manager, uploader manager, etc...)
        "source_list" is either:
            - a list of string corresponding to paths to datasources modules
            - a package containing sub-folders with datasources modules
        Specific managers can be retrieved adjusting "features" parameter, where
        each feature corresponds to one or more managers. Parameter defaults to
        all possible available.
        "managers_custom_args" is an optional dict used to pass specific arguments while
        init managers:
            managers_custom_args={"upload" : {"poll_schedule" : "*/5 * * * *"}}
        will set poll schedule to check upload every 5min (instead of default 10s)
        "reloader_config", "dataupload_config" and "websocket_config" can be used to
        customize reloader, dataupload and websocket. If None, default config is used.
        If explicitely False, feature is deactivated.
        """
        self.name = name
        self.source_list = source_list
        self.logger, self.logfile = get_logger("hub")
        self._passed_features = features
        self._passed_managers_custom_args = managers_custom_args
        self.features = features or self.DEFAULT_FEATURES
        self.managers_custom_args = managers_custom_args or self.DEFAULT_MANAGERS_ARGS
        if reloader_config == False:
            self.logger.debug("Reloader deactivated")
            self.reloader_config = False
        else:
            self.reloader_config = reloader_config or self.DEFAULT_RELOADER_CONFIG
        if dataupload_config == False:
            self.logger.debug("Data upload deactivated")
            self.dataupload_config = False
        else:
            self.dataupload_config = dataupload_config or self.DEFAULT_DATAUPLOAD_CONFIG
        if websocket_config == False:
            self.logger.debug("Websocket deactivated")
            self.websocket_config = False
        else:
            self.websocket_config = websocket_config or self.DEFAULT_WEBSOCKET_CONFIG
        if api_config == False:
            self.logger.debug("API deactivated")
            self.api_config = False
        else:
            self.api_config = api_config or self.DEFAULT_API_CONFIG
        # set during configure()
        self.managers = None
        self.api_endpoints = None
        self.shell = None
        self.commands = None
        self.extra_commands = None
        self.routes = []
        # flag "do we need to configure?"
        self.configured = False

    def configure(self):
        self.configure_managers()
        self.configure_commands()
        self.configure_extra_commands()
        # setup the shell
        self.shell = HubShell(self.managers["job_manager"])
        self.shell.register_managers(self.managers)
        self.shell.set_commands(self.commands,self.extra_commands)
        self.shell.server = self # propagate server instance in shell
                                 # so it's accessible from the console if needed
        # set api
        if self.api_config != False:
            self.configure_api_endpoints() # after shell setup as it adds some default commands
                                           # we want to expose throught the api
            from biothings.hub.api import generate_api_routes
            self.routes = generate_api_routes(self.shell, self.api_endpoints)

        if self.dataupload_config != False:
            # this one is not bound to a specific command
            from biothings.hub.api.handlers.upload import UploadHandler
            # tuple type = interpreted as a route handler
            self.routes.append(("/dataupload/([\w\.-]+)?",UploadHandler,self.dataupload_config))

        if self.websocket_config != False:
            # add websocket endpoint
            import biothings.hub.api.handlers.ws as ws
            import sockjs.tornado
            from biothings.utils.hub_db import ChangeWatcher
            listener = ws.HubDBListener()
            ChangeWatcher.add(listener)
            ChangeWatcher.publish()
            ws_router = sockjs.tornado.SockJSRouter(partial(ws.WebSocketConnection,listener=listener), '/ws')
            self.routes.extend(ws_router.urls)

        if self.reloader_config != False:
            monitored_folders = self.reloader_config["folders"] or ["hub/dataload/sources",getattr(config,"DATA_PLUGIN_FOLDER",None)]
            reload_managers = [self.managers[m] for m in self.reloader_config["managers"]]
            reload_func = self.reloader_config["reload_func"] or partial(self.shell.restart,force=True)
            reloader = HubReloader(monitored_folders, reload_managers, reload_func=reload_func)
            reloader.monitor()

        # done
        self.configured = True

    def start(self):
        if not self.configured:
            self.configure()
        self.logger.info("Starting server '%s'" % self.name)
        # can't use asyncio.get_event_loop() if python < 3.5.3 as it would return
        # another instance of aio loop, take it from job_manager to make sure
        # we share the same one
        loop = self.managers["job_manager"].loop

        if self.routes:
            import tornado.web
            import tornado.platform.asyncio
            tornado.platform.asyncio.AsyncIOMainLoop().install()
            # register app into current event loop
            api = tornado.web.Application(self.routes)
            self.extra_commands["api"] = api
            from biothings.hub.api import start_api
            api_server = start_api(api,config.HUB_API_PORT,settings=config.TORNADO_SETTINGS)
        else:
            self.logger.info("No route defined, API server won't start")
        self.server = start_server(loop,self.name,passwords=config.HUB_PASSWD,
                              port=config.HUB_SSH_PORT,shell=self.shell)
        try:
            loop.run_until_complete(self.server)
        except (OSError, asyncssh.Error) as exc:
            sys.exit('Error starting server: ' + str(exc))
        loop.run_forever()


    def configure_managers(self):

        def mixargs(feat,params={}):
            args = {}
            for p in params:
                args[p] = self.managers_custom_args.get(feat,{}).pop(p,None) or params[p]
            # mix remaining
            args.update(self.managers_custom_args.get(feat,{}))
            return args

        self.managers = {}

        self.logger.info("Setting up managers for following features: %s" % self.features)
        assert "job" in self.features, "'job' feature is mandatory"

        if "job" in self.features:
            import asyncio
            loop = asyncio.get_event_loop()
            from biothings.utils.manager import JobManager
            args = mixargs("job",{"num_workers":config.HUB_MAX_WORKERS,"max_memory_usage":config.HUB_MAX_MEM_USAGE})
            job_manager = JobManager(loop,**args)
            self.managers["job_manager"] = job_manager
        if "dump" in self.features:
            from biothings.hub.dataload.dumper import DumperManager
            args = mixargs("dump")
            dmanager = DumperManager(job_manager=self.managers["job_manager"],**args)
            self.managers["dump_manager"] = dmanager
        if "upload" in self.features:
            from biothings.hub.dataload.uploader import UploaderManager
            args = mixargs("upload",{"poll_schedule":"* * * * * */10"})
            upload_manager = UploaderManager(job_manager=self.managers["job_manager"],**args)
            self.managers["upload_manager"] = upload_manager
        if "dataplugin" in self.features:
            from biothings.hub.dataplugin.manager import DataPluginManager
            dp_manager = DataPluginManager(job_manager=self.managers["job_manager"])
            self.managers["dataplugin_manager"] = dp_manager
            from biothings.hub.dataplugin.assistant import AssistantManager
            args = mixargs("dataplugin")
            assistant_manager = AssistantManager(
                    data_plugin_manager=dp_manager,
                    dumper_manager=self.managers["dump_manager"],
                    uploader_manager=self.managers["upload_manager"],
                    job_manager=self.managers["job_manager"],
                    **args)
            self.managers["assistant_manager"] = assistant_manager
        if "build" in self.features:
            from biothings.hub.databuild.builder import BuilderManager
            args = mixargs("build")
            build_manager = BuilderManager(job_manager=self.managers["job_manager"],**args)
            build_manager.configure()
            self.managers["build_manager"] = build_manager
        if "diff" in self.features:
            from biothings.hub.databuild.differ import DifferManager, SelfContainedJsonDiffer
            args = mixargs("diff")
            diff_manager = DifferManager(job_manager=self.managers["job_manager"],**args)
            diff_manager.configure([SelfContainedJsonDiffer,])
            self.managers["diff_manager"] = diff_manager
        if "index" in self.features:
            from biothings.hub.dataindex.indexer import IndexerManager
            args = mixargs("index")
            index_manager = IndexerManager(job_manager=self.managers["job_manager"],**args)
            index_manager.configure(config.ES_CONFIG)
            self.managers["index_manager"] = index_manager
        if "sync" in self.features:
            from biothings.hub.databuild.syncer import ThrottledESJsonDiffSelfContainedSyncer, \
                    ESJsonDiffSelfContainedSyncer, SyncerManager
            args = mixargs("sync")
            sync_manager = SyncerManager(job_manager=self.managers["job_manager"],**args)
            self.managers["sync_manager"] = sync_manager
        if "inspect" in self.features:
            assert "upload" in self.features, "'inspect' feature requires 'upload'"
            assert "build" in self.features, "'inspect' feature requires 'build'"
            from biothings.hub.datainspect.inspector import InspectorManager
            args = mixargs("inspect")
            inspect_manager = InspectorManager(
                    upload_manager=self.managers["upload_manager"],
                    build_manager=self.managers["build_manager"],
                    job_manager=self.managers["job_manager"],**args)
            self.managers["inspect_manager"] = inspect_manager
        if "api" in self.features:
            from biothings.hub.api.manager import APIManager
            args = mixargs("api")
            api_manager = APIManager(**args)
            self.managers["api_manager"] = api_manager
        if "dump" in self.features or "upload" in self.features:
            args = mixargs("source")
            from biothings.hub.dataload.source import SourceManager
            source_manager = SourceManager(
                    source_list=self.source_list,
                    dump_manager=self.managers["dump_manager"],
                    upload_manager=self.managers["upload_manager"],
                    data_plugin_manager=self.managers.get("dataplugin_manager"),
                    )
            self.managers["source_manager"] = source_manager
            # now that we have the source manager setup, we can schedule and poll
            if "dump" in self.features and not getattr(config,"SKIP_DUMPER_SCHEDULE",False):
                self.managers["dump_manager"].schedule_all()
            if "upload" in self.features and not getattr(config,"SKIP_UPLOADER_POLL",False):
                self.managers["upload_manager"].poll('upload',lambda doc:
                        self.shell.launch(partial(upload_manager.upload_src,doc["_id"])))
        # init data plugin once source_manager has been set (it inits dumper and uploader
        # managers, if assistant_manager is configured/loaded before, datasources won't appear
        # in dumper/uploader managers as they were not ready yet)
        if "dataplugin" in self.features:
            self.managers["assistant_manager"].configure()
            self.managers["assistant_manager"].load()

        self.logger.info("Active manager(s): %s" % pformat(self.managers))

    def configure_commands(self):
        """
        Configure hub commands according to available managers
        """
        assert self.managers, "No managers configured"
        self.commands = OrderedDict()
        self.commands["status"] = CommandDefinition(command=partial(status,self.managers),tracked=False)
        # getting info
        if self.managers.get("source_manager"):
            self.commands["source_info"] = CommandDefinition(command=self.managers["source_manager"].get_source,tracked=False)
        # dump commands
        if self.managers.get("dump_manager"):
            self.commands["dump"] = self.managers["dump_manager"].dump_src
            self.commands["dump_all"] = self.managers["dump_manager"].dump_all
        # upload commands
        if self.managers.get("upload_manager"):
            self.commands["upload"] = self.managers["upload_manager"].upload_src
            self.commands["upload_all"] = self.managers["upload_manager"].upload_all
        # building/merging
        if self.managers.get("build_manager"):
            self.commands["whatsnew"] = CommandDefinition(command=self.managers["build_manager"].whatsnew,tracked=False)
            self.commands["lsmerge"] = self.managers["build_manager"].list_merge
            self.commands["rmmerge"] = self.managers["build_manager"].delete_merge
            self.commands["merge"] = self.managers["build_manager"].merge
        if hasattr(config,"ES_CONFIG"):
            self.commands["es_config"] = config.ES_CONFIG
        # diff
        if self.managers.get("diff_manager"):
            self.commands["diff"] = self.managers["diff_manager"].diff
            self.commands["report"] = self.managers["diff_manager"].diff_report
            self.commands["release_note"] = self.managers["diff_manager"].release_note
            self.commands["publish_diff"] = self.managers["diff_manager"].publish_diff
        # indexing commands
        if self.managers.get("index_manager"):
            self.commands["index"] = self.managers["index_manager"].index
            self.commands["snapshot"] = self.managers["index_manager"].snapshot
            self.commands["publish_snapshot"] = self.managers["index_manager"].publish_snapshot
        # inspector
        if self.managers.get("inspect_manager"):
            self.commands["inspect"] = self.managers["inspect_manager"].inspect
        # data plugins
        if self.managers.get("assistant_manager"):
            self.commands["register_url"] = partial(self.managers["assistant_manager"].register_url)
            self.commands["unregister_url"] = partial(self.managers["assistant_manager"].unregister_url)
        if self.managers.get("dataplugin_manager"):
            self.commands["dump_plugin"] = self.managers["dataplugin_manager"].dump_src

        logging.info("Registered commands: %s" % list(self.commands.keys()))

    def configure_extra_commands(self):
        """
        Same as configure_commands() but commands are not exposed publicly in the shell
        (they are shortcuts or commands for API endpoints, supporting commands, etc...)
        """
        assert self.managers, "No managers configured"
        self.extra_commands = {} # unordered since not exposed, we don't care
        loop = self.managers.get("job_manager") and self.managers["job_manager"].loop or asyncio.get_event_loop()
        self.extra_commands["g"] = CommandDefinition(command=globals(),tracked=False)
        self.extra_commands["sch"] = CommandDefinition(command=partial(schedule,loop),tracked=False)
        # expose contant so no need to put quotes (eg. top(pending) instead of top("pending")
        self.extra_commands["pending"] = CommandDefinition(command=pending,tracked=False)
        self.extra_commands["done"] = CommandDefinition(command=done,tracked=False)
        self.extra_commands["loop"] = CommandDefinition(command=loop,tracked=False)

        if self.managers.get("source_manager"):
            self.extra_commands["sources"] = CommandDefinition(command=self.managers["source_manager"].get_sources,tracked=False)
            self.extra_commands["source_save_mapping"] = CommandDefinition(command=self.managers["source_manager"].save_mapping)
        if self.managers.get("dump_manager"):
            self.extra_commands["dm"] = CommandDefinition(command=self.managers["dump_manager"],tracked=False)
            self.extra_commands["dump_info"] = CommandDefinition(command=self.managers["dump_manager"].dump_info,tracked=False)
        if self.managers.get("dataplugin_manager"):
            self.extra_commands["dpm"] = CommandDefinition(command=self.managers["dataplugin_manager"],tracked=False)
        if self.managers.get("assistant_manager"):
            self.extra_commands["am"] = CommandDefinition(command=self.managers["assistant_manager"],tracked=False)
        if self.managers.get("upload_manager"):
            self.extra_commands["um"] = CommandDefinition(command=self.managers["upload_manager"],tracked=False)
            self.extra_commands["upload_info"] = CommandDefinition(command=self.managers["upload_manager"].upload_info,tracked=False)
        if self.managers.get("build_manager"):
            self.extra_commands["bm"] = CommandDefinition(command=self.managers["build_manager"],tracked=False)
            self.extra_commands["builds"] = CommandDefinition(command=self.managers["build_manager"].build_info,tracked=False)
            self.extra_commands["build"] = CommandDefinition(command=lambda id: self.managers["build_manager"].build_info(id=id),tracked=False)
            self.extra_commands["build_config_info"] = CommandDefinition(command=self.managers["build_manager"].build_config_info,tracked=False)
            self.extra_commands["build_save_mapping"] = CommandDefinition(command=self.managers["build_manager"].save_mapping)
            self.extra_commands["create_build_conf"] = CommandDefinition(command=self.managers["build_manager"].create_build_configuration)
            self.extra_commands["delete_build_conf"] = CommandDefinition(command=self.managers["build_manager"].delete_build_configuration)
        if self.managers.get("diff_manager"):
            self.extra_commands["dim"] = CommandDefinition(command=self.managers["diff_manager"],tracked=False)
            self.extra_commands["diff_info"] = CommandDefinition(command=self.managers["diff_manager"].diff_info,tracked=False)
            self.extra_commands["jsondiff"] = CommandDefinition(command=jsondiff,tracked=False)
        if self.managers.get("sync_manager"):
            self.extra_commands["sm"] = CommandDefinition(command=self.managers["sync_manager"],tracked=False)
            self.extra_commands["sync"] = CommandDefinition(command=self.managers["sync_manager"].sync)
        if self.managers.get("index_manager"):
            self.extra_commands["im"] = CommandDefinition(command=self.managers["index_manager"],tracked=False)
            self.extra_commands["index_info"] = CommandDefinition(command=self.managers["index_manager"].index_info,tracked=False)
            self.extra_commands["validate_mapping"] = CommandDefinition(command=self.managers["index_manager"].validate_mapping)
            self.extra_commands["pqueue"] = CommandDefinition(command=self.managers["job_manager"].process_queue,tracked=False)
            self.extra_commands["tqueue"] = CommandDefinition(command=self.managers["job_manager"].thread_queue,tracked=False)
            self.extra_commands["jm"] = CommandDefinition(command=self.managers["job_manager"],tracked=False)
            self.extra_commands["top"] = CommandDefinition(command=self.managers["job_manager"].top,tracked=False)
            self.extra_commands["job_info"] = CommandDefinition(command=self.managers["job_manager"].job_info,tracked=False)
        if self.managers.get("inspect_manager"):
            self.extra_commands["ism"] = CommandDefinition(command=self.managers["inspect_manager"],tracked=False)
        if self.managers.get("api_manager"):
            self.extra_commands["api"] = CommandDefinition(command=self.managers["api_manager"],tracked=False)
            self.extra_commands["get_apis"] = CommandDefinition(command=self.managers["api_manager"].get_apis,tracked=False)
            self.extra_commands["delete_api"] = CommandDefinition(command=self.managers["api_manager"].delete_api)
            self.extra_commands["create_api"] = CommandDefinition(command=self.managers["api_manager"].create_api)
            self.extra_commands["start_api"] = CommandDefinition(command=self.managers["api_manager"].start_api)
            self.extra_commands["stop_api"] = self.managers["api_manager"].stop_api

        logging.debug("Registered extra (private) commands: %s" % list(self.extra_commands.keys()))

    def configure_api_endpoints(self):
        cmdnames = list(self.commands.keys())
        if self.extra_commands:
            cmdnames.extend(list(self.extra_commands.keys()))
        from biothings.hub.api import EndpointDefinition
        self.api_endpoints = {}
        if "builds" in cmdnames: self.api_endpoints["builds"] = EndpointDefinition(name="builds",method="get")
        self.api_endpoints["build"] = []
        if "build" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(method="get",name="build"))
        if "rmmerge" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(method="delete",name="rmmerge"))
        if "merge" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(name="merge",method="put",suffix="new"))
        if "build_save_mapping" in cmdnames: self.api_endpoints["build"].append(EndpointDefinition(name="build_save_mapping",method="put",suffix="mapping"))
        if not self.api_endpoints["build"]:
            self.api_endpoints.pop("build")
        if "diff" in cmdnames: self.api_endpoints["diff"] = EndpointDefinition(name="diff",method="put",force_bodyargs=True)
        if "job_info" in cmdnames: self.api_endpoints["job_manager"] = EndpointDefinition(name="job_info",method="get")
        if "dump_info" in cmdnames: self.api_endpoints["dump_manager"] = EndpointDefinition(name="dump_info", method="get")
        if "upload_info" in cmdnames: self.api_endpoints["upload_manager"] = EndpointDefinition(name="upload_info",method="get")
        if "build_config_info" in cmdnames: self.api_endpoints["build_manager"] = EndpointDefinition(name="build_config_info",method="get")
        if "index_info" in cmdnames: self.api_endpoints["index_manager"] = EndpointDefinition(name="index_info",method="get")
        if "diff_info" in cmdnames: self.api_endpoints["diff_manager"] = EndpointDefinition(name="diff_info",method="get")
        if "commands" in cmdnames: self.api_endpoints["commands"] = EndpointDefinition(name="commands",method="get")
        if "command" in cmdnames: self.api_endpoints["command"] = EndpointDefinition(name="command",method="get")
        if "sources" in cmdnames: self.api_endpoints["sources"] = EndpointDefinition(name="sources",method="get")
        self.api_endpoints["source"] = []
        if "source_info" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="source_info",method="get"))
        if "dump" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="dump",method="put",suffix="dump"))
        if "upload" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="upload",method="put",suffix="upload"))
        if "source_save_mapping" in cmdnames: self.api_endpoints["source"].append(EndpointDefinition(name="source_save_mapping",method="put",suffix="mapping"))
        if not self.api_endpoints["source"]:
            self.api_endpoints.pop("source")
        if "inspect" in cmdnames: self.api_endpoints["inspect"] = EndpointDefinition(name="inspect",method="put",force_bodyargs=True)
        if "register_url" in cmdnames: self.api_endpoints["dataplugin/register_url"] = EndpointDefinition(name="register_url",method="post",force_bodyargs=True)
        if "unregister_url" in cmdnames: self.api_endpoints["dataplugin/unregister_url"] = EndpointDefinition(name="unregister_url",method="delete",force_bodyargs=True)
        if "dump_plugin" in cmdnames: self.api_endpoints["dataplugin"] = [EndpointDefinition(name="dump_plugin",method="put",suffix="dump")]
        if "jsondiff" in cmdnames: self.api_endpoints["jsondiff"] = EndpointDefinition(name="jsondiff",method="post",force_bodyargs=True)
        if "validate_mapping" in cmdnames: self.api_endpoints["mapping/validate"] = EndpointDefinition(name="validate_mapping",method="post",force_bodyargs=True)
        self.api_endpoints["buildconf"] = []
        if "create_build_conf" in cmdnames: self.api_endpoints["buildconf"].append(EndpointDefinition(name="create_build_conf",method="post",force_bodyargs=True))
        if "delete_build_conf" in cmdnames: self.api_endpoints["buildconf"].append(EndpointDefinition(name="delete_build_conf",method="delete",force_bodyargs=True))
        if not self.api_endpoints["buildconf"]:
            self.api_endpoints.pop("buildconf")
        if "index" in cmdnames: self.api_endpoints["index"] = EndpointDefinition(name="index",method="put",force_bodyargs=True)
        if "sync" in cmdnames: self.api_endpoints["sync"] = EndpointDefinition(name="sync",method="post",force_bodyargs=True)
        if "whatsnew" in cmdnames: self.api_endpoints["whatsnew"] = EndpointDefinition(name="whatsnew",method="get")
        if "status" in cmdnames: self.api_endpoints["status"] = EndpointDefinition(name="status",method="get")
        self.api_endpoints["api"] = []
        if "start_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="start_api",method="put",suffix="start"))
        if "stop_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="stop_api",method="put",suffix="stop"))
        if "delete_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="delete_api",method="delete",force_bodyargs=True))
        if "create_api" in cmdnames: self.api_endpoints["api"].append(EndpointDefinition(name="create_api",method="post",force_bodyargs=True))
        if not self.api_endpoints["api"]:
            self.api_endpoints.pop("api")
        if "get_apis" in cmdnames: self.api_endpoints["api/list"] = EndpointDefinition(name="get_apis",method="get")
        if "stop" in cmdnames: self.api_endpoints["stop"] = EndpointDefinition(name="stop",method="put")
        if "restart" in cmdnames: self.api_endpoints["restart"] = EndpointDefinition(name="restart",method="put")


