# from http://asyncssh.readthedocs.io/en/latest/#id13

# To run this program, the file ``ssh_host_key`` must exist with an SSH
# private key in it to use as a server host key.

import os, glob, re, pickle, datetime, json, pydoc, copy
import asyncio, asyncssh, crypt, sys, io, inspect
import types, aiocron, time
from functools import partial
from IPython import InteractiveShell
import psutil
from pprint import pprint, pformat
from collections import OrderedDict
import pymongo
import pyinotify

from biothings import config
from biothings.utils.dataload import to_boolean
logging = config.logger
from biothings.utils.common import timesofar, sizeof_fmt
import biothings.utils.aws as aws
from biothings.utils.hub_db import get_cmd

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

    def __init__(self):
        self.commands = OrderedDict()
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
            cur = klass.cmd.find({},{"_id":1}).sort("_id",pymongo.DESCENDING).limit(1)
            res = next(cur)
            logging.debug("Last launched command ID: %s" % res["_id"])
            klass.cmd_cnt = int(res["_id"]) + 1
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
        # update with ssh server default commands
        register(basic_commands)
        # don't track help calls
        register({"help":CommandDefinition(command=self.help,tracked=False)})

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
                        host='',port=8022,commands={},extra_ns={}):
    for key in keys:
        assert os.path.exists(key),"Missing key '%s' (use: 'ssh-keygen -f %s' to generate it" % (key,key)
    HubSSHServer.PASSWORDS = passwords
    HubSSHServer.NAME = name
    HubSSHServer.SHELL = shell or HubShell(commands,extra_ns)
    cron = aiocron.crontab(HUB_REFRESH_COMMANDS,func=shell.__class__.refresh_commands,
                           start=True, loop=loop)
    if commands:
        HubSSHServer.COMMANDS.update(commands)
    if extra_ns:
        HubSSHServer.EXTRA_NS.update(extra_ns)
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
        

def find_process(pid):
    g = psutil.process_iter()
    for p in g:
        if p.pid == pid:
            break
    return p


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
    def my_init(self,managers,watcher_manager):
        logging.debug("for managers %s" % managers)
        self.managers = managers
        self.watcher_manager = watcher_manager
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
        for m in self.managers:
            try:
                logging.debug("Reloading manager %s" % m)
                m.reload()
            except Exception as e:
                logging.warning("Can't reload %s: %s" % (m,e))

class HubReloader(object):
    """
    Monitor sources' code and reload managers accordingly to update running code
    """

    def __init__(self,paths,managers=[],wait=5,mask=None):
        """
        Monitor given paths for directory deletion/creation (which will update pyinotify watchers)
        and for file deletion/creation. Reload given managers accordingly.
        Poll for events every 'wait' seconds.
        """
        if type(paths) == str:
            paths = [paths]
        paths = set(paths) # get rid of duplicated, just in case
        self.mask = mask or pyinotify.IN_CREATE|pyinotify.IN_DELETE|pyinotify.IN_CLOSE_WRITE
        # only listen to these events. Note: directory detection is done via a flag so
        # no need to use IS_DIR
        self.watcher_manager = pyinotify.WatchManager(exclude_filter=exclude_from_reloader)
        _paths = [] # cleaned
        for path in paths:
            if not os.path.isabs(path):
                path = os.path.abspath(path)
            _paths.append(path)
            self.watcher_manager.add_watch(path,self.mask,rec=True,exclude_filter=exclude_from_reloader) # recursive
        self.listener = ReloadListener(managers=managers,watcher_manager=self.watcher_manager)
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
        self.do_monitor = True
        return asyncio.ensure_future(do())

    def watches(self):
        return [v.path for v in self.watcher_manager.watches.values()]
