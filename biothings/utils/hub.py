# from http://asyncssh.readthedocs.io/en/latest/#id13

# To run this program, the file ``ssh_host_key`` must exist with an SSH
# private key in it to use as a server host key.


import asyncio, asyncssh, crypt, sys, io
import types, aiocron, time
from functools import partial
from IPython import InteractiveShell


##############
# HUB SERVER #
##############

class HubSSHServerSession(asyncssh.SSHServerSession):
    def __init__(self, name, commands):
        self.shell = InteractiveShell(user_ns=commands)
        self.name = name
        self._input = ''

    def connection_made(self, chan):
        self._chan = chan
        self.origout = sys.stdout
        self.buf = io.StringIO()
        sys.stdout = self.buf

    def shell_requested(self):
        return True

    def session_started(self):
        self._chan.write('Welcome to %s, %s!\n' % (self.name,self._chan.get_extra_info('username')))
        self._chan.write('hub> ')

    def data_received(self, data, datatype):
        self._input += data

        lines = self._input.split('\n')
        for line in lines[:-1]:
            if not line:
                continue
            self.origout.write("run %s " % repr(line))
            r = self.shell.run_code(line)
            if r == 1:
                self.origout.write("Error\n")
                etype, value, tb = self.shell._get_exc_info(None)
                self._chan.write("Error: %s\n" % value)
            else:
                #self.origout.write(self.buf.read() + '\n')
                self.origout.write("OK\n")
                self.buf.seek(0)
                self._chan.write(self.buf.read())
                # clear buffer
                self.buf.seek(0)
                self.buf.truncate()
        self._chan.write('hub> ')
        self._input = lines[-1]

    def eof_received(self):
        self._chan.write('Have a good one...\n')
        self._chan.exit(0)

    def break_received(self, msec):
        # simulate CR
        self._chan.write('\n')
        self.data_received("\n",None)


class HubSSHServer(asyncssh.SSHServer):

    COMMANDS = {}
    PASSWORDS = {}

    def session_requested(self):
        return HubSSHServerSession(self.__class__.NAME,
                                   self.__class__.COMMANDS)

    def connection_made(self, conn):
        print('SSH connection received from %s.' %
                  conn.get_extra_info('peername')[0])

    def connection_lost(self, exc):
        if exc:
            print('SSH connection error: ' + str(exc), file=sys.stderr)
        else:
            print('SSH connection closed.')

    def begin_auth(self, username):
        # If the user's password is the empty string, no auth is required
        return self.__class__.PASSWORDS.get(username) != ''

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        pw = self.__class__.PASSWORDS.get(username, '*')
        return crypt.crypt(password, pw) == pw


async def start_server(loop,name,passwords,keys=['bin/ssh_host_key'],
                        host='',port=8022,commands={}):
    HubSSHServer.PASSWORDS = passwords
    HubSSHServer.NAME = name
    if commands:
        HubSSHServer.COMMANDS.update(commands)
    await asyncssh.create_server(HubSSHServer, host, port, loop=loop,
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
        r = self.rendered.get(type(job._callback))#,print)
        rstr = r(job._callback)
        delta = job._when - job._loop.time()
        strdelta = time.strftime("%Hh:%Mm:%Ss", time.gmtime(int(delta)))
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
    for sch in loop._scheduled:
        if type(sch) != asyncio.events.TimerHandle:
            continue
        if sch._cancelled:
            continue
        try:
            info = renderer.render(sch)
            print(info)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(sch)

def top(executor):
    pass

def stats(src_dump):
    pass

