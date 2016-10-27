# from http://asyncssh.readthedocs.io/en/latest/#id13

# To run this program, the file ``ssh_host_key`` must exist with an SSH
# private key in it to use as a server host key.


import asyncio, asyncssh, crypt, sys, io
from IPython import InteractiveShell


class HubSSHServerSession(asyncssh.SSHServerSession):
    def __init__(self, commands):
        self.shell = InteractiveShell(user_ns=commands)
        self._input = ''

    def connection_made(self, chan):
        self._chan = chan

        self.origout = sys.stdout
        self.buf = io.StringIO()
        sys.stdout = self.buf

    def shell_requested(self):
        return True

    def session_started(self):
        self._chan.write('Welcome to MyVariant hub, %s!\n' %
                          self._chan.get_extra_info('username'))
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

    COMMANDS = globals()
    PASSWORDS = {}

    def session_requested(self):
        return HubSSHServerSession(self.__class__.COMMANDS)

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

async def start_server(passwords,key='bin/ssh_host_key',host='',port=8022,commands={}):
    HubSSHServer.PASSWORDS = passwords
    if commands:
        HubSSHServer.COMMANDS = commands
    await asyncssh.create_server(HubSSHServer, '', 8022, server_host_keys=['bin/ssh_host_key'])


