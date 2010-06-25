#!/usr/bin/env python
#
# Copyright 2010 Dr. Masroor Ehsan Choudhury
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import ioloop, iostream
import logging, os, socket, types, re, sys, errno
import time
import threading

try:
    import fcntl
except ImportError:
    if os.name == 'nt':
        import win32_support as fcntl
    else:
        raise

# Cache the hostname (XXX Yes - this is broken)
HOST_NAME = socket.gethostname() if sys.platform == 'darwin' else socket.getfqdn()

ALLOW, DENY, DENY_DISCONNECT, DENYSOFT, DENYSOFT_DISCONNECT, DONE = range(6)

def uniq_id():
    i = 100
    while True:
        yield i
        i += 1

########################################################################
class ServerWatchdog:
    #----------------------------------------------------------------------
    def check_access(self, peer_addr):
        """Return ALLOW if peer_addr is allowed, else DENY"""
        packed_ip = socket.inet_aton(peer_addr)
        return ALLOW  

    
########################################################################
class MessageDelivery(object):
    #----------------------------------------------------------------------
    def begin_session(self, helo, peer_ip):
        """
        Return a unique session id
        """
        return None
    
    #----------------------------------------------------------------------
    def reset_session(self, session_token):
        """"""
        pass
    
    #----------------------------------------------------------------------    
    def verify_recipient(self, session_token, user):
        """
        Validate the address for which the message is destined.

        @type user: C{SMTPUser}
        @param user: The address to validate.

        @rtype: no-argument callable
        @return: A C{Deferred} which becomes, or a callable which
        takes no arguments and returns an object implementing C{IMessage}.
        This will be called and the returned object used to deliver the
        message when it arrives.

        @raise SMTPBadRcpt: Raised if messages to the address are
        not to be accepted.
        
        return CODE
        """
        pass

    #----------------------------------------------------------------------    
    def validate_recipient(self, session_token, mailfrom, rcptto):
        """
        Validate the address for which the message is destined.

        @type user: C{SMTPUser}
        @param user: The address to validate.

        @rtype: no-argument callable
        @return: A C{Deferred} which becomes, or a callable which
        takes no arguments and returns an object implementing C{IMessage}.
        This will be called and the returned object used to deliver the
        message when it arrives.

        @raise SMTPBadRcpt: Raised if messages to the address are
        not to be accepted.
        
        return (CODE, EmailAddress)
        """
        pass

    #----------------------------------------------------------------------    
    def validate_sender(self, session_token, helo, mailfrom):
        """
        Validate the address from which the message originates.

        @type helo: C{(str, str)}
        @param helo: The argument to the HELO command and the client's IP
        address.

        @type origin: C{EmailAddress}
        @param origin: The address the message is from

        @rtype: C{Deferred} or C{EmailAddress}
        @return: C{origin} or a C{Deferred} whose callback will be
        passed C{origin}.

        @raise SMTPBadSender: Raised of messages from this address are
        not to be accepted.
        
        # return CODE, + EmailAddress
        """
        pass
    
    # Do something with the gathered message
    # return CODE[, Message]
    def message_received(self, session_token, mailfrom, rcpttos, data):
        pass
    

########################################################################
class MessageDeliveryFactory(object):
    """An alternate interface to implement for handling message delivery.

    It is useful to implement this interface instead of L{IMessageDelivery}
    directly because it allows the implementor to distinguish between
    different messages delivery over the same connection.  This can be
    used to optimize delivery of a single message to multiple recipients,
    something which cannot be done by L{IMessageDelivery} implementors
    due to their lack of information.
    """
    #----------------------------------------------------------------------    
    def getMessageDelivery(self):
        """Return an L{IMessageDelivery} object.

        This will be called once per message.
        """
        return None
    
########################################################################       

class AddressError(Exception):
    "Parse error in address"

# Character classes for parsing addresses
atom = r"[-A-Za-z0-9!\#$%&'*+/=?^_`{|}~]"

class EmailAddress:
    """Parse and hold an RFC 2821 address.

    Source routes are stipped and ignored, UUCP-style bang-paths
    and %-style routing are not parsed.

    @type domain: C{str}
    @ivar domain: The domain within which this address resides.

    @type local: C{str}
    @ivar local: The local (\"user\") portion of this address.
    """

    tstring = re.compile(r'''( # A string of
                          (?:"[^"]*" # quoted string
                          |\\. # backslash-escaped characted
                          |''' + atom + r''' # atom character
                          )+|.) # or any single character''',re.X)
    atomre = re.compile(atom) # match any one atom character

    def __init__(self, addr, defaultDomain=None):
        if isinstance(addr, EmailAddress):
            self.__dict__ = addr.__dict__.copy()
            return
        elif not isinstance(addr, types.StringTypes):
            addr = str(addr)
        self.addrstr = addr

        # Tokenize
        atl = filter(None,self.tstring.split(addr))

        local = []
        domain = []

        while atl:
            if atl[0] == '<':
                if atl[-1] != '>':
                    raise AddressError, "Unbalanced <>"
                atl = atl[1:-1]
            elif atl[0] == '@':
                atl = atl[1:]
                if not local:
                    # Source route
                    while atl and atl[0] != ':':
                        # remove it
                        atl = atl[1:]
                    if not atl:
                        raise AddressError, "Malformed source route"
                    atl = atl[1:] # remove :
                elif domain:
                    raise AddressError, "Too many @"
                else:
                    # Now in domain
                    domain = ['']
            elif len(atl[0]) == 1 and not self.atomre.match(atl[0]) and atl[0] !=  '.':
                raise AddressError, "Parse error at %r of %r" % (atl[0], (addr, atl))
            else:
                if not domain:
                    local.append(atl[0])
                else:
                    domain.append(atl[0])
                atl = atl[1:]

        self.local = ''.join(local)
        self.domain = ''.join(domain)
        if self.local != '' and self.domain == '':
            if defaultDomain is None:
                defaultDomain = HOST_NAME
            self.domain = defaultDomain

    dequotebs = re.compile(r'\\(.)')

    def dequote(self,addr):
        """Remove RFC-2821 quotes from address."""
        res = []

        atl = filter(None,self.tstring.split(str(addr)))

        for t in atl:
            if t[0] == '"' and t[-1] == '"':
                res.append(t[1:-1])
            elif '\\' in t:
                res.append(self.dequotebs.sub(r'\1',t))
            else:
                res.append(t)

        return ''.join(res)

    def __str__(self):
        if self.local or self.domain:
            return '@'.join((self.local, self.domain))
        else:
            return ''

    def __repr__(self):
        return "%s.%s(%s)" % (self.__module__, self.__class__.__name__,
                              repr(str(self)))

########################################################################
class SMTPServer(object):
    """SMTP Server"""

    #----------------------------------------------------------------------
    def __init__(self, io_loop=None, watchdog=None, delivery=None, delivery_factory=None, num_processes=1):
        """Initializes the server with the given request callback.

        If you use pre-forking/start() instead of the listen() method to
        start your server, you should not pass an IOLoop instance to this
        constructor. Each pre-forked child process will create its own
        IOLoop instance after the forking process.
        """
        self.io_loop = io_loop
        self.watchdog = watchdog
        self._socket = None
        self._started = False 
        self.delivery = delivery
        self.delivery_factory = delivery_factory
        self._num_processes = num_processes

    def listen(self, port, address=""):
        """Binds to the given port and starts the server in a single process."""
        self.bind(port, address)
        self.start(self._num_processes)

    def bind(self, port, address=""):
        """Binds this server to the given port on the given IP address.

        To start the server, call start(). If you want to run this server
        in a single process, you can call listen() as a shortcut to the
        sequence of bind() and start() calls.
        """
        assert not self._socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        flags = fcntl.fcntl(self._socket.fileno(), fcntl.F_GETFD)
        flags |= fcntl.FD_CLOEXEC
        fcntl.fcntl(self._socket.fileno(), fcntl.F_SETFD, flags)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setblocking(0)
        self._socket.bind((address, port))
        self._socket.listen(128)

    def start(self, num_processes=None):
        """Starts this server in the IOLoop.

        By default, we detect the number of cores available on this machine
        and fork that number of child processes. If num_processes is given, we
        fork that specific number of sub-processes.

        If num_processes is 1 or we detect only 1 CPU core, we run the server
        in this process and do not fork any additional child process.

        Since we run use processes and not threads, there is no shared memory
        between any server code.
        """
        assert not self._started
        self._started = True
        
        if num_processes is None:
            # Use sysconf to detect the number of CPUs (cores)
            try:
                num_processes = os.sysconf("SC_NPROCESSORS_CONF")
            except ValueError:
                logging.error("Could not get num processors from sysconf; "
                              "running with one process")
                num_processes = 1
        if num_processes > 1 and ioloop.IOLoop.initialized():
            logging.error("Cannot run in multiple processes: IOLoop instance "
                          "has already been initialized. You cannot call "
                          "IOLoop.instance() before calling start()")
            num_processes = 1
        if num_processes > 1:
            logging.info("Pre-forking %d server processes", num_processes)
            for i in range(num_processes):
                if os.fork() == 0:
                    self.io_loop = ioloop.IOLoop.instance()
                    self.io_loop.add_handler(
                        self._socket.fileno(),
                        self._handle_accept,
                        ioloop.IOLoop.READ)
                    return
            os.waitpid(-1, 0)
        else:
            if not self.io_loop:
                self.io_loop = ioloop.IOLoop.instance()
            self.io_loop.add_handler(self._socket.fileno(),
                                     self._handle_accept,
                                     ioloop.IOLoop.READ)

    def stop(self):
        self.io_loop.remove_handler(self._socket.fileno())
        self._socket.close()

    def _handle_accept(self, fd, events):
        while True:
            try:
                sock, peer = self._socket.accept()
            except socket.error, e:
                if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    return
                raise
            if self.watchdog is not None:
                if self.watchdog(peer[0]) == DENY:
                    conn.close()
                    return
            try:
                stream = iostream.IOStream(sock, io_loop=self.io_loop)
                SMTPClientConnection(self, self.io_loop, stream, peer, self.delivery, self.delivery_factory)
            except:
                logging.error("Error in connection callback", exc_info=True)
    

COMMAND, DATA, AUTH = 'COMMAND', 'DATA', 'AUTH'

class SMTPClientConnection(object):
    """SMTP server-side protocol."""

    timeout_command = 30.0
    timeout_data = 30.0
    timeout_lifespan = 60.0
    fqdn = HOST_NAME
    TERM_EOL = '\r\n'
    TERM_EOM = '\r\n.\r\n'

    # A factory for IMessageDelivery objects.  If an
    # avatar implementing IMessageDeliveryFactory can
    # be acquired from the portal, it will be used to
    # create a new IMessageDelivery object for each
    # message which is received.
    delivery_factory = None

    # An IMessageDelivery object.  A new instance is
    # used for each message received if we can get an
    # IMessageDeliveryFactory from the portal.  Otherwise,
    # a single instance is used throughout the lifetime
    # of the connection.
    delivery = None
    
    def __init__(self, server, io_loop, stream, peer_addr, delivery=None, delivery_factory=None):
        self._server = server
        self._io_loop = io_loop
        self._stream = stream
        self.peer_ip = peer_addr[0]
        self.peer_port = peer_addr[1]
        self.delivery = delivery
        self.delivery_factory = delivery_factory
        self.mode = COMMAND
        self._from = None
        self._helo = None
        self._recipients = []
        self._pending_close = False
        
        self.__timeout_obj = None
        self.__timeout_id = None
        self.__timeout_lifespan = self._io_loop.add_timeout(self.timeout_lifespan + time.time(),
                                                            self.__timed_out_lifespan, 
                                                            None)
        self._session_token = None        
        
        self.send_greeting()
        self.post_read_request()
        
    #----------------------------------------------------------------------
    def get_terminator(self):
        return self.TERM_EOM if self.mode == DATA else self.TERM_EOL
    
    def __timed_out_lifespan(self, param):
        self.__timeout_lifespan = None
        self.reset_session()
        self.respond(421, "Game over pal! You just can't stay that long...")
        self.close()

    def send_greeting(self):
        self.respond(220, 'ESMTP %s ready; send us your mail, but not your spam.' % (self.fqdn,))
    
    def close(self):
        self._pending_close = True
        if not self._stream.writing():
            self._close_connection()

    def _close_connection(self):
        self.reset_timeout()
        if self.__timeout_lifespan is not None: 
            self._io_loop.remove_timeout(self.__timeout_lifespan)
        self.reset_session()
        self._pending_close = False
        self._stream.close()        
        
    def respond(self, status_code, message=''):
        "Send an SMTP code with a message."
        lines = message.splitlines()
        lastline, tmplines = lines[-1:], []
        for line in lines[:-1]:
            tmplines.append('%3.3d-%s' % (status_code, line))
        tmplines.append('%3.3d %s' % (status_code, lastline and lastline[0] or ''))
        self.write_line('\r\n'.join(tmplines))
        
    def write_line(self, message):
        if not self._stream.closed():
            self._stream.write(message + '\r\n', self._on_write_complete)
    
    def write(self, chunk):
        if not self._stream.closed():
            self._stream.write(chunk, self._on_write_complete)
    
    def _on_write_complete(self):
        self.reset_timeout()
        
        if self._pending_close:
            self._close_connection()
            return
    
    #----------------------------------------------------------------------
    def post_read_request(self, timeout=True):
        """"""
        if self._pending_close:
            return
        
        self._stream.read_until(self.get_terminator(), self._on_read_data)
        if timeout:
            self.set_timeout()
    
    def _on_read_data(self, data):
        #print '\t\t\t<<<%s' % data
        self.reset_timeout()
        ret = getattr(self, 'state_' + self.mode)(data)
        if ret != False:
            self.post_read_request()
    
    def state_COMMAND(self, line):
        # Ignore leading and trailing whitespace, as well as an arbitrary
        # amount of whitespace between the command and its argument, though
        # it is not required by the protocol, for it is a nice thing to do.
        line = line.strip()

        parts = line.split(None, 1)
        if parts:
            method = self.lookup_method(parts[0]) or self.smtp_UNKNOWN
            if len(parts) == 2:
                method(parts[1])
            else:
                method('')
        else:
            self.respond(500, 'Bad syntax')
    
    def lookup_method(self, command):
        return getattr(self, 'smtp_' + command.upper(), None)
    
    def smtp_UNKNOWN(self, rest):
        self.respond(500, 'Unrecognized command')

    def smtp_HELO(self, arg):
        if not arg:
            self.respond(501, 'HELO requires domain/address - see RFC-2821 4.1.1.1')
            return
        if self._helo:
            self.respond(503, 'but you already said HELO...')
        else:
            self._helo = arg
            self.begin_session()
            self.respond(250, '%s Hello %s, nice to meet you' % (self.fqdn, arg))        

    def smtp_QUIT(self, arg):
        self.respond(221, 'See you later')
        self.close()        
        return False
    
    def smtp_RSET(self, arg):
        self.reset_session()
        self.respond(250, 'I remember nothing.')
        
    def smtp_NOOP(self, arg):
        if arg:
            self.respond(250, 'Syntax: NOOP')
        else:
            self.respond(250, 'Ok')
        
    def smtp_VRFY(self, arg):
        #TODO: implement VRFY support
        code = DENY
        if code == DENY:
            self.respond(554, "Access denied")
        elif code == ALLOW:
            self.respond(250, "User OK")
        else:
            self.respond(252, "Just try sending a mail and we'll see how it turns out...")
        
    # A string of quoted strings, backslash-escaped character or
    # atom characters + '@.,:'
    qstring = r'("[^"]*"|\\.|' + atom + r'|[@.,:])+'

    mail_re = re.compile(r'''\s*FROM:\s*(?P<path><> # Empty <>
                         |<''' + qstring + r'''> # <addr>
                         |''' + qstring + r''' # addr
                         )\s*(\s(?P<opts>.*))? # Optional WS + ESMTP options
                         $''',re.I|re.X)
    rcpt_re = re.compile(r'\s*TO:\s*(?P<path><' + qstring + r'''> # <addr>
                         |''' + qstring + r''' # addr
                         )\s*(\s(?P<opts>.*))? # Optional WS + ESMTP options
                         $''',re.I|re.X)

    def smtp_MAIL(self, arg):
        if self._helo == None:
            self.respond(503, "Don't be rude, say hello first...")
            return
        elif self._from:
            self.respond(503, "Only one sender per message, please")
            return
        
        m = self.mail_re.match(arg)
        if not m:
            self.respond(501, "Syntax error")
            return

        try:
            addr = EmailAddress(m.group('path'), self.fqdn)
        except AddressError, e:
            self.respond(553, str(e))
            return
        
        try:
            ret, addr = self._validate_sender(addr)
            if ret == DENY:
                self.respond(550, 'Denied')
                return
            elif ret == DENYSOFT:
                self.respond(450, 'Temporarily denied')
                return
            elif ret == DENY_DISCONNECT:
                self.reset_session()
                self.respond(550, 'Denied')
                self.close()
                return False
            elif ret == DENYSOFT_DISCONNECT:
                self.reset_session()
                self.respond(421, 'Temporarily denied')
                self.close()
                return False
        except Exception, exc:
            logging.error("SMTP sender (%s) validation failure %s" % (addr, exc))
            self.respond(451, 'Internal server error')
            return
        
        self._from = addr
        self.respond(250, 'Sender OK')
    
    def _validate_sender(self, mailfrom):
        """
        Validate the address from which the message originates.

        @type helo: C{(str, str)}
        @param helo: The argument to the HELO command and the client's IP
        address.

        @type origin: C{EmailAddress}
        @param origin: The address the message is from

        @rtype: C{Deferred} or C{EmailAddress}
        @return: C{origin} or a C{Deferred} whose callback will be
        passed C{origin}.

        @raise SMTPBadSender: Raised of messages from this address are
        not to be accepted.
        """
        if self.delivery is not None:
            return self.delivery.validate_sender(self._session_token, self._helo, mailfrom)
        
        return DENY, None
    
    def smtp_RCPT(self, arg):
        if not self._from:
            self.respond(503, "Must have sender before recipient")
            return
        m = self.rcpt_re.match(arg)
        if not m:
            self.respond(501, "Syntax error")
            return

        try:
            addr = EmailAddress(m.group('path'), self.fqdn)
        except AddressError, e:
            self.respond(553, str(e))
            return
        
        try:
            ret, addr = self._validate_recipient(addr)
            if ret == DENY:
                self.respond(550, 'Relaying denied')
                return
            elif ret == DENYSOFT:
                self.respond(450, 'Relaying denied')
                return
            elif ret == DENY_DISCONNECT:
                self.reset_session()
                self.respond(550, 'Delivery denied')
                self.close()
                return False
            elif ret == DENYSOFT_DISCONNECT:
                self.reset_session()
                self.respond(421, 'Delivery denied')
                self.close()
                return False
        except Exception, exc:
            logging.error("SMTP receiver (%s) validation failure" % (addr,))
            self.respond(451, 'Internal server error')            
            return
        
        self._recipients.append(addr)
        self.respond(250, "Recipient OK")
    
    def _validate_recipient(self, rcptto):
        """
        Validate the address for which the message is destined.

        @type user: C{SMTPUser}
        @param user: The address to validate.

        @rtype: no-argument callable
        @return: A C{Deferred} which becomes, or a callable which
        takes no arguments and returns an object implementing C{IMessage}.
        This will be called and the returned object used to deliver the
        message when it arrives.

        @raise SMTPBadRcpt: Raised if messages to the address are
        not to be accepted.
        return (CODE, EmailAddress)
        """
        if self.delivery is not None:
            return self.delivery.validate_recipient(self._session_token, self._from, rcptto)
        return DENY, None
    
    def smtp_DATA(self, rest):
        if self._from is None or (not self._recipients):
            self.respond(503, 'Must have valid receiver and originator')
            return
        
        self.mode = DATA
        self.respond(354, 'Continue')
            
        #if True:
        #fmt = 'Receiving message for delivery: from=%s to=%s'
        #logging.error(fmt % (origin, [str(u) for (u, f) in recipients]))

    def state_DATA(self, data):
        self.mode = COMMAND
        ret, msg = self.message_received(data)
        self.reset_session()
        if ret == ALLOW:
            self.respond(250, 'Delivery in progress')
        else:
            self.respond(550, msg)
    
    def message_received(self, data):
        if self.delivery is not None:
            return self.delivery.message_received(self._session_token, self._from, self._recipients, data)
        return DENY, None
    
    #----------------------------------------------------------------------
    def set_timeout(self, timeout=None, N=uniq_id().next):
        if (timeout is not None) & (timeout > 0):
            deadline = timeout
        else:
            deadline = self.timeout_data if self.mode == DATA else self.timeout_command
        if deadline is not None:
            deadline += time.time()
            if self.__timeout_obj is not None:
                #self.reset_timeout(self)
                self._io_loop.update_timeout(self.__timeout_obj, deadline)
            
            self.__timeout_id = N()
            self.__timeout_obj = self._io_loop.add_timeout(deadline, self.__timed_out, self.__timeout_id)
    
    def reset_timeout(self):
        if self.__timeout_obj is not None:
            self._io_loop.remove_timeout(self.__timeout_obj)
            self.__timeout_id = None
            self.__timeout_obj = None
    
    def __timed_out(self, param):
        self.__timeout_obj = None
        if self.__timeout_id == param:
            self.timeout_connection()
    
    #----------------------------------------------------------------------
    def timeout_connection(self):
        """"""
        self.respond(421, 'Timeout. Try talking faster next time!')
        self.close()        
    
    #----------------------------------------------------------------------
    def begin_session(self):
        """"""
        if self.delivery_factory is not None:
            self.delivery = self.delivery_factory.getMessageDelivery()

        if self.delivery is not None:
            self._session_token = self.delivery.begin_session(self._helo, self.peer_ip)
    
    #----------------------------------------------------------------------
    def reset_session(self):
        """"""
        if self.delivery is not None and self._session_token is not None:
            self.delivery.reset_session(self._session_token)
        
        self._session_token = None
        self._from = None
        self._recipients = []


########### TEST ###########################################################

email = EmailAddress('abc@gmail.com')
email = EmailAddress('samarah', 'kumkum.masroor')

class DummyMessageDelivery(MessageDelivery):
    def validate_sender(self, session_token, helo, mailfrom):
        # All addresses are accepted
        return (ALLOW, mailfrom)
    
    def validate_recipient(self, session_token, mailfrom, rcptto):
        # Only messages directed to the "console" user are accepted.
        if rcptto.local == "c":
            return (ALLOW, rcptto)
        return (DENY, None)

    def verify_recipient(self, user):
        return DENY
    
    def message_received(self, session_token, mailfrom, rcpttos, data):
        return (ALLOW, 'Ok')
    
srv = SMTPServer(None, None, DummyMessageDelivery(), num_processes=1)
srv.listen(8888)
try:
    #poller = threading.Thread(target=ioloop.IOLoop.instance().start)
    #poller.start()
    ioloop.IOLoop.instance().start()
except KeyboardInterrupt as key:
    srv.stop()
    
    
    