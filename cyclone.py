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
'''
import functools
'''

try:
    import fcntl
except ImportError:
    if os.name == 'nt':
        import win32_support as fcntl
    else:
        raise

# Cache the hostname (XXX Yes - this is broken)
HOST_NAME = socket.gethostname() if sys.platform == 'darwin' else socket.getfqdn()
EMPTYSTRING = ''

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
    def received_header(self, helo, origin, recipients):
        """
        Generate the Received header for a message

        @type helo: C{(str, str)}
        @param helo: The argument to the HELO command and the client's IP
        address.

        @type origin: C{EmailAddress}
        @param origin: The address the message is from

        @type recipients: C{list} of L{SMTPUser}
        @param recipients: A list of the addresses for which this message
        is bound.

        @rtype: C{str}
        @return: The full \"Received\" header string.
        """
        pass

        #----------------------------------------------------------------------    
    def verify_recipient(self, user):
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
        """
        pass

    #----------------------------------------------------------------------    
    def validate_recipient(self, user):
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
        """
        pass

    #----------------------------------------------------------------------    
    def validate_sender(self, helo, origin):
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
class SMTPError(Exception):
    def __init__(self, code, resp):
        self.code = code
        self.resp = resp

    def __str__(self):
        return "%.3d %s" % (self.code, self.resp)

class SMTPAddressError(SMTPError):
    def __init__(self, addr, code, resp):
        SMTPError.__init__(self, code, resp)
        self.addr = EmailAddress(addr)

    def __str__(self):
        return "%.3d <%s>... %s" % (self.code, self.addr, self.resp)

class SMTPBadRecipient(SMTPError):
    def __init__(self, addr, code=550,
                 resp='Cannot receive for specified address'):
        SMTPAddressError.__init__(self, addr, code, resp)

class SMTPBadSender(SMTPError):
    def __init__(self, addr, code=550, resp='Sender not acceptable'):
        SMTPAddressError.__init__(self, addr, code, resp)


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
        if isinstance(addr, SMTPUser):
            addr = addr.dest
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

class SMTPUser:
    """Hold information about and SMTP message recipient,
    including information on where the message came from
    """

    def __init__(self, destination, helo, conn, orig):
        host = getattr(conn, 'host', None)
        self.dest = EmailAddress(destination, host)
        self.helo = helo
        self.conn = conn
        if isinstance(orig, EmailAddress):
            self.orig = orig
        else:
            self.orig = EmailAddress(orig, host)

    def __getstate__(self):
        """Helper for pickle.

        conn isn't picklabe, but we want SMTPUser to be, so skip it in
        the pickle.
        """
        return { 'dest' : self.dest,
                 'helo' : self.helo,
                 'conn' : None,
                 'orig' : self.orig }

    def __str__(self):
        return str(self.dest)

class MessageReceiver(object):
    """Interface definition for messages that can be sent via SMTP."""

    def line_received(self, line):
        """handle another line"""

    def eom_received(self):
        """handle end of message

        return a deferred. The deferred should be called with either:
        callback(string) or errback(error)
        """

    def connection_lost(self):
        """handle message truncated

        semantics should be to discard the message
        """    

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
                        self._handle_accepts,
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
                
    # Do something with the gathered message
    def _process_message(self, peer, mailfrom, rcpttos, data):
        pass
    

COMMAND, DATA, AUTH = 'COMMAND', 'DATA', 'AUTH'

class SMTPClientConnection(object):
    """SMTP server-side protocol."""

    timeout = 30.0
    timeout_final = 60.0
    fqdn = HOST_NAME
    TERMINATOR = '\r\n'

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
        self.__timeout_final = self._io_loop.add_timeout(self.timeout_final + time.time(), self.__timed_out_final, None)
        
        self.ac_in_buffer = b''
        self.__line = []
        self.__data = b''
        
        self.send_greeting()
    
    def __timed_out_final(self, param):
        self.__timed_out_final = None
        self.respond(421, "Game over pal! You just can't stay that long...", False)
        self.close()

    def send_greeting(self):
        self.respond(220, 'ESMTP %s ready; send us your mail, but not your spam.' % (self.fqdn,))
    
    def close(self):
        self._pending_close = True
        if not self._stream.writing():
            self._close_connection()

    def _close_connection(self):
        self.reset_timeout()
        if self.__timed_out_final is not None:
            self._io_loop.remove_timeout(self.__timeout_final)
        self._pending_close = False
        self._stream.close()        
        return
        
    def respond(self, status_code, message='', timeout=True):
        "Send an SMTP code with a message."
        lines = message.splitlines()
        lastline, tmplines = lines[-1:], []
        for line in lines[:-1]:
            tmplines.append('%3.3d-%s' % (status_code, line))
        tmplines.append('%3.3d %s' % (status_code, lastline and lastline[0] or ''))
        self.write_line('\r\n'.join(tmplines))
        if timeout:
            self.set_timeout()
        
    def write_line(self, message):
        self.write(message + '\r\n')
        
    def write(self, chunk):
        if not self._stream.closed():
            self._stream.write(chunk, self._on_write_complete)
    
    def _on_write_complete(self):
        self.reset_timeout()
        
        if self._pending_close:
            self._close_connection()
            return
        
        #TODO: what to do next?        
        self._stream.read_until(self.TERMINATOR, self._on_read_data)
        self.set_timeout()
    
    def _on_read_data(self, data):
        self.reset_timeout()
        self.ac_in_buffer = self.ac_in_buffer + data
        terminator_len = len(self.TERMINATOR)
        
        while self.ac_in_buffer:
            lb = len(self.ac_in_buffer)
            index = self.ac_in_buffer.find(self.TERMINATOR)
            if index != -1:
                # we found the terminator
                if index > 0:
                    # don't bother reporting the empty string (source of subtle bugs)
                    self.__line.append(self.ac_in_buffer[:index])
                self.ac_in_buffer = self.ac_in_buffer[index+terminator_len:]
                # This does the Right Thing if the terminator is changed here.
                self.found_terminator()
            else:
                # check for a prefix of the terminator
                index = find_prefix_at_end (self.ac_in_buffer, self.TERMINATOR)
                if index:
                    if index != lb:
                        # we found a prefix, collect up to the prefix
                        self.__line.append(self.ac_in_buffer[:-index])
                        self.ac_in_buffer = self.ac_in_buffer[-index:]
                    break
                else:
                    # no prefix, collect it all
                    self.__line.append(self.ac_in_buffer)
                    self.ac_in_buffer = ''
    
    def found_terminator(self):
        line = EMPTYSTRING.join(self.__line)
        self.__line = []
        getattr(self, 'state_' + self.mode)(line)
        
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
    
    #----------------------------------------------------------------------
    def reset_session(self):
        """"""
        self._from = None
        self._recipients = []        
    
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
            self._from = None
            self._recipients = []
            self.respond(250, '%s Hello %s, nice to meet you' % (self.fqdn, arg))        

    def smtp_QUIT(self, arg):
        self.respond(221, 'See you later')
        self.close()        
    
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

    def smtp_MAIL(self, rest):
        if self._helo == None:
            self.respond(503, "Don't be rude, say hello first...")
            return
        elif self._from:
            self.respond(503, "Only one sender per message, please")
            return
        
        self.reset_session()
        m = self.mail_re.match(rest)
        if not m:
            self.respond(501, "Syntax error")
            return

        try:
            addr = EmailAddress(m.group('path'), self.fqdn)
        except AddressError, e:
            self.respond(553, str(e))
            return
        
        try:
            ret = self.validate_sender(addr)
            if ret == DENY:
                self.respond(550, 'Denied')
                return
            elif ret == DENYSOFT:
                self.respond(450, 'Temporarily denied')
                return
            elif ret == DENY_DISCONNECT:
                self.respond(550, 'Denied')
                self.close()
                return
            elif ret == DENYSOFT_DISCONNECT:
                self.respond(421, 'Temporarily denied')
                self.close()
                return
        except Exception, exc:
            logging.error("SMTP sender (%s) validation failure %s" % (addr, exc))
            self.respond(451, 'Internal server error')
            raise
            #return
        
        self._from = addr
        self.respond(250, 'Sender OK')
    
    def validate_sender(self, sender_addr):
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
        if self.delivery_factory is not None:
            self.delivery = self.delivery_factory.getMessageDelivery()

        if self.delivery is not None:
            return self.delivery.validate_sender(self._helo, sender_addr)
        
        return ALLOW
    
    def smtp_RCPT(self, arg):
        if not self._from:
            self.respond(503, "Must have sender before recipient")
            return
        m = self.rcpt_re.match(arg)
        if not m:
            self.respond(501, "Syntax error")
            return

        try:
            user = SMTPUser(m.group('path'), self._helo, self, self._from)
        except AddressError, e:
            self.respond(553, str(e))
            return
        
        try:
            ret, msg_proc = self.validate_recipient(user)
            if ret == DENY:
                self.respond(550, 'Relaying denied')
                return
            elif ret == DENYSOFT:
                self.respond(450, 'Relaying denied')
                return
            elif ret == DENY_DISCONNECT:
                self.respond(550, 'Delivery denied')
                self.close()
                return
            elif ret == DENYSOFT_DISCONNECT:
                self.respond(421, 'Delivery denied')
                self.close()
                return
        except Exception, exc:
            logging.error("SMTP sender (%s) validation failure" % (user.dest,))
            self.respond(451, 'Internal server error')
            raise
            #return
        
        self._recipients.append((user, msg_proc))
        self.respond(250, "Recipient OK")
    
    def validate_recipient(self, user):
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
        """
        if self.delivery is not None:
            return self.delivery.validate_recipient(user)
        return DENY, None
    
    def smtp_DATA(self, rest):
        if self._from is None or (not self._recipients):
            self.respond(503, 'Must have valid receiver and originator')
            return
        self.mode = DATA
        helo, origin = self._helo, self._from
        recipients = self._recipients

        self._from = None
        self._recipients = []
        self.datafailed = None

        msgs = []
        for (user, msgFunc) in recipients:
            try:
                msg = msgFunc()
                rcvdhdr = self.received_header(helo, origin, [user])
                if rcvdhdr:
                    msg.line_received(rcvdhdr)
                msgs.append(msg)
            except SMTPError, e:
                self.respond(e.code, e.resp)
                self.mode = COMMAND
                self._disconnect(msgs)
                return
            except Exception, e:
                logging.error(e)
                self.respond(550, "Internal server error")
                self.mode = COMMAND
                self._disconnect(msgs)
                return
        self.__messages = msgs
        
        self.__inheader = self.__inbody = 0
        self.respond(354, 'Continue')
        
        #if True:
        #fmt = 'Receiving message for delivery: from=%s to=%s'
        #logging.error(fmt % (origin, [str(u) for (u, f) in recipients]))        

    def _disconnect(self, msgs):
        for msg in msgs:
            try:
                msg.connection_lost()
            except:
                logging.error("msg raised exception from connection_lost")
    
    
    #def state_DATA(self, line):
    def data_line_received(self, line):
        if line[:1] == '.':
            if line == '.':
                self.mode = COMMAND
                if self.datafailed:
                    self.respond(self.datafailed.code,
                                  self.datafailed.resp)
                    return False
                if not self.__messages:
                    self._message_handled("thrown away")
                    return False
                
                resp = []
                for m in self.__messages:
                    resp.append(m.eom_received())
                
                self._message_handled(resp)

                del self.__messages
                return False
            line = line[1:]
        
        if self.datafailed:
            return False

        try:
            # Add a blank line between the generated Received:-header
            # and the message body if the message comes in without any
            # headers
            if not self.__inheader and not self.__inbody:
                if ':' in line:
                    self.__inheader = 1
                elif line:
                    for message in self.__messages:
                        message.line_received('')
                    self.__inbody = 1

            if not line:
                self.__inbody = 1

            for message in self.__messages:
                message.line_received(line)
        except SMTPError, e:
            self.datafailed = e
            for message in self.__messages:
                message.connection_lost()
        return True
    
    #state_DATA = data_line_received
    def state_DATA(self, line):
        if self.data_line_received(line):
            self._stream.read_until(self.TERMINATOR, self._on_read_data)

    def _message_handled(self, resultList):
        failures = 0
        for (success, result) in resultList:
            if success != ALLOW:
                failures += 1
                log.err(result)
        if failures:
            msg = 'Could not send e-mail'
            L = len(resultList)
            if L > 1:
                msg += ' (%d failures out of %d recipients)' % (failures, L)
            self.respond(550, msg)
        else:
            self.respond(250, 'Delivery in progress')
        return False
    
    def connection_lost(self, reason):
        # self.respond(421, 'Dropping connection.') # This does nothing...
        # Ideally, if we (rather than the other side) lose the connection,
        # we should be able to tell the other side that we are going away.
        # RFC-2821 requires that we try.
        if self.mode is DATA:
            try:
                for message in self.__messages:
                    try:
                        message.connection_lost()
                    except Exception, e:
                        logging.error(e)
                del self.__messages
            except AttributeError:
                pass
        self.reset_timeout()
        
    def received_header(self, helo, origin, recipients):
        if self.delivery is not None:
            return self.delivery.received_header(helo, origin, recipients)

        heloStr = ""
        if helo[0]:
            heloStr = " helo=%s" % (helo[0],)
        domain = self.peer_ip
        from_ = "from %s ([%s]%s)" % (helo[0], helo[1], heloStr)
        by = "by %s with %s (%s)" % (domain,
                                     self.__class__.__name__,
                                     longversion)
        for_ = "for %s; %s" % (' '.join(map(str, recipients)),
                               rfc822date())
        return "Received: %s\n\t%s\n\t%s" % (from_, by, for_)
    

    #----------------------------------------------------------------------
    def set_timeout(self, timeout=None, N=uniq_id().next):
        deadline = timeout if timeout else self.timeout
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
        self.respond(421, 'Timeout. Try talking faster next time!', False)
        self.close()        


def find_prefix_at_end (haystack, needle):
    l = len(needle) - 1
    while l and not haystack.endswith(needle[:l]):
        l -= 1
    return l


########### TEST ###########################################################

email = EmailAddress('abc@gmail.com')
email = EmailAddress('samarah', 'kumkum.masroor')

class ConsoleMessageDelivery(MessageDelivery):
    def received_header(self, helo, origin, recipients):
        return "Received: ConsoleMessageDelivery"
    
    def validate_sender(self, helo, origin):
        # All addresses are accepted
        return ALLOW
    
    def validate_recipient(self, user):
        # Only messages directed to the "console" user are accepted.
        if user.dest.local == "c":
            #return (ALLOW, lambda: ConsoleMessage())
            return (ALLOW, ConsoleMessage)
        return (DENY, None)

    def verify_recipient(self, user):
        return None

class ConsoleMessage(MessageReceiver):
    def __init__(self):
        self.lines = []
        self.count = 0
    
    def line_received(self, line):
        self.lines.append(line)
    
    def eom_received(self):
        #print "New message received:\n----------------------------"
        #print "\n".join(self.lines)
        #print '----------------------------'
        self.lines = None
        self.count += 1
        
        return ALLOW, ""
        #return defer.succeed(None)
    
    def connection_lost(self):
        # There was an error, throw away the stored lines
        self.lines = None
        print "Connection lost"

srv = SMTPServer(None, None, ConsoleMessageDelivery())
srv.listen(8888)
ioloop.IOLoop.instance().start()