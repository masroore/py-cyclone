import errno
import functools
import ioloop
import iostream
import logging
import os
import socket
import time
import fcntl

class Devnull:
    def write(self, msg): pass
    def flush(self): pass


DEBUGSTREAM = Devnull()
NEWLINE = '\n'
EMPTYSTRING = ''
COMMASPACE = ', '

class TSMTPServer(object):
    cnt = 0
    
    def __init__(self, request_callback, io_loop=None):
        """Initializes the server with the given request callback.

        If you use pre-forking/start() instead of the listen() method to
        start your server, you should not pass an IOLoop instance to this
        constructor. Each pre-forked child process will create its own
        IOLoop instance after the forking process.
        """
        self.request_callback = request_callback
        self.io_loop = io_loop
        self._socket = None
        self._started = False

    def listen(self, port, address=""):
        """Binds to the given port and starts the server in a single process.

        This method is a shortcut for:

            server.bind(port, address)
            server.start(1)

        """
        self.bind(port, address)
        self.start(1)

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
        
        self._START = time.time()
        
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
                        self._socket.fileno(), self._handle_events,
                        ioloop.IOLoop.READ)
                    return
            os.waitpid(-1, 0)
        else:
            if not self.io_loop:
                self.io_loop = ioloop.IOLoop.instance()
            self.io_loop.add_handler(self._socket.fileno(),
                                     self._handle_events,
                                     ioloop.IOLoop.READ)

    def stop(self):
        self.io_loop.remove_handler(self._socket.fileno())
        self._socket.close()

    def _handle_events(self, fd, events):
        while True:
            try:
                connection, address = self._socket.accept()
            except socket.error, e:
                if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    return
                raise
            try:
                stream = iostream.IOStream(connection, io_loop=self.io_loop)
                TSMTPConnection(stream, address, self.process_message)
            except:
                logging.error("Error in connection callback", exc_info=True)
                
    # Do something with the gathered message
    def process_message(self, peer, mailfrom, rcpttos, data):
        self.cnt += 1
        
        if self.cnt % 10000 == 0:
            now = time.time()
            seconds = now - self._START
            print '%d mails | %d seconds | %d/sec' % (self.cnt, seconds, self.cnt / seconds)
            self.cnt = 0
            self._START = now

            #mlen = len(data)
            #print '#%d MSG: %s (%d bytes)' % (self.cnt, peer, mlen)
        '''
        inheaders = 1
        lines = data.split('\n')
        print '---------- MESSAGE FOLLOWS ----------'
        for line in lines:
            # headers first
            if inheaders and not line:
                print 'X-Peer:', peer
                inheaders = 0
            print line
        print '------------ END MESSAGE ------------'                
        '''

class TSMTPConnection(object):
    """Handles a connection to an SMTP client, executing SMTP requests.

    We parse SMTP headers and bodies, and execute the request callback
    until the SMTP conection is closed.
    """
    COMMAND = 0
    DATA = 1
    
    ac_in_buffer_size       = 4096
    ac_out_buffer_size      = 4096
    
    def __init__(self, stream, address, request_callback):
        self.stream = stream
        self.address = address
        self.request_callback = request_callback
        self._request_finished = False
        
        #self.stream.read_until("\r\n", self._on_headers)
        self.ac_in_buffer = ''
        self.__line = []
        self.__state = self.COMMAND
        self.__greeting = 0
        self.__mailfrom = None
        self.__rcpttos = []
        self.__data = ''
        self.__fqdn = 'PySMTPd'
        self.__version = '0.0.0'
        self.__peer = 'PEERS'
        #self.__fqdn = socket.getfqdn()
        #self.__peer = conn.getpeername()
        #print >> DEBUGSTREAM, 'Peer:', repr(self.__peer)
        self.terminator = '\r\n'
        self.push('220 %s %s' % (self.__fqdn, self.__version))
        
    def push(self, msg):
        self.write(msg + '\r\n')
        
    def set_terminator (self, term):
        "Set the input delimiter.  Can be a fixed string of any length, an integer, or None"
        self.terminator = term

    def get_terminator (self):
        return self.terminator
        
    def collect_incoming_data(self, data):
        self.__line.append(data)
        
    def found_terminator(self):
        line = EMPTYSTRING.join(self.__line)
        print >> DEBUGSTREAM, 'Data:', repr(line)
        self.__line = []
        if self.__state == self.COMMAND:
            if not line:
                self.push('500 Error: bad syntax')
                return
            method = None
            i = line.find(' ')
            if i < 0:
                command = line.upper()
                arg = None
            else:
                command = line[:i].upper()
                arg = line[i+1:].strip()
            method = getattr(self, 'smtp_' + command, None)
            if not method:
                self.push('502 Error: command "%s" not implemented' % command)
                return
            method(arg)
            return
        else:
            if self.__state != self.DATA:
                self.push('451 Internal confusion')
                return
            # Remove extraneous carriage returns and de-transparency according
            # to RFC 821, Section 4.5.2.
            data = []
            for text in line.split('\r\n'):
                if text and text[0] == '.':
                    data.append(text[1:])
                else:
                    data.append(text)
            self.__data = NEWLINE.join(data)
            status = self.request_callback(self.__peer,
                                           self.__mailfrom,
                                           self.__rcpttos,
                                           self.__data)
            self.__rcpttos = []
            self.__mailfrom = None
            self.__state = self.COMMAND
            self.set_terminator('\r\n')
            if not status:
                self.push('250 Ok')
            else:
                self.push(status)        

    def write(self, chunk):
        #assert self._request, "Request closed"
        if not self.stream.closed():
            self.stream.write(chunk, self._on_write_complete)

    def finish(self):
        #assert self._request, "Request closed"
        self._request_finished = True
        if not self.stream.writing():
            self._finish_request()

    def _on_write_complete(self):
        if self._request_finished:
            self._finish_request()
            return
        self.stream.read_until(self.get_terminator(), self._on_read_data)

    def _finish_request(self):
        self._request = None
        self._request_finished = False
        self.stream.close()
        return

    def _on_read_data(self, data):
        self.ac_in_buffer = self.ac_in_buffer + data
        terminator = self.get_terminator()
        terminator_len = len(terminator)
        
        while self.ac_in_buffer:
            lb = len(self.ac_in_buffer)
            index = self.ac_in_buffer.find(terminator)
            if index != -1:
                # we found the terminator
                if index > 0:
                    # don't bother reporting the empty string (source of subtle bugs)
                    self.collect_incoming_data (self.ac_in_buffer[:index])
                self.ac_in_buffer = self.ac_in_buffer[index+terminator_len:]
                # This does the Right Thing if the terminator is changed here.
                self.found_terminator()
            else:
                # check for a prefix of the terminator
                index = find_prefix_at_end (self.ac_in_buffer, terminator)
                if index:
                    if index != lb:
                        # we found a prefix, collect up to the prefix
                        self.collect_incoming_data (self.ac_in_buffer[:-index])
                        self.ac_in_buffer = self.ac_in_buffer[-index:]
                    break
                else:
                    # no prefix, collect it all
                    self.collect_incoming_data (self.ac_in_buffer)
                    self.ac_in_buffer = ''

    # SMTP and ESMTP commands
    def smtp_HELO(self, arg):
        if not arg:
            self.push('501 Syntax: HELO hostname')
            return
        if self.__greeting:
            self.push('503 Duplicate HELO/EHLO')
        else:
            self.__greeting = arg
            self.push('250 %s' % self.__fqdn)

    def smtp_EHLO(self, arg):
        if not arg:
            self.push('501 Syntax: HELO hostname')
            return
        if self.__greeting:
            self.push('503 Duplicate HELO/EHLO')
        else:
            self.__greeting = arg
            self.push('250-%s\r\n250-HELP\r\n250 XADR' % self.__fqdn)

    def smtp_NOOP(self, arg):
        if arg:
            self.push('501 Syntax: NOOP')
        else:
            self.push('250 Ok')

    def smtp_QUIT(self, arg):
        # args is ignored
        self.push('221 Bye')
        self.close_when_done()

    # factored
    def __getaddr(self, keyword, arg):
        address = None
        keylen = len(keyword)
        if arg[:keylen].upper() == keyword:
            address = arg[keylen:].strip()
            if not address:
                pass
            elif address[0] == '<' and address[-1] == '>' and address != '<>':
                # Addresses can be in the form <person@dom.com> but watch out
                # for null address, e.g. <>
                address = address[1:-1]
        return address

    def smtp_MAIL(self, arg):
        print >> DEBUGSTREAM, '===> MAIL', arg
        address = self.__getaddr('FROM:', arg) if arg else None
        if not address:
            self.push('501 Syntax: MAIL FROM:<address>')
            return
        if self.__mailfrom:
            self.push('503 Error: nested MAIL command')
            return
        self.__mailfrom = address
        print >> DEBUGSTREAM, 'sender:', self.__mailfrom
        self.push('250 Ok')

    def smtp_RCPT(self, arg):
        print >> DEBUGSTREAM, '===> RCPT', arg
        if not self.__mailfrom:
            self.push('503 Error: need MAIL command')
            return
        address = self.__getaddr('TO:', arg) if arg else None
        if not address:
            self.push('501 Syntax: RCPT TO: <address>')
            return
        self.__rcpttos.append(address)
        print >> DEBUGSTREAM, 'recips:', self.__rcpttos
        self.push('250 Ok')

    def smtp_RSET(self, arg):
        if arg:
            self.push('501 Syntax: RSET')
            return
        # Resets the sender, recipients, and data, but not the greeting
        self.__mailfrom = None
        self.__rcpttos = []
        self.__data = ''
        self.__state = self.COMMAND
        self.push('250 Ok')

    def smtp_DATA(self, arg):
        if not self.__rcpttos:
            self.push('503 Error: need RCPT command')
            return
        if arg:
            self.push('501 Syntax: DATA')
            return
        self.__state = self.DATA
        self.set_terminator('\r\n.\r\n')
        self.push('354 End data with <CR><LF>.<CR><LF>')
    
    def close_when_done (self):
        self.finish()

def find_prefix_at_end (haystack, needle):
    l = len(needle) - 1
    while l and not haystack.endswith(needle[:l]):
        l -= 1
    return l
