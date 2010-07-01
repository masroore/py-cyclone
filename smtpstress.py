#!/usr/bin/env python

import ioloop
import iostream
import socket
import time
import sys, random
import email, re

SMTP_PORT = 25
CRLF="\r\n"
EOM="\r\n.\r\n"

def quoteaddr(addr):
    """Quote a subset of the email addresses defined by RFC 821.

    Should be able to handle anything email.utils.parseaddr can handle.
    """
    m = (None, None)
    try:
        m = email.utils.parseaddr(addr)[1]
    except AttributeError:
        pass
    if m == (None, None): # Indicates parse failure or AttributeError
        # something weird here.. punt -ddm
        return "<%s>" % addr
    elif m is None:
        # the sender wants an empty return address
        return "<>"
    else:
        return "<%s>" % m

def quotedata(data):
    """Quote data for email.

    Double leading '.', and change Unix newline '\\n', or Mac '\\r' into
    Internet CRLF end-of-line.
    """
    return re.sub(r'(?m)^\.', '..',
        re.sub(r'(?:\r\n|\n|\r(?!\n))', CRLF, data))

class MailGenerator(object):
    def begin_session(self):
        return 1
    
    def end_session(self, session_token):
        pass
    
    def get_message(self, session_token):
        return 'Hello world!\r\n' * random.randint(20, 100)
    
    def get_sender(self, session_token):
        return 'john@doe.com'
    
    def get_recipients(self, session_token):
        return ['c@nowhere.com' for i in range(random.randint(1, 10))]

class SmtpAgent(object):
    CONNECT, HELO, MAIL, RCPT, DATA1, DATA2, DATA3, QUIT = 'CONNECT', 'HELO', 'MAIL', 'RCPT', 'DATA1', 'DATA2', 'DATA3', 'QUIT'
    
    def __init__(self, host, port=SMTP_PORT, 
                 local_hostname=None, io_loop=None, mail_generator=None,
                 connection_manager=None, num_emails = 3, debug_level=0,
                 timeout=socket._GLOBAL_DEFAULT_TIMEOUT, client_id=None):
        self.host = host
        self.port = port
        self.local_hostname = local_hostname or socket.getfqdn()        
        self.debuglevel = debug_level
        self.timeout = timeout
        self._ioloop = io_loop or ioloop.IOLoop.instance()
        self._manager = connection_manager
        self._stream = None
        self._state = self.CONNECT
        self._expected_code = -1
        self._num_emails = num_emails
        self._mail_generator = mail_generator
        self._session_token = None
        self.total_bytes = 0
        self.total_messages = 0
        self.alive_since = time.time()
        self._pending_close = False
        self.client_id = client_id
    
    def start(self):
        self.debug_dump('connect: (%s:%d)' % (self.host, self.port))
        self.sock = socket.create_connection((self.host, self.port), self.timeout)
        self._stream = iostream.IOStream(self.sock)
        self._state = self.HELO
        self._expected_code = 220
        if self._mail_generator:
            self._session_token = self._mail_generator.begin_session()
        self.await_reply()        
    
    def await_reply(self):
        self._stream.read_until(CRLF, self._on_read_complete)
        
    def write_line(self, line, term=CRLF):
        if not self._stream.writing():
            if self._state == self.DATA2:
                self.debug_dump('>> sending message (%d bytes)' % len(line))
            else:
                self.debug_dump('>> send: %s' % line)
            self._stream.write(line + term, self._on_write_complete)
    
    def _on_write_complete(self):
        if self._pending_close:
            self._shutdown()
            return
        self.await_reply()
    
    def parse_reply(self, line):
        #self.debug_dump('reply: %s' % repr(line))
        msg = line[4:].strip(b' \t\r\n')
        
        try:
            code = int(line[:3])
        except ValueError:
            code = -1
        
        self.debug_dump('<< reply: code (%s); msg: %s' % (code,msg))
        return code, msg
    
    def _on_read_complete(self, data):
        if self._pending_close:
            self._shutdown()
            return
        
        (code, msg) = self.parse_reply(data)
        self._process_reply(code, msg)
    
    def _process_reply(self, code, msg):
        if code != self._expected_code:
            self.close()
            self.debug_dump('bad reply. exiting')
            return
        
        getattr(self, 'smtp_' + self._state.upper(), None)()
    
    def smtp_HELO(self):
        self.write_line('HELO %s' % self.local_hostname)
        self._state = self.MAIL
        self._expected_code = 250
    
    def smtp_MAIL(self):
        self.__sender = self.get_sender()
        self._recipients = self.get_recipients()
        
        if self.__sender is not None:
            self.write_line('MAIL FROM: %s' % quoteaddr(self.__sender))
        self._state = self.RCPT
        self._expected_code = 250

    def smtp_RCPT(self):
        if self._recipients:
            rcptto = self._recipients.pop()
            self.write_line('RCPT TO: %s' % quoteaddr(rcptto))
        
        self._state = self.RCPT if self._recipients else self.DATA1
        self._expected_code = 250

    def smtp_DATA1(self):
        self.write_line('DATA')
        self._state = self.DATA2
        self._expected_code = 354
    
    def smtp_DATA2(self):
        message_data = self.get_message()
        if message_data is not None:
            self.write_line(quotedata(message_data), EOM)
        self.__message_size = len(message_data)
        self._state = self.DATA3
        self._expected_code = 250

    def smtp_DATA3(self):
        self._num_emails -= 1
        self.total_messages += 1
        self.total_bytes += self.__message_size
        self.__message_size = 0
        
        if self._num_emails > 0:
            self.smtp_MAIL()
        else:
            self._state = self.QUIT
            self.smtp_QUIT()

    def smtp_QUIT(self):
        self.write_line('QUIT')
        self.close()

    def debug_dump(self, msg):
        if self.debuglevel > 0:
            print>>sys.stderr, msg
    
    def close(self):
        self._pending_close = True        
        if self._stream and (not self._stream.writing() or not self._stream.reading()):
            self._shutdown()
    
    def _shutdown(self):
        if self._stream:        
            self._stream.close()
        
        if self._manager is not None:
            self._manager.handle_close(self)
    
    def get_message(self):
        if self._mail_generator:
            return self._mail_generator.get_message(self._session_token)
    
    def get_sender(self):
        if self._mail_generator:
            return self._mail_generator.get_sender(self._session_token)
    
    def get_recipients(self):
        if self._mail_generator:
            return self._mail_generator.get_recipients(self._session_token)
    
########################################################################
class  SmtpLoadManager(object):
    def __init__(self, host, port=SMTP_PORT, local_hostname=None, 
                 io_loop=None, mail_generator=None, 
                 num_agents=1, num_emails=3, debug_level=0):
        self.host = host
        self.port = port
        self.local_hostname = local_hostname or socket.getfqdn()        
        self.debuglevel = 0
        self._ioloop = io_loop or ioloop.IOLoop.instance()
        self._num_emails = num_emails
        self._num_agents = num_agents
        self._mail_generator = mail_generator
        self._start_time = None
        self._agent_refs = []
        self.rampup = 1
        self.running = False
        self._debug_level = debug_level
        
        ################################################3
        self.tbytes = 0
        self.tmails = 0
    
    def start(self):
        """"""
        print "PyCyclone SMTP Stresser\r\n"
        self._start_time = time.time()
        self.running = True
        for i in range(self._num_agents):
            '''            
            spacing = float(self.rampup) / float(self._num_agents)
            if i > 0:  # first agent starts right away
                time.sleep(spacing)
            '''
            if self.running:  # in case stop() was called before all agents are started
                agent = SmtpAgent(host=self.host, port=self.port, local_hostname=self.local_hostname, 
                                  io_loop=self._ioloop, mail_generator=self._mail_generator, 
                                  connection_manager=self, num_emails=self._num_emails, 
                                  debug_level=self._debug_level, client_id=i)
                agent.start()
                self._agent_refs.append(agent)
                agent_started_line = 'Started agent ' + str(i + 1)
                if sys.platform.startswith('win'):
                    sys.stdout.write(chr(0x08) * len(agent_started_line))  # move cursor back so we update the same line again
                    sys.stdout.write(agent_started_line)
                else:
                    esc = chr(27) # escape key
                    sys.stdout.write(esc + '[G' )
                    sys.stdout.write(esc + '[A' )
                    sys.stdout.write(agent_started_line + '\n')        
    
    def handle_close(self, agent):
        self._agent_refs.remove(agent)
        #print '%d: %d - %d' % (agent.client_id, agent.total_bytes, agent.total_messages)
        self.tbytes += agent.total_bytes
        self.tmails += agent.total_messages
        if len(self._agent_refs) == 0:
            now = time.time()            
            self.stop()
            diff = now - self._start_time
            datarate = self.tbytes / diff
            datarate /= 1024
            mailrate = self.tmails / diff
            print '%d secs - %d KB/s - %d mails/s' % (diff, datarate, mailrate)
    
    def stop(self):
        diff = time.time() - self._start_time
        print '%d seconds total' % diff
        self.running = False
        self._ioloop.stop()
    
mail_feeder=MailGenerator()
smtp = SmtpLoadManager('localhost', 8888, num_emails = 500, 
                       mail_generator=mail_feeder, num_agents = 1000, 
                       debug_level=0, local_hostname='maxXx')
smtp.start()
ioloop.IOLoop.instance().start()
