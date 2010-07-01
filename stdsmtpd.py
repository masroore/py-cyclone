#!/usr/bin/env python

import smtpd
import asyncore
import socket
import logging, threading, os, time

class SMTPReceiver(smtpd.SMTPServer):
    """Receives emails and hands it to the Router for further processing."""

    def __init__(self, host='127.0.0.1', port=8888):
        """
        Initializes to bind on the given port and host/ipaddress.  Typically
        in deployment you'd give 0.0.0.0 for "all internet devices" but consult
        your operating system.

        This uses smtpd.SMTPServer in the __init__, which means that you have to 
        call this far after you use python-daemonize or else daemonize will
        close the socket.
        """
        self.host = host
        self.port = port
        smtpd.SMTPServer.__init__(self, (self.host, self.port), None)

    def start(self):
        """
        Kicks everything into gear and starts listening on the port.  This
        fires off threads and waits until they are done.
        """
        logging.info("SMTPReceiver started on %s:%d." % (self.host, self.port))
        #self.poller = threading.Thread(target=asyncore.loop, kwargs={'timeout':0.1, 'use_poll':True})
        self.cnt = 0
        self._START = None
        #self.poller.start()

    def process_message(self, Peer, From, To, Data):
        """
        Called by smtpd.SMTPServer when there's a message received.
        """
        if self.cnt == 0:
            self._START = time.time()        
        self.cnt += 1
        
        if self.cnt % 10000 == 0:
            now = time.time()
            seconds = now - self._START
            print '%d mails | %d seconds | %d/sec' % (self.cnt, seconds, self.cnt / seconds)
            self.cnt = 0
            self._START = now
    
    def stop(self):
        self.poller.join()
    
srv = SMTPReceiver()
try:
    srv.start()
    asyncore.loop(use_poll = True)
except KeyboardInterrupt as kbi:
    srv.stop()