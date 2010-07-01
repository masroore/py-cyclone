#!/pydev/bin/python

import tsmtpd2
import ioloop
from datetime import date

def handle_request(request):
   pass

http_server = tsmtpd2.TSMTPServer(handle_request)
http_server.listen(8888)
ioloop.IOLoop.instance().start()
