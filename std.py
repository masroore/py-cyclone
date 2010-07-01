#!/pydev/bin/python

import tsmtpd
from datetime import date

def handle_request(request):
   pass

http_server = tsmtpd.TSMTPServer(handle_request)
http_server.listen(8888)
ioloop.IOLoop.instance().start()
