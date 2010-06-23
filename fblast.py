#! /usr/bin/env python
"""Throw email at Mailman as fast as you can.

This is not a unit test, it's a functional test, so you can't run it within
the unit test framework (hence its filename doesn't start with `test_').
Here's how I use this one:

- set up a dummy list

- add an alias to your MTA, say `devnull' that pipes its messages to, you
  guessed it, /dev/null

- make this address a member of your list

- add another address to `accept_these_non_members', let's call it ok@dom.ain

- change the FROMADDR variable to ok@dom.ain

- change the LISTADDR variable to point to your list

- run this program like so: python fblast.py N
  where N is the number of seconds to sleep before sending the next msg

- let this run until you're tired of it, then hit ^C
"""

FROMADDR = 'ok@dom.ain'
LISTADDR = 'c@dom.ain'

import sys
import time, random
import smtplib

conn = smtplib.SMTP('127.0.0.1', 8888)
#conn.connect()

snooze = 0.3 #int(sys.argv[1])
BLAST_SIZE = 100

rcpts = ['mbox' + str(i) + '@test' for i in range(100)]

def getRcpt():
    global rcpts
    return rcpts[random.randint(1, 99)]

start = time.time()

try:
    i = 1
    while 1:
        #sys.stdout.write('.')
        #sys.stdout.flush()
        i += 1
        if i % 10 == 0:
            diff = time.time() - start
            total = i * BLAST_SIZE
            print 'Messages:%d Elapsed:%d secs Rate:%d/sec' % (total, diff, total / diff)
        
        times = int(random.randrange(20, 60))
        data = 'XXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxxXXxx\r\n' * times
        
        for j in range(BLAST_SIZE):
            conn.sendmail(FROMADDR, [LISTADDR], """\
From: %(FROMADDR)s
To: $(LISTADDR)s
Subject: test %(num)d
X-No-Archive: yes

testing %(num)d
%(FILLER)s
""" % {'num'     : i,
       'FROMADDR': FROMADDR,
       'LISTADDR': LISTADDR, #getRcpt(),
       'FILLER': data,
       })
        time.sleep(snooze)
finally:
    conn.quit()
