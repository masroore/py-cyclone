#!/usr/bin/env python

import smtplib

smtp = smtplib.SMTP('localhost', 8888)

print 'HELO'
smtp.helo('test')

print 'FROM'
smtp.mail('masroor')

print 'RCPT'
smtp.rcpt('c')

print 'DATA'
smtp.data('Subject: Test mail\r\n\r\nLine 1\r\nLine 2\r\nLine 3\r\n')

print 'QUIT'
smtp.quit()

print 'DONE'