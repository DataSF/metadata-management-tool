
# coding: utf-8

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
import csv
import time
import datetime
import logging
from retry import retry
import yaml
import os
import itertools
import base64
import inflection
import csv, codecs, cStringIO
from ConfigUtils import *



class Emailer():
    '''
    util class to email stuff to people.
    '''
    def __init__(self, configItems):
        self._config_dir =  configItems['config_dir']
        self._email_config_file = configItems['email_config_file']
        self._emailConfigs = ConfigUtils.setConfigs(self._config_dir, self._email_config_file)
        self._server = None
        self._server_port = None
        self._sender = None
        self._password = None
        self._bcc = None
        self.setConfigs()


    def getRecipients(self):
        if 'etl_recipients'in self._emailConfigs.keys():
            return self._emailConfigs['etl_recipients']
        return None

    def setConfigs(self):
        self._server = self._emailConfigs['server_addr']
        self._server_port = self._emailConfigs['server_port']
        self._sender =  self._emailConfigs['sender_addr']
        self._etl_sender =   self._emailConfigs['etl_sender_addr']
        self._bcc = self._emailConfigs['bcc']
        if (self._emailConfigs['sender_password']):
            self._password = base64.b64decode(self._emailConfigs['sender_password'])

    @staticmethod
    def make_attachment(msg, fname_attachment, fname_attachment_fullpath):
        filename = fname_attachment
        attachment = open(fname_attachment_fullpath, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(part)
        return msg

    def sendEmails(self, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None, recipients=None, attachment_dictList = None, isETL=True):
        if(isETL):
            fromaddr = self._etl_sender
            recipients = self.getRecipients()
        else:
            fromaddr = self._sender
            recipients = recipients
        toaddr = recipients
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = recipients
        msg['Subject'] = subject_line
        msg['Bcc'] = self._bcc
        body = msgBody
        msg.attach(MIMEText(body, 'html'))
        #Optional Email Attachment:
        if(not(fname_attachment is None and fname_attachment_fullpath is None)):
            msg = self.make_attachment(msg, fname_attachment, fname_attachment_fullpath)
        if attachment_dictList:
            print attachment_dictList
            for attachment in attachment_dictList:
                fname = attachment.keys()
                fname = fname[0]
                fname_attachment_fullpath = attachment.values()
                fname_attachment_fullpath = fname_attachment_fullpath[0]
                msg = self.make_attachment(msg, fname, fname_attachment_fullpath)

        #normal emails, no attachment
        print self._server
        server = smtplib.SMTP(self._server, self._server_port)
        #server.starttls()
        #server.login(fromaddr, self._password)
        text = msg.as_string()
        server.sendmail(fromaddr+  self._bcc, toaddr, text)
        server.quit()




if __name__ == "__main__":
    main()

