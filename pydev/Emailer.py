
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
        self.config_dir =  configItems['config_dir']
        self.email_config_file = configItems['email_config_file']
        self.emailConfigs = self.setEmailerConfig()
        self.server = None 
        self.server_port = None 
        self.sender = None 
        self.password = None 
        self.setConfigs()
    
    def getEmailerConfigs(self):
        return self.emailConfigs
        
        
    def setEmailerConfig(self):
        with open( self.config_dir + self.email_config_file ,  'r') as stream:
            try:
                email_items = yaml.load(stream)
                return email_items
            except yaml.YAMLError as exc:
                print(exc)
        return 0
    
    def setConfigs(self):
        self.server = self.emailConfigs['server_addr']
        self.server_port = self.emailConfigs['server_port']
        self.sender =  self.emailConfigs['sender_addr']
        if (self.emailConfigs['sender_password']):
            self.password = base64.b64decode(self.emailConfigs['sender_password'])
       
    
    def sendEmails(self, recipients, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None):
        fromaddr = self.sender
        toaddr =  recipients
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = recipients
        msg['Subject'] = subject_line
        body = msgBody
        msg.attach(MIMEText(body, 'html'))
          
        #Optional Email Attachment:
        if(not(fname_attachment is None and fname_attachment_fullpath is None)):
            filename = fname_attachment
            attachment = open(fname_attachment_fullpath, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)
        
        #normal emails, no attachment
        server = smtplib.SMTP(self.server, self.server_port)
        server.starttls()
        server.login(fromaddr, self.password)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()




if __name__ == "__main__":
    main()

