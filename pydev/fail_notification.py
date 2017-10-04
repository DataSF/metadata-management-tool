 
from Emailer import *
from DictUtils import *




 e = Emailer(self.configItems)
        emailconfigs = e.setConfigs()
        if((os.path.isfile(self.logfile_fullpath)) and (msg_attachments is None)):
            e.sendEmails( subject_line, msgBody, self.logfile_fname, self.logfile_fullpath)
        elif msg_attachments:
            e.sendEmails( subject_line, msgBody, None, None, None, msg_attachments)
        else:
            e.sendEmails( subject_line, msgBody)
        logger_msg =  "****JOB STATUS: " +  self.job_name + "*:* "+ subject_line +"***"
        print logger_msg
        print "Email Sent!"
        self._logger.info(logger_msg)
        self._logger.info( subject_line)