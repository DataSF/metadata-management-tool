from Emailer import *
from Utils import *


class MetaData_Email_Composer(object):
    '''
    composes emails to receipients
    '''
    def __init__(self, configItems, emailer):
        self._configItems =  configItems
        self._emailer = emailer
        self._emailer_configs = emailer.getEmailerConfigs()
        self._email_txt_dir = self._emailer_configs['email_situations']['email_txt_dir']
        self._email_situations = self._emailer_configs['email_situations']
        
    def getMsgText(self, situation):
        msgBody = None
        fn =  self._email_txt_dir + self._email_situations[situation]['text_file']
        with open(fn, 'r') as myfile:
            msgBody=myfile.read().replace('\n', '')
        return msgBody
    
    def email_msg(self, receipient, subject_line, msgBody, attachment=None, attachment_fullpath=None ):
        if os.path.isfile(attachment_fullpath):
            self._emailer.sendEmails( receipient, subject_line, msgBody, attachment, attachment_fullpath)
        else:
            self._emailer.sendEmails( receipient, subject_line, msgBody)
        
class ForReviewBySteward(MetaData_Email_Composer):
    """emailer class for the For Review By Data Steward Step"""
    def __init__(self,configItems, emailer):
        MetaData_Email_Composer.__init__(self,configItems, emailer)
        self._situation = 'review_steward'
        self._subject_line = self._email_situations[self._situation]['subject_line']
    
  
    def get_subject_line(self):
        return self._subject_line
    
    def msgBodyFill(self, wkbk):
        msgBody = self.getMsgText(self._situation)
        steward_name = wkbk[ "data_cordinator"]["First Name"]
        worksheet_filename = self.wkbk_file_name(wkbk["path_to_wkbk"])
        number_of_fields_to_document = str(sum([elem['count'] for elem in wkbk['datasets'] if 'count' in elem]))
        datasets_to_review = self.dataset_Name_and_Cnts(wkbk)
        msgBody = msgBody % (steward_name, number_of_fields_to_document, datasets_to_review, worksheet_filename )
        return msgBody.encode('ascii', 'ignore')
    
    @staticmethod
    def wkbk_file_name(wkbk_path):
        wkbk_path_list = wkbk_path.split("/")
        return wkbk_path_list[-1]
    
    def dataset_Name_and_Cnts(self, wkbk):
        dataset_html = ''
        keysToKeep = ['count', 'Dataset Name']
        datasets = myUtils.filterDictList(wkbk['datasets'], keysToKeep)
        dataset_html = " ".join([ self.makeDatasetHtml(dataset) for dataset in datasets])
        return dataset_html
        
    @staticmethod
    def makeDatasetHtml(dataset):
        return "<tr><td>" + dataset["Dataset Name"] + '</td><td class="count">' + str(dataset["count"]) + "</td></tr>"
    
    
    def generate_All_Emails(self, afterCreateDataDicts):
        for wkbk in afterCreateDataDicts._wkbks:
            msgBody =  self.msgBodyFill(wkbk)
            #receipient = wkbk[ "data_cordinator"]['Email']
            receipient = "janine.heiser@sfgov.org"
            subject_line = self.get_subject_line()
            attachment_fullpath = wkbk["path_to_wkbk"]
            attachment = self.wkbk_file_name(wkbk["path_to_wkbk"])
            self.email_msg(receipient, subject_line, msgBody, attachment, attachment_fullpath )
            
  
    
    
    
if __name__ == "__main__":
    main()

