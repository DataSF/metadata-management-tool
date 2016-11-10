from __future__ import division
from Emailer import *
from Utils import *


class MetaData_Email_Composer(object):
    '''
    composes emails to receipients
    '''
    def __init__(self, configItems, emailer):
        self._configItems =  configItems
        self._emailer = emailer
        self._emailer_configs = emailer._emailConfigs
        self._email_txt_basedir = self._emailer_configs['email_situations']['email_txt_basedir']
        self._email_situations = self._emailer_configs['email_situations']

    @staticmethod
    def getMsgText(email_txt_basedir, situation, filename):
        msgBody = None
        fn = email_txt_basedir + situation+ "/" + filename
        with open(fn, 'r') as myfile:
            msgBody=myfile.read().replace('\n', '')
        return msgBody

    def email_msg(self, receipient, subject_line, msgBody, attachment=None, attachment_fullpath=None ):
        if os.path.isfile(attachment_fullpath):
            self._emailer.sendEmails( receipient, subject_line, msgBody, attachment, attachment_fullpath)
        else:
            self._emailer.sendEmails( receipient, subject_line, msgBody)

    @staticmethod
    def get_msgparts(email_txt_basedir, situation, text_file_subparts):
        msg_subpart_dict = {}
        for txt_file in text_file_subparts:
            msg_subpart_dict[txt_file] = MetaData_Email_Composer.getMsgText(email_txt_basedir, situation, txt_file )
        return  msg_subpart_dict

class ForReviewBySteward(MetaData_Email_Composer):
    """emailer class for the For Review By Data Steward Step"""
    def __init__(self, configItems, emailer):
        MetaData_Email_Composer.__init__(self,configItems, emailer)
        self._situation = 'review_steward'
        self._subject_line = self._email_situations[self._situation]['subject_line']
        self._text_file_subparts =  self._email_situations[self._situation]['text_file_subparts']
        self.main_msg_file = self._email_situations[self._situation]['text_file_main']

    def getMsgBodyText(self):
        msg_parts = {}
        msg_parts = self.get_msgparts(self._email_txt_basedir, self._situation, self._text_file_subparts)
        msg_parts['main_msg'] = self.getMsgText(self._email_txt_basedir, self._situation, self.main_msg_file )
        return msg_parts

    @staticmethod
    def buildFinishedFieldsMsgBody(msgParts, wkbk):
        submsg = msgParts["email_field_cnt_finished.txt"]
        submitted_field_cnt = wkbk['submittedFields']['submitted_fields_cnt']
        total_fields = wkbk['submittedFields']['total_fields']  + submitted_field_cnt
        percent_done = str(wkbk['submittedFields']['percent_done'] ) + "%"
        to_do_fields_cnt = wkbk['submittedFields']['fields_to_do_cnt']
        submsg = submsg % (submitted_field_cnt, percent_done, to_do_fields_cnt )
        return submsg

    @staticmethod
    def buildUnfinishedFieldsMsgBody(msgParts, wkbk):
        submsg = msgParts["email_field_cnt_unfinished.txt"]
        #number_of_fields_to_document = str(sum([elem['count'] for elem in wkbk['datasets'] if 'count' in elem]))
        to_do_fields_cnt = wkbk['submittedFields']['fields_to_do_cnt']
        submsg = submsg % (  to_do_fields_cnt )
        return submsg

    def msgBodyFill(self, wkbk):
        msgParts = self.getMsgBodyText()
        steward_name = wkbk[ "data_cordinator"]["First Name"]
        worksheet_filename = self.wkbk_file_name(wkbk["path_to_wkbk"])
        submitted_fields = wkbk['submittedFields']['submitted']
        datasets_to_review = self.dataset_Name_and_Cnts(wkbk)
        if submitted_fields:
            submsg = self.buildFinishedFieldsMsgBody(msgParts, wkbk)
            #print submsg
        else:
            submsg =  self.buildUnfinishedFieldsMsgBody(msgParts, wkbk)
        msgBody = msgParts['main_msg'] % (steward_name, submsg, datasets_to_review, worksheet_filename )
        return msgBody.encode('ascii', 'ignore')

    @staticmethod
    def wkbk_file_name(wkbk_path):
        wkbk_path_list = wkbk_path.split("/")
        return wkbk_path_list[-1]

    def dataset_Name_and_Cnts(self, wkbk):
        dataset_html = ''
        keysToKeep = ['count', 'Dataset Name', 'datasetID']
        datasets = myUtils.filterDictList(wkbk['datasets'], keysToKeep)
        dataset_html = " ".join([ self.makeDatasetHtml(dataset) for dataset in datasets])
        return dataset_html

    @staticmethod
    def makeDatasetHtml(dataset):
        return "<tr><td>" + dataset["datasetID"] + "</td><td>" + dataset["Dataset Name"] + '</td><td class="count">' + str(dataset["count"]) + "</td></tr>"


    def generate_All_Emails(self, wkbks):
        '''generates and sends wkbks to recipients'''
        wkbks_sent_out = []
        print len(wkbks['workbooks'])
        for wkbk in wkbks['workbooks']:
            #if updated_list_json['updated'][wkbk[ "data_cordinator"]['Email']]:
            msgBody =  self.msgBodyFill(wkbk)
            receipient = wkbk[ "data_cordinator"]['Email']
            print receipient
            receipient = "janine.heiser@sfgov.org"
            subject_line = self._subject_line
            attachment_fullpath = wkbk["path_to_wkbk"]
            attachment = self.wkbk_file_name(wkbk["path_to_wkbk"])
            try:
                #print "sending email"
                self.email_msg(receipient, subject_line, msgBody, attachment, attachment_fullpath )
                wkbks_sent_out.append(wkbk)
            except Exception, e:
                print str(e)
        return wkbks_sent_out




if __name__ == "__main__":
    main()

