
# coding: utf-8
from ConfigUtils import *
from Utils import *
import pycurl
from io import BytesIO
import json


class ScreenDoorStuff:
    '''class to handle screen door stuff'''
    def __init__(self, configItems):
        self._config_dir =  configItems['config_dir']
        self._screendoor_config_file = configItems['screendoor_config_file']
        self._screendoor_configs = myUtils.setConfigs(self._config_dir, self._screendoor_config_file)
        self._api_key = self._screendoor_configs['api_key']
        self._projectid = str(self._screendoor_configs['projectid'])
        self._responses_url = self.set_responsesUrl()
        self._attachment_url = self._screendoor_configs['attachment_url']
        self._responses = self.set_reponses()
        self._wkbk_uploads_dir = self._screendoor_configs['wkbk_uploads_dir']
        self._files_to_download = self.set_FileInfo()
        self._wkbk_uploads_json_fn = self._screendoor_configs['wkbk_uploads_json_fn']
        
  
    def set_responsesUrl(self):
        responses_url = self._screendoor_configs['responses_url']
        responses_url =  responses_url % (self._projectid,self._api_key)
        return responses_url
        
    def set_reponses(self):
        responses = ""
        try:
            buffer = BytesIO()
            c = pycurl.Curl()
            c.setopt(c.URL, self._responses_url)
            c.setopt(c.WRITEDATA, buffer)
            responses = c.perform()
            c.close()
            body = buffer.getvalue()
            # Body is a byte string.
            # We have to know the encoding in order to print it to a text file such as standard output.
            responses = json.loads(body.decode('iso-8859-1'))
        except Exception, e:
            print str(e)
        return responses

    def set_FileInfo(self):
        files_to_download = []
        for response in self._responses:
            response_items =  response['responses'].keys()
            for item in response_items:
                response_id = response['responses'][item]
                for item in response['responses'][item]['files']:
                    files_to_download.append(item)
        return files_to_download 
    
    def setDownloadUrl(self, fileId):
        return self._attachment_url % (fileId)
        
    def getAttachment(self, fileId, filename):
        '''downloads an attachment from screen door using the fileid and filename'''
        #equivelent to: curl -L "https://screendoor.dobt.co/attachments/s5wflD750Nxhai9MfNmxes4TR-0xoDyw/download" > whateverFilename.csv
        # As long as the file is opened in binary mode, can write response body to it without decoding.
        download_url = self.setDownloadUrl(fileId)
        downloaded = False
        try:
            with open(self._wkbk_uploads_dir + filename, 'wb') as f:
                c = pycurl.Curl()
                c.setopt(c.URL, download_url)
                # Follow redirect.
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
                downloaded = True
        except Exception, e:
            print str(e)
        return downloaded 
    
    def get_attachments(self):
        '''downloads a list of files'''
        downloaded_files = { "uploaded_workbooks": []}
        for file in self._files_to_download:
            downloaded_files[ "uploaded_workbooks"].append( { file['filename']: self.getAttachment(file['id'], file['filename'])})
        return downloaded_files
        
        
    
if __name__ == "__main__":
    main()
