# coding: utf-8


import json
import os.path

class WkbkJson: 
    def __init__(self, configItems):
        #self._json_item = {"data_cordinator": None , "path_to_wkbk":None , "datasets":None ,"timestamp":None}
        self._wkbk_output_json = configItems['wkbk_output_json']
        self._wkbk_output_dir = configItems["wkbk_output_dir"]
        self.self._wkbk_uploads_dir = configItems['wkbk_uploads_dir']
        self._json_obj = self.loadJsonObject()
        self._wkbks = self.setWkbks()
    
    @property
    def json_obj(self):
        return self._json_obj
        
    def loadJsonObject(self):
        json_obj =  { "workbooks": []}
        if os.path.isfile( self._wkbk_output_dir + self._wkbk_output_json):
            json_data = open(self._wkbk_output_dir + self._wkbk_output_json).read()
            json_obj =  json.loads(json_data)
        return json_obj
    
    @staticmethod
    def loadJsonFile(path_to_file, json_fn):
        if os.path.isfile( path_to_file + json_fn):
            json_data = open(path_to_file + json_fn).read()
            json_obj =  json.loads(json_data)
        return json_obj
    
    @property
    def wkbks(self):
       return self._wkbks
       
    def setWkbks(self):
        return self._json_obj["workbooks"]
    
    def write_json_object_wkbks(self, json_object):
        wroteFile = False
        try:
            with open(self._wkbk_output_dir + self._wkbk_output_json, 'w') as f:
                json.dump(json_object, f, ensure_ascii=False)
                self._json_obj  = json_object
                self.wkbks =  self.setWkbks()
                wroteFile = True
        except Exception, e:
            print str(e)
        return wroteFile
    
    @staticmethod
    def write_json_object(json_object, output_dir, json_fn):
        wroteFile = False
        try:
            with open(output_dir + json_fn, 'w') as f:
                json.dump(json_object, f, ensure_ascii=False)
                wroteFile = True
        except Exception, e:
            print str(e)
        return wroteFile
        
if __name__ == "__main__":
    main()