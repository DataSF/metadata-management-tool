

# coding: utf-8
import datetime
import pandas as pd
import xlrd
from Utils import *
from Wkbk_Json import *

class WkbkParser:
    '''class to parse wkbk responses'''
    def __init__(self, configItems):
        #self.keys_to_keep = ['columnID', 'Field Type', 'Field Definition', 'Field Alias', 'Field Type Flag']
        self.keys_to_keep = ['columnID', 'Field Definition', 'Field Alias', 'Field Type Flag']
        self.updt_fields_json_name =  datetime.datetime.now().strftime("%Y_%m_%d_") + configItems['updt_fields_json_fn']
        self.json_key = "updt_fields"
        self.wkbk_uploads_dir = configItems['wkbk_uploads_dir']
        self.field_positions = configItems['update_info']['field_positions']
        self.field_name_mappings = configItems['update_info']['field_name_mappings']
        
    @staticmethod
    def getWkbk(fn):
        wkbk = pd.ExcelFile(fn)
        return wkbk
    
    @staticmethod
    def get_sht_names(wkbk):
        shts =  wkbk.sheet_names
        return [ sht for sht in shts if sht != 'Dataset Summary']
    
    def parse_sht(self, wkbk, sht_name):
        df_wkbk =  wkbk.parse(sht_name)
        df_wkbk = df_wkbk[self.keys_to_keep]
        #rename the cols to the spreadsheet cols for ease
        field_name_mapping =  dict((v,k) for k,v in self.field_name_mappings.iteritems())
        field_mappings = myUtils.filterDict(self.field_positions, self.field_name_mappings.keys())
        df_wkbk= df_wkbk.rename(columns = field_name_mapping)
        df_wkbk= df_wkbk.rename(columns = field_mappings)
        df_dictList =  df_wkbk.to_dict(orient='records')
        #filter out the nans
        return  [myUtils.filterDictOnNans(field_dict) for field_dict in df_dictList]
        
    def get_shts(self, fn):
        wkbk = self.getWkbk(fn)
        sht_names = self.get_sht_names(wkbk)
        metadata_dicts = [ self.parse_sht(wkbk, sht_name) for sht_name in sht_names]
        return myUtils.flatten_list(metadata_dicts)
        
    def get_metadata_updt_fields_from_shts(self, fileList):
        wroteJsonFile = False
        metadata_dicts = [ self.get_shts(fn) for fn in fileList ]
        metadata_dictJson = { self.json_key: myUtils.flatten_list(metadata_dicts)}
        try:
            wroteJsonFile = WkbkJson.write_json_object( metadata_dictJson, self.wkbk_uploads_dir, self.updt_fields_json_name)
        except Exception, e:
            print str(e)
        return wroteJsonFile  
            
    def updt_fields(self):
        updt_fieldList = []
        try:
            jsonList = WkbkJson.loadJsonFile(self.wkbk_uploads_dir, self.updt_fields_json_name)
            updt_fieldList = jsonList[self.json_key]
        except Exception, e:
            print str(e)
        return updt_fieldList
        

if __name__ == "__main__":
    main()