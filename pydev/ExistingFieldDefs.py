from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle
from openpyxl import Workbook
import json
import inflection
from Utils import *
import re

##
class ExistingFieldDefs:
    """class to generate data dictionaries"""
    def __init__(self,  configItems, cells_dataDict=None, cells_stewards=None):
        self._documented_fields_dir = configItems['documented_fields_dir']
        self._df_master = self.set_master_df(cells_dataDict)
        self._datasetsToLoadList = self.set_datasetsToLoadList()

    def get_master(self):
      return self._df_master

    @staticmethod
    def set_master_df(cells):
      '''creates a dataframe of the master field list'''
      if cells:
          df = pd.DataFrame(cells)
          return df[ (df["Data Dictionary Attached"] == "TRUE" ) & (df['datasetID'] != '#N/A') ]

    def set_datasetsToLoadList(self):
      '''gets the datasets with download urls'''
      datasets_df =  pd.DataFrame({'count' : self._df_master[ (self._df_master["Data Dictionary Attached"] == "TRUE")].groupby(["inventoryID", "datasetID", "Dataset Name", "Attachment URL"]).size()}).reset_index()
      return datasets_df.to_dict(orient='records')

    def get_datasetsToLoadList(self):
      return self._datasetsToLoadList

    def get_dfDatasetToLoad(self, datasetID):
      return self._df_master[self._df_master['datasetID'] == datasetID]


    @staticmethod
    def make_fnName(dataset):
      fName = False
      urlStuff  =  dataset['Attachment URL'].split('&filename=')
      #download the attachment for the dataset
      fnPretty = urlStuff[1]
      searchObj = re.search( r'.xlsx', fnPretty)
      if(searchObj):
        return fnPretty
      return fName


    @staticmethod
    def get_shts(fn):
      wkbk = ShtUtils.getWkbk(fn)
      sht_names = ShtUtils.get_sht_names(wkbk)
      return {'wkbk': wkbk, 'shts': sht_names}

    def buildDocumentedFields(self):
      dowloaded = False
      for dataset in self._datasetsToLoadList[0:10]:
        print "*****"
        print dataset
        print "******"
        #get the main dataset
        dataset_df = self.get_dfDatasetToLoad(dataset['datasetID'])
        fName = self.make_fnName(dataset)
        if(fName):
          if(myUtils.getAttachmentFullPath( self._documented_fields_dir, fName , dataset['Attachment URL'])):
            print "success!!"
            wkbk_stuff = self.get_shts(self._documented_fields_dir+fName)
            print wkbk_stuff
          else:
            print "failed"





if __name__ == "__main__":
    main()


