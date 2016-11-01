from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle
from openpyxl import Workbook
import json
import inflection
from Utils import *
import re
import requests

#from GSpread_Stuff import *
import shutil
from tabula import read_pdf_table

##
class ExistingFieldDefs:
    """class to generate data dictionaries"""
    def __init__(self,  configItems, cells_dataDict=None, cells_stewards=None):
        self._documented_fields_dir = configItems['documented_fields_dir']
        self._df_master = self.set_master_df(cells_dataDict)
        self._datasetsToLoadList = self.set_datasetsToLoadList()
        self._documented_fields_name_mapping = { 'datasetID': 'systemID', 'columnID': 'columnID', 'Dataset Name': 'dataset_name', 'Field Name': 'field_name', 'Field Alias': 'field_alias', 'Definition': 'field_definition', 'Field Type Flag':'field_type_flag','Status': 'status', 'date_last_changed': 'date_last_changed'  }
        self._documented_fields_list = [ 'columnID',  'systemID', 'dataset_name',  'field_name',  'field_alias',  'field_definition', 'field_type_flag','date_last_changed' ]
        self._wkbk_formats = configItems['wkbk_formats']
        self._current_date = datetime.datetime.now().strftime("%m/%d/%Y")
        self._document_fields_outputfile_fn = configItems['document_fields_outputfile_fn']

    def get_master(self):
      return self._df_master

    @staticmethod
    def set_master_df(cells):
      '''creates a dataframe of the master field list'''
      if cells:
          df = pd.DataFrame(cells)
          return df[ (df["Data Dictionary Attached"] == "TRUE" ) & (df['datasetID'] != '#N/A')  & (df['Global Field'] == 'FALSE') ]

    def set_datasetsToLoadList(self):
      '''gets the datasets with download urls'''
      datasets_df =  pd.DataFrame({'count' : self._df_master[ (self._df_master["Data Dictionary Attached"] == "TRUE")].groupby(["inventoryID", "datasetID", "Dataset Name", "Attachment URL"]).size()}).reset_index()
      return datasets_df.to_dict(orient='records')

    def get_datasetsToLoadList(self):
      return self._datasetsToLoadList

    def get_dfDatasetToLoad(self, datasetID):
      return self._df_master[self._df_master['datasetID'] == datasetID]


    @staticmethod
    def make_fnNameXlxs(dataset):
      fName = False
      urlStuff  =  dataset['Attachment URL'].split('&filename=')
      #download the attachment for the dataset
      fnPretty = urlStuff[1]
      searchObj = re.search( r'.xlsx', fnPretty)
      if(searchObj):
        return fnPretty
      return fName

    @staticmethod
    def make_fnNamePdf(dataset):
      fName = False
      urlStuff  =  dataset['Attachment URL'].split('&filename=')
      #download the attachment for the dataset
      fnPretty = urlStuff[1]
      searchObj = re.search( r'.pdf', fnPretty)
      if(searchObj):
        return fnPretty
      return fName

    @staticmethod
    def get_shts(fn):
      wkbk = ShtUtils.getWkbk(fn)
      sht_names = ShtUtils.get_sht_names(wkbk)
      return {'wkbk': wkbk, 'shts': sht_names}

    @staticmethod
    def getShtDf(wkbk_stuff, wkbkName, skipRows):
      '''turns a wksht into a df based on a name and the number of rows to skip'''
      dfSht = False
      df = wkbk_stuff['wkbk'].parse(wkbkName, header=skipRows )
      dfCols = list(df.columns)
      if len(dfCols) > 3:
        return df
      return dfSht


    def addStatusFields(self, dfAll):
      '''replaces the df definition and data types columns with the xlsx colums'''
      #dfAll['final_approval'] = "Yes"
      dfAll['date_last_changed'] = self._current_date
      return dfAll


    def buildDocumentedFields(self):
      allFields = []
      dowloaded = False
      for dataset in self._datasetsToLoadList[0:10]:
        #get the main dataset
        dataset_df = self.get_dfDatasetToLoad(dataset['datasetID'])

        fName = self.make_fnNameXlxs(dataset)
        if(fName):
          #if(myUtils.getFiles(self._documented_fields_dir, fName , dataset['Attachment URL'])) :
          if(myUtils.getAttachmentFullPath( self._documented_fields_dir, fName , dataset['Attachment URL'])):
            wkbk_stuff = self.get_shts(self._documented_fields_dir+fName)
            wkbkName = self._wkbk_formats['format1']['wksht_name']
            skipRows = self._wkbk_formats['format1']['skip_rows']
            try:
              dfSht = self.getShtDf(wkbk_stuff, wkbkName, skipRows )
            except Exception, e:
              print str(e)
              print wkbk_stuff
            if dfSht.any:
              df_all = pd.merge(dataset_df, dfSht, how='left', on='Field Name')
              df_all = PandasUtils.renameCols(df_all, self._documented_fields_name_mapping)
              df_all = self.addStatusFields(df_all)
              df_all = df_all[self._documented_fields_list]
              df_all = df_all.fillna('')
              df_allList =  df_all.to_dict('records')
              allFields  = allFields  + df_allList
        else:
          fName = self.make_fnNamePdf(dataset)
          if(fName):
            print "***pdf****"
            if(myUtils.getAttachmentFullPath( self._documented_fields_dir, fName , dataset['Attachment URL'])):
              print "cool!"
              try:
                print fName
                df = read_pdf_table(self._documented_fields_dir+fName, area=[0, 0, 612, 792])
                #print df.columns
                #mycsv = myUtils.pdf_to_csv(self._documented_fields_dir+fName)
                #print mycsv
                #print mycsv.split("\n")

                #subprocess.call("pdf2htmlEX", '--zoom', '1.3', self._documented_fields_dir+fName, shell=True)
              except Exception, e:
                print str(e)
          else:
            print "********failed!*******"
            print
            print
            print dataset
            print "********************"
      #print allFields
      wroteFile = myUtils.write_wkbk_csv(self._documented_fields_dir + self._document_fields_outputfile_fn, allFields, self._documented_fields_list)
      print
      print wroteFile




if __name__ == "__main__":
    main()


