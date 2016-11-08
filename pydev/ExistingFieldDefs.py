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
        self._pdf_others_fields_list = ['datasetID', 'Dataset Name', 'Field count', 'Attachment URL']
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
      datasets_df =  pd.DataFrame({'Field count' : self._df_master[ (self._df_master["Data Dictionary Attached"] == "TRUE")].groupby(["datasetID", "Dataset Name", "Attachment URL"]).size()}).reset_index()
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
      allBlanks = []
      allDefs = []
      allPdfs = []
      dowloaded = False
      for dataset in self._datasetsToLoadList[0:10]:
        #get the main dataset
        dataset_df = self.get_dfDatasetToLoad(dataset['datasetID'])

        fName = self.make_fnNameXlxs(dataset)
        if(fName):
          #if(myUtils.getFiles(self._documented_fields_dir, fName , dataset['Attachment URL'])) :
          if(myUtils.getAttachmentFullPath( self._documented_fields_dir + "xlsx", fName , dataset['Attachment URL'])):
            wkbk_stuff = self.get_shts(self._documented_fields_dir+"xlsx/"+fName)
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
              df_blanks = df_all[df_all['field_definition'] == '']
              df_defs = df_all[df_all['field_definition'] != '']
              blanksList =  df_blanks.to_dict('records')
              defList = df_defs.to_dict('records')
              allBlanks  = allBlanks  +  blanksList
              allDefs = allDefs + defList
        else:
          fName = self.make_fnNamePdf(dataset)
          if(fName):
            print "***pdf****"
            if(myUtils.getAttachmentFullPath( self._documented_fields_dir+"pdf/", fName , dataset['Attachment URL'])):
              try:
                print "in here"
                allPdfs.append(dataset)

              except Exception, e:
                print str(e)
          else:
            print "********failed!*******"
            print
            print
            print dataset
            print "********************"
      #print allFields
      wroteFileDefs = myUtils.write_wkbk_csv(self._documented_fields_dir +"output/" + self._document_fields_outputfile_fn, allDefs, self._documented_fields_list)
      wroteFileBlanks =  myUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "blank_fields.csv", allBlanks, self._documented_fields_list)
      wrotePdfDatasets = myUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "pdf_datasets.csv", allPdfs,self._pdf_others)

      print wroteFileDefs
      print wroteFileBlanks
      print wrotePdfDatasets



if __name__ == "__main__":
    main()


