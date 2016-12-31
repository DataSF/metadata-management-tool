
from __future__ import division
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle
from openpyxl import Workbook
import json
import inflection
from Utils import *
import re
import requests
import shutil
from MetaDatasets import *
from ConfigUtils import *


class ExistingFieldDefs:
    """class to generate data dictionaries"""
    def __init__(self,  configItems):
        self._config_dir = configItems['config_dir']
        self._documented_fields_dir = configItems['documented_fields_dir']
        self._documented_fields_name_mapping = { 'datasetID': 'datasetid', 'columnID': 'columnid', 'Dataset Name': 'dataset_name', 'Field Name': 'field_name', 'Field Alias': 'field_alias', 'Definition': 'field_definition', 'Field Type Flag':'field_type_flag','Status': 'status', 'date_last_changed': 'date_last_changed', 'Data Type':'field_type'  }
        self._documented_fields_list = [ 'columnid',  'datasetid', 'dataset_name',  'field_name',  'field_alias',  'field_definition', 'field_type_flag']
        self._pdf_others_fields_list = ['datasetid', 'dataset_name', 'field_count', 'attachment_url']
        self._wkbk_formats = configItems['wkbk_formats']
        self._cols_to_remove_basedf = ['field_definition', 'status', 'date_last_changed']
        self._cols_to_keep_uploaded = ['field_definition', 'field_name']
        self._current_date = DateUtils.get_current_date_month_day_year()
        self._document_fields_outputfile_fn = configItems['document_fields_outputfile_fn']
        self._pickle_data_dir = configItems['pickle_data_dir']
        self._metadata_config_fn = configItems['metadataset_config']
        self._metadataset_config = ConfigUtils.setConfigs(self._config_dir,  self._metadata_config_fn )
        self._master_dd_config = self._metadataset_config['master_data_dictionary']
        self._master_dd_fn = self._master_dd_config['json_fn']
        self._df_master = MetaDatasets.set_master_df_dd(self._pickle_data_dir, self._master_dd_config['json_fn'])
        self._datasets_load_list = self.find_attachment_datasets()
        #print self._datasets_load_list

    def find_attachment_datasets(self):
      '''gets the datasets with download urls'''
      #print list(self._df_master['data_dictionary_attached'])
      attachment_df =  pd.DataFrame({'field_count' : self._df_master[ (self._df_master["data_dictionary_attached"] == True)].groupby(["datasetid", "dataset_name", "attachment_url"]).size()}).reset_index()
      return attachment_df.to_dict(orient='records')

    def get_dfDatasetToLoad(self, datasetID):
      df =  self._df_master[self._df_master['datasetid'] == datasetID]
      #make the field_name to lower
      df = PandasUtils.colToLower(df.copy(), 'field_name')
      return df

    @staticmethod
    def make_fnNameXlxs(dataset):
      fName = False
      urlStuff  =  dataset['attachment_url'].split('&filename=')
      #download the attachment for the dataset
      fnPretty = urlStuff[1]
      searchObj = re.search( r'.xlsx', fnPretty)
      if(searchObj):
        return fnPretty
      return fName

    @staticmethod
    def make_fnNamePdf(dataset):
      fName = False
      urlStuff  =  dataset['attachment_url'].split('&filename=')
      #download the attachment for the dataset
      fnPretty = urlStuff[1]
      searchObj = re.search( r'.pdf', fnPretty)
      if(searchObj):
        return fnPretty
      return fName

    @staticmethod
    def make_fnNameOther(dataset):
      fName = False
      urlStuff  =  dataset['attachment_url'].split('&filename=')
      #download the attachment for the dataset
      fnPretty = urlStuff[1]
      #searchObj = re.search( r'.pdf', fnPretty)
      if(fnPretty):
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
      dfAll['status'] = "Complete"
      dfAll['date_last_changed'] = self._current_date
      return dfAll


    def buildDocumentedFields(self):
      allOthers = []
      allBlanks = []
      allDefs = []
      allPdfs = []
      dowloaded = False
      for dataset in self._datasets_load_list:
        #get the main dataset
        dataset_id = dataset['datasetid']
        dataset_df = self.get_dfDatasetToLoad(dataset['datasetid'])
        fNameXlsx = self.make_fnNameXlxs(dataset)
        if( fNameXlsx):
            print "***xlxs:"+ fNameXlsx + "******"
            if(FileUtils.getFiles(self._documented_fields_dir+ "xlsx/", fNameXlsx , dataset['attachment_url'])):
              if(FileUtils.getAttachmentFullPath( self._documented_fields_dir + "xlsx/", fNameXlsx , dataset['attachment_url'])):
                wkbk_stuff = self.get_shts(self._documented_fields_dir+"xlsx/"+fNameXlsx)
                wkbkName = self._wkbk_formats['format1']['wksht_name']
                skipRows = self._wkbk_formats['format1']['skip_rows']
                try:
                  dfSht = self.getShtDf(wkbk_stuff, wkbkName, skipRows )
                except Exception, e:
                  print str(e)
                  print wkbk_stuff
                if dfSht.any:
                  dshts_rename = { k:v for k, v in self._documented_fields_name_mapping.iteritems() if k in  list(dfSht.columns)}
                  dfSht = PandasUtils.renameCols(dfSht, dshts_rename)
                  #just keep the df name and the definition
                  dfSht = dfSht[self._cols_to_keep_uploaded]
                  #convert field name to lower for better match rate
                  if 'field_name' in list(dfSht.columns):
                    dfSht = PandasUtils.colToLower(dfSht, 'field_name')
                  #remove the cols that we will be updating
                  datset_df_clipped = PandasUtils.removeCols(dataset_df, self._cols_to_remove_basedf)

                  df_all = pd.merge(datset_df_clipped, dfSht, how='left', on='field_name')
                  if dataset_id == "2wsq-7wmv":
                    print
                    print "***************"
                    print "fields in master dd"
                    print list(datset_df_clipped['field_name'])
                    print len(datset_df_clipped)
                    print "fields in sht"
                    print list(dfSht['field_name'])
                    print len(dfSht)
                    print "**match rate*"
                    match_rate =  ((len(df_all)-len(dfSht))/len(df_all))*100
                    print match_rate
                    print "*********"

                  df_all = df_all[df_all['global_field'] == False]
                  #match_rate =  ((len(df_all)-len(dfSht))/len(df_all))*100
                  #print match_rate
                  df_all = df_all[self._documented_fields_list]
                  df_all = df_all.fillna('')
                  df_blanks = df_all[df_all['field_definition'] == '']
                  df_defs = df_all[df_all['field_definition'] != '']
                  df_defs = self.addStatusFields(df_defs)
                  blanksList =  df_blanks.to_dict('records')
                  if dataset_id == "2wsq-7wmv":
                    print blanksList
                  defList = df_defs.to_dict('records')
                  allBlanks  = allBlanks  +  blanksList
                  allDefs = allDefs + defList
        else:
            fNamePdf = self.make_fnNamePdf(dataset)
            if(fNamePdf):
              if(FileUtils.getFiles(self._documented_fields_dir+ "pdf/", fNamePdf , dataset['attachment_url'])) :

                print "***pdf:"+ fNamePdf + "****"
                try:
                    allPdfs.append(dataset)

                except Exception, e:
                  print str(e)
            else:
              fnameOther = self.make_fnNameOther(dataset)
              if(FileUtils.getFiles(self._documented_fields_dir+ "other/", fnameOther , dataset['attachment_url'])) :
                print "********other*******"
                print
                print
                print dataset
                print "********************"
      #print allFields
      wroteFileDefs = FileUtils.write_wkbk_csv(self._documented_fields_dir +"output/" + self._document_fields_outputfile_fn, allDefs, self._documented_fields_list)
      wroteFileBlanks =  FileUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "blank_fields.csv", allBlanks, self._documented_fields_list)
      wrotePdfDatasets = FileUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "pdf_datasets.csv", allPdfs,self._pdf_others_fields_list)

      print wroteFileDefs
      print wroteFileBlanks
      print wrotePdfDatasets



if __name__ == "__main__":
    main()


