
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
        #self._documented_fields_list = [ 'columnid',  'datasetid', 'dataset_name',  'field_name',  'field_alias',  'field_definition', 'field_type_flag']

        self._cols_to_keep_master = ['status', 'datasetid', 'columnid', 'dataset_name', 'field_name', 'field_count', 'attachment_url']
        self._documented_fields_matched = [ 'columnid',  'field_definition', 'status', 'date_last_changed' ]
        self._documented_fields_unmatched = [ 'columnid',  'datasetid', 'dataset_name',  'field_name']
        self._pdf_others_fields_list = ['datasetid', 'dataset_name', 'field_count', 'attachment_url', 'file_name']
        self._wkbk_formats = configItems['wkbk_formats']
        #fields to keep from the screendoor wkbks
        self._cols_to_keep_uploaded = ['field_definition', 'field_name']

        self._cols_to_remove_basedf = ['field_count', 'attachment_url']
        self._current_date = DateUtils.get_current_date_month_day_year()
        self._document_fields_outputfile_fn = configItems['document_fields_outputfile_fn']
        self._pickle_data_dir = configItems['pickle_data_dir']

        self._metadata_config_fn = configItems['metadataset_config']
        self._metadataset_config = ConfigUtils.setConfigs(self._config_dir,  self._metadata_config_fn )
        self._master_dd_config = self._metadataset_config['master_data_dictionary']
        self._globalfields_config = self._metadataset_config['global_fields']
        self._df_master = MetaDatasets.set_master_df_dd(self._pickle_data_dir, self._master_dd_config['json_fn'])
        self._globalfields_list = MetaDatasets.set_global_fields_list(self._pickle_data_dir, self._globalfields_config['json_fn'])
        self._datasets_load_list = self.find_attachment_datasets()
        self._cnt_report_columns = ['datasetid', 'dataset_name', 'matched_cnt', 'only_in_master', 'tot_fields_in_attch', 'only_in_attached',  'tot_fields_in_master']


    def find_attachment_datasets(self):
      '''gets the datasets with download urls'''
      #print list(self._df_master['data_dictionary_attached'])
      attachment_df =  pd.DataFrame({'field_count' : self._df_master[ (self._df_master["data_dictionary_attached"] == True)].groupby(["datasetid", "dataset_name", "attachment_url"]).size()}).reset_index()
      return attachment_df.to_dict(orient='records')

    def get_dfDatasetToLoad(self, datasetID):
      df =  self._df_master[self._df_master['datasetid'] == datasetID]
      #make the field_name to lower
      df = PandasUtils.colToLower(df.copy(), 'field_name')
      #print '***here are all the cols in the df***'
      #print list(df.columns)
      #print "********"
      #print  self._cols_to_keep_master
      cols_to_remove = [col for col in list(df.columns) if col not in self._cols_to_keep_master]
      #print cols_to_remove
      #remove the cols that we will be updating
      df = PandasUtils.removeCols(df,cols_to_remove)
      if 'status' not in list(df.columns):
        df['status'] = ''
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


    def addStatusFields(self, dfAll):
      '''replaces the df definition and data types columns with the xlsx colums'''
      dfAll['status'] = "Complete"
      dfAll['date_last_changed'] = self._current_date
      return dfAll

    def getAttachmentSheet(self, dataset, fNameXlsx):
      '''returns of a dataframe of the data dictionary attachment info'''
      dfSht = None
      if(FileUtils.getFiles(self._documented_fields_dir+ "xlsx/", fNameXlsx , dataset['attachment_url'])):
        if(FileUtils.getAttachmentFullPath( self._documented_fields_dir + "xlsx/", fNameXlsx , dataset['attachment_url'])):
          wkbk_stuff = WkbkUtils.get_shts(self._documented_fields_dir+"xlsx/"+fNameXlsx)
          wkbkName = self._wkbk_formats['format1']['wksht_name']
          skipRows = self._wkbk_formats['format1']['skip_rows']
          try:
            dfSht = WkbkUtils.getShtDf(wkbk_stuff, wkbkName, skipRows )
            dfSht = self.formatAttachmentSht(dfSht)
          except Exception, e:
            print str(e)
            print wkbk_stuff
      return dfSht

    def formatAttachmentSht(self, dfSht):
      '''formats the dataframe of the attachment sheet so it can be merged with the master data dictionary more easily'''
      dshts_rename = { k:v for k, v in self._documented_fields_name_mapping.iteritems() if k in  list(dfSht.columns)}
      dfSht = PandasUtils.renameCols(dfSht, dshts_rename)
      #just keep the df name and the definition
      dfSht = dfSht[self._cols_to_keep_uploaded]
      #convert field name to lower for better match rate
      if 'field_name' in list(dfSht.columns):
        dfSht = PandasUtils.colToLower(dfSht, 'field_name')
      return dfSht


    def make_df_both(self, dataset_df, dfSht):
      self._cols_to_remove_basedf
      '''returns a dataframe of the fields that match in both '''
      df_both = pd.merge(dataset_df, dfSht, on='field_name')
      #filter out globals
      df_both =  df_both[(~df_both['field_name'].isin(self._globalfields_list))] #.reset_index()
      #get a cnt of fields that were previously complete
      cnt_prev_complete = len(df_both[df_both['status'] == 'Complete'])
      #remove rows where status is complete
      df_both = df_both[df_both['status'] != 'Complete'].reset_index()
      #add the status col info
      df_both = PandasUtils.removeCols(df_both, ['status'])
      df_both = self.addStatusFields(df_both)
       #filter out just the columns we want to update
      df_both = df_both[self._documented_fields_matched]
      return df_both, cnt_prev_complete

    def make_df_only_in_mm(self, dataset_df, dfSht):
      '''returns a df of the fields that only appear in the master dd'''
      df_only_in_mm = pd.merge(dataset_df, dfSht, how="left", on='field_name')
      df_only_in_mm = df_only_in_mm.fillna('')
      df_only_in_mm =  df_only_in_mm[df_only_in_mm['field_definition'] == '']
      df_only_in_mm =  df_only_in_mm[(~df_only_in_mm['field_name'].isin(self._globalfields_list))] #.reset_index()
      df_only_in_mm =  df_only_in_mm[self._documented_fields_unmatched]
      return df_only_in_mm

    def make_df_only_in_attch(self, dataset, dataset_df, dfSht):
      '''returns a df of the fields that are only in the attachment'''
      df_only_in_attch = pd.merge(dataset_df, dfSht, how="right", on='field_name')
      df_only_in_attch = df_only_in_attch.fillna('')
      df_only_in_attch = df_only_in_attch[df_only_in_attch['columnid']== '']
      df_only_in_attch =  df_only_in_attch[(~df_only_in_attch['field_name'].isin(self._globalfields_list))] #.reset_index()
      df_only_in_attch =  df_only_in_attch[self._documented_fields_unmatched]
      df_only_in_attch['datasetid'] = dataset['datasetid']
      df_only_in_attch['dataset_name'] = dataset['dataset_name']
      return df_only_in_attch

    def makeFieldLists(self, dataset, dataset_df, dfSht ):
      '''tries to match the attachment to the master dd and returns matched and unmatched'''
      #get fields that match
      df_both, cnt_prev_complete = self.make_df_both(dataset_df, dfSht)
      #get the fields that only appear in the master datadict
      df_only_in_mm = self.make_df_only_in_mm(dataset_df, dfSht)
      #get the fields that are only in attachment
      df_only_in_attch = self.make_df_only_in_attch(dataset, dataset_df, dfSht)
      #make a report of the cnts
      cnt_report = self.getCntsOnFieldMatch(dataset, dataset_df, dfSht, df_both, df_only_in_mm, df_only_in_attch, cnt_prev_complete)
      items = {'both': df_both, 'mm': df_only_in_mm, 'attch': df_only_in_attch}
      #turn all the dfs to dict lists
      dfs_as_rows =  {k:v.to_dict('records') for k,v in items.iteritems() }
      return cnt_report, dfs_as_rows

    def getCntsOnFieldMatch(self, dataset, dataset_df, dfSht, df_both, df_only_in_master, df_only_in_attachment, cnt_prev_complete):
      '''returns some cnts on match rate-removes globals fields from the counts'''
      attch_globals = len(dfSht[(dfSht['field_name'].isin(self._globalfields_list))])
      mm_globals = len(dataset_df[(dataset_df['field_name'].isin(self._globalfields_list))])
      cnt_report = {'datasetid': dataset['datasetid'],'dataset_name': dataset['dataset_name'],  'tot_fields_in_master': len(dataset_df)-mm_globals, 'tot_fields_in_attch': len(dfSht) - attch_globals, 'matched_cnt': len(df_both)+ cnt_prev_complete , 'only_in_master': len( df_only_in_master), 'only_in_attached': len(df_only_in_attachment) }
      cnt_report = {k:str(v) for k,v in cnt_report.iteritems() }
      return cnt_report

    def buildDocumentedFields(self):
      allOthers = []
      only_in_mm = []
      only_in_attch = []
      in_both = []
      allPdfs = []
      cnt_report_all =  []
      dowloaded = False
      for dataset in self._datasets_load_list:
        #get the main dataset
        datasetid = dataset['datasetid']
        dataset_df = self.get_dfDatasetToLoad(dataset['datasetid'])
        #print "***** here are the dataset columns****"
        #print dataset_df.columns
        #print "********"
        fNameXlsx = self.make_fnNameXlxs(dataset)
        if( fNameXlsx):
          print "***xlxs:"+ fNameXlsx + "******"
          dfSht = self.getAttachmentSheet(dataset, fNameXlsx)
          if (not(dfSht is None)):
            cnt_report, dfs_as_rows = self.makeFieldLists( dataset, dataset_df, dfSht)
            cnt_report_all.append(cnt_report)
            in_both  = in_both + dfs_as_rows['both']
            only_in_mm = only_in_mm  + dfs_as_rows['mm']
            only_in_attch = only_in_attch + dfs_as_rows['attch']
          else:
            dataset['file_name'] = fNameXlsx
            allOthers.append(dataset)
        else:
            fNamePdf = self.make_fnNamePdf(dataset)
            if(fNamePdf):
              if(FileUtils.getFiles(self._documented_fields_dir+ "pdf/", fNamePdf , dataset['attachment_url'])) :
                print "***pdf:"+ fNamePdf + "****"
                dataset['file_name'] = fNamePdf
                allPdfs.append(dataset)
            else:
              fnameOther = self.make_fnNameOther(dataset)
              if(FileUtils.getFiles(self._documented_fields_dir+ "other/", fnameOther , dataset['attachment_url'])) :
                print "********other*******"
                print fnameOther
                dataset['file_name'] = fnameOther
                allOthers.append(dataset)
                print "********************"

      wroteFileDefs = FileUtils.write_wkbk_csv(self._documented_fields_dir +"output/" + self._document_fields_outputfile_fn, in_both, self._documented_fields_matched)
      wroteFileOnlyInAttach =  FileUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "fields_only_in_attachments.csv", only_in_attch,  self._documented_fields_unmatched)
      wroteFileOnlyInMaster =  FileUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "fields_only_in_master_dd.csv", only_in_mm, self._documented_fields_unmatched)
      wrotePdfDatasets = FileUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "pdf_datasets.csv", allPdfs,self._pdf_others_fields_list)
      wroteOthersDatasets = FileUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "other_datasets.csv", allOthers,self._pdf_others_fields_list)
      wroteCntReport = FileUtils.write_wkbk_csv(self._documented_fields_dir + "output/"+ "match_report.csv",cnt_report_all, self._cnt_report_columns)
      return {'match_report.csv':self._documented_fields_dir + "output/"+ "match_report.csv"}, wroteFileDefs, wroteFileOnlyInAttach, wroteFileOnlyInMaster, wrotePdfDatasets, wroteOthersDatasets



if __name__ == "__main__":
    main()


