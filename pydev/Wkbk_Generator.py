# coding: utf-8


from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
#from openpyxl import Workbook
import json
import inflection
from Wkbk_Json import *
from PandasUtils import *
from ConfigUtils import *
from WkBk_Writer import *
from MetaDatasets import *

class WkbkGenerator:
    """class to generate data dictionaries"""
    def __init__(self,  configItems, logger):
        self.sheet_keys = configItems['sheet_keys']
        self._config_inputdir = configItems['config_dir']
        self._wkbk_output_json = configItems['wkbk_output_json']
        self._json_object = { "workbooks": []}
        self._metadata_config_fn = configItems['metadataset_config']
        self._metadataset_config = ConfigUtils.setConfigs(self._config_inputdir,  self._metadata_config_fn )
        self._master_dd_config = self._metadataset_config['master_data_dictionary']
        self._fieldtypes_config = self._metadataset_config['field_types']
        self._pickle_dir = configItems['pickle_dir']
        self._pickle_data_dir = configItems['pickle_data_dir']
        self._wkbk_json = WkbkJson(configItems, logger)
        self._df_master = MetaDatasets.set_master_df(self._pickle_data_dir, self._master_dd_config['json_fn'])
        #print "**"
        #print len(self._df_master)
        #print
        self._df_master = self._df_master[self._df_master['privateordeleted'] == False ].reset_index()
        #print "**"
        #print len(self._df_master)
        #print
        self._df_stewards = self.set_df_stewards()
        self._stewardsList = self.set_stewardsList()
        self._field_types_list = self.set_field_types_list()
        self._wkbk_writer = WkBkWriter(configItems, logger, self._field_types_list)
        self._valsToNotOverride = configItems['valsToNotOverride']
        #self._current_date = datetime.datetime.now().strftime("%m/%d/%Y")
        self._current_date = DateUtils.get_current_date_month_day_year()


    @property
    def df_master(self):
        return self._df_master


    def set_field_types_list(self):
        json_obj = self._wkbk_json.loadJsonFile(self._pickle_data_dir, self._fieldtypes_config['json_fn'])
        df = PandasUtils.makeDfFromJson(json_obj)
        field_types = list(df['field_type'])
        return field_types

    @property
    def df_stewards(self):
        return self._df_stewards

    def set_df_stewards(self):
        '''sets a dataframe of datastewards and counts based on main dataframe'''
        #return pd.DataFrame({'count' : self._df_master.groupby("data_steward").size()}).reset_index()
        return PandasUtils.groupbyCountStar(self._df_master, ['data_steward', 'data_steward_name', 'department'])

    @property
    def stewardsList(self):
        return self._stewardsList

    def set_stewardsList(self):
        '''creates a list of data steward email adddresses'''
        return list(set(list(self._df_stewards['data_steward'])))

    @property
    def df_stewardsInfo(self):
        return self._df_stewardsInfo

    def set_df_stewardsInfo(self, cells):
        '''creates a dataframe of datasteward info'''
        if cells:
            df = pd.DataFrame(cells)
            return df[self.steward_keys]

    @staticmethod
    def generate_blank_stwd_info(stwd):
        '''generates blank steward info when stwd_info is blank'''
        stwd_info = {"First Name": None, "Last Name": None, "Email": None}
        nameList = stwd.split("@")
        nameList = nameList[0].split(".")
        if len(nameList)> 1:
            stwd_info = {"First Name": inflection.titleize(nameList[0]), "Last Name": inflection.titleize(nameList[1]), "Email": stwd}
        else:
            stwd_info = {"First Name": inflection.titleize(nameList[0]), "Last Name": "", "Email": stwd}
        return stwd_info

    def steward_info(self, stwd):
        '''grabs the data steward info from the df and turns into json'''
        steward_info = self._df_stewards[ self._df_stewards['data_steward'] == stwd.strip()]
        if len(steward_info) > 0:
            steward_info = steward_info.to_dict(orient='records')[0]
        else:
            steward_info = self.generate_blank_stwd_info(stwd)
        return steward_info

    def set_Datasets(self, stwd):
        '''gets the datasets associatiated with a steward'''
        df_datasets = pd.DataFrame({'count' : self._df_master[ ( self._df_master["data_steward"] == stwd) & (self._df_master['status'] != "Submitted by Steward") & (self._df_master['status'] != "Complete") & (self._df_master['status'] != "Do Not Process") & (self._df_master['status'] != "Submitted by Coordinator") & (self._df_master['status'] != "Entered on Portal") ].groupby(["inventoryid", "datasetid", "dataset_name", "open_data_portal_url"]).size()}).reset_index()
        #get the dates of datsets with submitted fields
        df_datasetsSubmitted =  pd.DataFrame({'count' : self._df_master[ ((self._df_master["data_steward"] == stwd) & ( (self._df_master['status'] == "Submitted by Steward") | (self._df_master['status'] == "Complete") | (self._df_master['status'] == "Do Not Process") |  (self._df_master['status'] == "Entered on Portal") | (self._df_master['status'] == "Submitted by Coordinator"))) ].groupby(["date_last_changed"]).size()}).reset_index()
        df_datasetsList = list(df_datasets['datasetid'])
        datasetsSubmittedCnt = sum(list(df_datasetsSubmitted['count']))
        df_datasetsDict = df_datasets.T.to_dict().values()
        datasetToDoCount = sum(list(df_datasets['count']))
        return  df_datasets, df_datasetsList, df_datasetsDict, datasetsSubmittedCnt, datasetToDoCount

    @staticmethod
    def makeSummarySheet(df_datasets):
        df_datasets.rename(columns={'count': 'Number of Fields'}, inplace=True)
        return {"datasetid": "Dataset Summary", "sheet_df": df_datasets}

    def sheet(self, datasetID):
        '''creates a sheet dictionary item'''
        return {"datasetid": datasetID, "sheet_df" : self._df_master[self.sheet_keys][ (self._df_master['datasetid'] == datasetID) & (self._df_master['status'] != "Submitted by Steward") ]}

    def set_Sheets(self, df_datasetsList,df_datasets):
        '''creates a list of dictionaries that contain sheet information to be written to a wkbk'''
        sheets = []
        sheets.append(self.makeSummarySheet(df_datasets))
        for datasetID in df_datasetsList:
            sheet = self.sheet(datasetID)
            sheets.append(sheet)
        return sheets

    @staticmethod
    def make_json_item(steward_info, wkbk_fullpath, df_datasetsDict, current_date, submittedFields):
        '''writes a json object to save info about the workbook and steward to be used in the emailer'''
        json_item = {"data_cordinator":steward_info,
                          "path_to_wkbk": wkbk_fullpath,
                          "datasets": df_datasetsDict,
                          "timestamp":current_date,
                          "submittedFields": submittedFields
                    }
        return json_item

    @staticmethod
    def checkIfSubmittedFields(datasetsSubmittedCnt, datasetToDoCount ):
        '''sets flag to see if the stwd has submitted fields in the past and get the count '''
        submitted = False
        percent_done = 0
        totalFields = datasetsSubmittedCnt + datasetToDoCount
        if datasetsSubmittedCnt > 0:
            submitted = True
            percent_done = round(100 * float(datasetsSubmittedCnt)/float(totalFields), 2)
        if(percent_done != 100):
            return {"submitted": submitted, "submitted_fields_cnt": datasetsSubmittedCnt, "fields_to_do_cnt": datasetToDoCount, "total_fields": totalFields, "percent_done": percent_done }
        return False

    def build_Wkbks(self):
        '''builds and writes wkbks for datastewards'''
        #sprint self._stewardsList[0:3]
        for stwd in self._stewardsList:
            stwd_info  = self.steward_info(stwd)
            df_datasets, df_datasetsList, df_datasetsDict, datasetsSubmittedCnt, datasetToDoCount = self.set_Datasets(stwd)
            submittedFields = self.checkIfSubmittedFields(datasetsSubmittedCnt, datasetToDoCount)
            if(submittedFields):
                sheets = self.set_Sheets(df_datasetsList, df_datasets)
                wkbk_fullpath, current_date =  self._wkbk_writer.write_wkbk(sheets, stwd_info)
                self._json_object["workbooks"].append( self.make_json_item(stwd_info, wkbk_fullpath, df_datasetsDict, current_date, submittedFields))
        wrote_json = WkbkJson.write_json_object(self._json_object,  self._pickle_dir,  self._wkbk_output_json)
        update_rows = self.update_metadata_status()
        return wrote_json, update_rows

    def update_metadata_status(self):
        '''creates rows to update the master data dictionary with status codes'''
        wkbks =  self._json_object['workbooks']
        dataset_status_updts = []
        for wkbk in wkbks:
            dataset_info =  wkbk['datasets']
            dataset_ids = [d['datasetid'] for d in dataset_info ]
            dataset_status_updts =  dataset_status_updts + dataset_ids
        updt_df = self._df_master[(self._df_master['datasetid'].isin(dataset_status_updts)) & (~self._df_master['status'].isin(self._valsToNotOverride))].reset_index()
        updt_df['date_last_changed'] = self._current_date
        updt_df['status'] = 'For Review by Steward'
        return PandasUtils.convertDfToDictrows(updt_df[['columnid', 'status', 'date_last_changed']])

if __name__ == "__main__":
    main()

