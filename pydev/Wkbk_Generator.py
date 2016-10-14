# coding: utf-8


from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle
from openpyxl import Workbook
import json
import inflection

class WkbkGenerator:
    """class to generate data dictionaries"""
    def __init__(self,  configItems, cells_dataDict=None, cells_stewards=None):
        self.sheet_keys = configItems['sheet_keys']
        self.steward_keys = configItems['steward_keys']
        self._df_master = self.set_master_df(cells_dataDict)
        self._df_stewards = self.set_df_stewards()
        self._stewardsList = self.set_stewardsList()
        self._df_stewardsInfo = self.set_df_stewardsInfo(cells_stewards)
        self.json_object = { "workbooks": []}
       
    @property
    def df_master(self):
        return self._df_master
    
    def set_master_df(self, cells):
        '''creates a dataframe of the master field list'''
        '''returns all rows where Do Not Process == False '''
        if cells:
            df = pd.DataFrame(cells)
            return df[  (df["Do Not Process"] == "FALSE" ) & (df['datasetID'] != '#N/A') ]
        
    @property
    def df_stewards(self):
        return self._df_stewards      
        
    def set_df_stewards(self):
        '''sets a dataframe of datastewards and counts based on main dataframe'''
        return pd.DataFrame({'count' : self._df_master.groupby("Data Steward").size()}).reset_index()
        
    @property
    def stewardsList(self):
        return self._stewardsList
         
    def set_stewardsList(self):
        '''creates a list of data steward email adddresses'''
        return list(self._df_stewards['Data Steward'])
        
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
        steward_info = self.df_stewardsInfo[self.df_stewardsInfo["Email"] == stwd.strip()]
        if len(steward_info) > 0:
            steward_info = steward_info.to_dict(orient='records')[0]
        else:
            steward_info = self.generate_blank_stwd_info(stwd)
        return steward_info  

    def set_Datasets(self, stwd):
        '''gets the datasets associatiated with a steward'''
        df_datasets = pd.DataFrame({'count' : self.df_master[ (self.df_master["Data Steward"] == stwd) & (self.df_master['Status'] != "Submitted by Steward")].groupby(["inventoryID", "datasetID", "Dataset Name", "Open Data Portal URL"]).size()}).reset_index()
        #get the dates of datsets with submitted fields
        df_datasetsSubmitted =  pd.DataFrame({'count' : self.df_master[ (self.df_master["Data Steward"] == stwd) & (self.df_master['Status'] == "Submitted by Steward")].groupby(["Date Last Changed"]).size()}).reset_index()
        df_datasetsList = list(df_datasets['datasetID'])
        datasetsSubmittedCnt = sum(list(df_datasetsSubmitted['count']))
        df_datasetsDict = df_datasets.T.to_dict().values()
        datasetToDoCount = sum(list(df_datasets['count']))
        return  df_datasets, df_datasetsList, df_datasetsDict, datasetsSubmittedCnt, datasetToDoCount
    
    @staticmethod
    def makeSummarySheet(df_datasets):
        df_datasets.rename(columns={'count': 'Number of Fields'}, inplace=True)
        return {"datasetID": "Dataset Summary", "sheet_df": df_datasets}
        
    def sheet(self, datasetID):
        '''creates a sheet dictionary item'''
        return {"datasetID": datasetID, "sheet_df" : self.df_master[self.sheet_keys][ (self.df_master['datasetID'] == datasetID) & (self.df_master['Status'] != "Submitted by Steward") ]}
    
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
            #print datasetsSubmittedCnt
            submitted = True
            percent_done = round(100 * float(datasetsSubmittedCnt)/float(totalFields), 2)
            #print percent_done
        return {"submitted": submitted, "submitted_fields_cnt": datasetsSubmittedCnt, "fields_to_do_cnt": datasetToDoCount, "total_fields": totalFields, "percent_done": percent_done }
            
    def build_Wkbks(self, wkbk_writer):
        '''builds and writes wkbks for datastewards'''
        for stwd in  self._stewardsList:
            stwd_info  = self.steward_info(stwd)
            df_datasets, df_datasetsList, df_datasetsDict, datasetsSubmittedCnt, datasetToDoCount = self.set_Datasets(stwd)
            submittedFields = self.checkIfSubmittedFields(datasetsSubmittedCnt, datasetToDoCount)
            sheets = self.set_Sheets(df_datasetsList, df_datasets)
            wkbk_fullpath, current_date = wkbk_writer.write_wkbk(sheets, stwd_info)
            self.json_object["workbooks"].append( self.make_json_item(stwd_info, wkbk_fullpath, df_datasetsDict, current_date, submittedFields))
        return self.json_object
   
    

if __name__ == "__main__":
    main()

