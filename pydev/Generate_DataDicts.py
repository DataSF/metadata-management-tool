# coding: utf-8


from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle
import datetime
from openpyxl import Workbook
import json
import inflection

class Generate_DataDicts:
    """class to generate data dictionaries"""
    def __init__(self,  configItems, cells_dataDict=None, cells_stewards=None, fieldtype_cells=None):
        self.wkbk_output_dir = configItems['wkbk_output_dir']
        self.current_date = datetime.datetime.now().strftime("%Y_%m_%d")
        self.sheet_keys =  ["columnID", "Dataset Name", "Open Data Portal URL", "Field Name", "Field Type", "Field Definition", "Field Alias", "Field Type Flag"]
        self.steward_keys = ["Email", "Department", "First Name", "Last Name", "Dept Code"]
        self.df_master = None
        self.df_stewards = None
        self.df_stewardsList = None
        self.df_stewardsInfo = None
        self.field_types = None
        self.df_datasets = None
        self.df_datasetsList = None
        self.df_datasetsDict = None
        self.sheets = []
        self.format_font =  None
        self.workbook = None
        self.format_num = None
        self.writer = None
        self.steward_info = None
        self.wkbk_name = None
        self.wkbk_fullpath = None
        self.writer = None
        self.workbook = None 
        self.json_item = {"data_cordinator": None , "path_to_wkbk":None , "datasets":None ,"timestamp":None}
        self.json_object = { "workbooks": []}
        self.wkbk_output_json = configItems['wkbk_output_json']
        self.set_dfs( cells_dataDict,cells_stewards, fieldtype_cells )
    
    def set_dfs(self, cells_dataDict=None, cells_stewards=None, fieldtype_cells=None):
        if cells_dataDict:
            self.df_master = self.set_master_df(cells_dataDict)
            self.df_stewards = self.set_dataStewards()
            self.df_stewardsList =  self.set_dataStewardsList()
        if cells_stewards:
            self.df_stewardsInfo = self.set_dataStewardsInfo_df(cells_stewards)
        if fieldtype_cells:
            self.field_types = self.set_fieldtypes(fieldtype_cells) # + [None]
                     
    def set_master_df(self, cells):
        '''creates a dataframe of the master field list'''
        '''returns all rows where Do Not Process == False '''
        df = pd.DataFrame(cells)
        return df[(df["Do Not Process"] == "FALSE" ) & (df['datasetID'] != '#N/A') ]

    def get_master_df(self):
        return self.df_master
    
    def set_dataStewardsInfo_df(self, cells):
        '''creates a dataframe of datasteward info'''
        df = pd.DataFrame(cells)
        return df[self.steward_keys]

    def set_dataStewards(self):
        return pd.DataFrame({'count' : self.df_master.groupby("Data Steward").size()}).reset_index()
        
    def get_dataStewards(self):
        return self.df_stewards
    
    def get_dataStewardsInfo(self):
        return self.df_stewardsInfo
    
    def set_dataStewardsList(self):
        return list(self.df_stewards['Data Steward'])
        
    def get_dataStewardsList(self):
        return self.df_stewardsList
    
    def set_steward_info(self, stwd):
        self.steward_info = self.df_stewardsInfo[self.df_stewardsInfo["Email"] == stwd.strip()]
        if len(self.steward_info) > 0:
            self.steward_info = self.steward_info.to_dict(orient='records')[0]
        else:
            self.steward_info = self.generate_blank_stwd_info(stwd)
           
    @staticmethod
    def generate_blank_stwd_info(stwd):
        stwd_info = {"First Name": None, "Last Name": None, "Email": None}
        nameList = stwd.split("@")
        nameList = nameList[0].split(".")
        if len(nameList)> 1:
            return {"First Name": inflection.titleize(nameList[0]), "Last Name": inflection.titleize(nameList[1]), "Email": stwd}
        else:
            return {"First Name": inflection.titleize(nameList[0]), "Last Name": "", "Email": stwd}
        
    def get_steward_info(self):
        return self.steward_info
    
    def set_Datasets(self, stwd):
        self.df_datasets = pd.DataFrame({'count' : self.df_master[ self.df_master["Data Steward"] == stwd].groupby(["inventoryID", "datasetID", "Dataset Name", "Open Data Portal URL"]).size()}).reset_index()
        self.df_datasetsList = list(self.df_datasets['datasetID'])
        self.df_datasetsDict = self.df_datasets.T.to_dict().values()
    
    def get_Datasets(self):
        return self.df_datasets
    
    def get_DatasetsDict(self):
        return self.df_datasetsDict
    
    def get_DatasetsList(self):
        return self.df_datasetsList
    
    @staticmethod
    def set_fieldtypes(fieldtype_cells):
        df = pd.DataFrame(fieldtype_cells)
        return list(df['field_type'])
    
    def get_fieldtypes(self):
        return self.field_types
    
    def set_Sheet(self, datasetID):
        return {"datasetID": datasetID, "sheet_df" : self.df_master[self.sheet_keys][ self.df_master['datasetID'] == datasetID]}

    def set_Sheets(self):
        for datasetID in self.df_datasetsList:
            sheet = self.set_Sheet(datasetID)
            self.sheets.append(sheet)
    
    def get_Sheets(self):
        return self.sheets
    
    def set_wkbk_name(self):
        self.wkbk_name = "data_dictionary_" + self.steward_info["First Name"] + "_" + self.steward_info["Last Name"] + "_" + self.current_date+ ".xlsx"
        self.wkbk_fullpath = self.wkbk_output_dir + self.wkbk_name
    
    def get_wkbk_name(self):
        return self.wkbk_name
    
    def format_worksheet_cols(self, worksheet):
        worksheet.set_column('A:A', None,self.format_num)
        worksheet.set_column('F:F', 50, self.format_font)
        worksheet.set_column('D:D', 35, self.format_font)
        worksheet.set_column('B:B', 30, self.format_font)
        worksheet.set_column('C:C', 25, self.format_font)
        worksheet.set_column('B:B', 30, self.format_font)
        worksheet.set_column('E:E', 25, self.format_font)
        worksheet.set_column('G:G', 30, self.format_font)
        worksheet.set_column('H:H', 30, self.format_font)
        return worksheet
    
    def format_wkbk(self, sheet, sheet_len):
        worksheet = self.writer.sheets[sheet['datasetID']]
        worksheet.set_zoom(90)
        format = self.workbook.add_format()
        worksheet = self.format_worksheet_cols(worksheet)
        worksheet.data_validation('E2:E'+str(sheet_len), {'validate': 'list',
                                  'source': self.field_types })
        worksheet.data_validation('H2:H'+str(sheet_len), {'validate': 'list',
                                 'source': ["primary_id", "category"] })
        
    def set_workbook_format(self):
        self.workbook = self.writer.book
        self.format_num = self.workbook.add_format({'num_format': '0000000000'})
        self.format_font = self.workbook.add_format({'font_name': 'Arial','font_size': '11' })
        
    def write_wkbk(self):
        self.set_wkbk_name()
        self.writer = pd.ExcelWriter(self.wkbk_fullpath,  engine='xlsxwriter' )
        for sheet in self.sheets:
            sheet["sheet_df"].to_excel(self.writer, index=False,  sheet_name=sheet['datasetID'])
        self.set_workbook_format()
        for sheet in self.sheets:
            sheet_len = len(sheet["sheet_df"])
            self.format_wkbk(sheet, sheet_len)
        self.writer.save()
        
        
    def set_json_item(self):
        '''writes a json object to save info about the workbook and steward to be used in the emailer'''
        self.json_item = {"data_cordinator":self.steward_info , 
                          "path_to_wkbk": self.wkbk_fullpath, 
                          "datasets": self.get_DatasetsDict(),
                          "timestamp": self.current_date }
    
    def get_json_item(self):
        return self.json_item
    
    def set_json_object(self):
        self.set_json_item()
        self.json_object["workbooks"].append(self.get_json_item())
        
    def get_json_object(self):
        return self.json_object
    
    def write_json_object(self):
        with open(self.wkbk_output_dir + self.wkbk_output_json, 'w') as f:
            json.dump(self.get_json_object(), f, ensure_ascii=False)
    
    def build_Wkbk(self):
        for stwd in self.df_stewardsList:
            self.set_steward_info(stwd)
            self.set_Datasets(stwd)
            self.set_Sheets()
            self.write_wkbk()
            self.set_json_object()
        self.write_json_object()
    

class AfterCreateDataDicts:
    def __init__(self, configItems):
         self._wkbk_output_dir = configItems["wkbk_output_dir"]
         self._wkbk_output_json = configItems['wkbk_output_json']
         self._json_object = self.set_json_object()
         self._wkbks =self.set_wkbks()
    
    def set_json_object(self):
        json_data=open(self._wkbk_output_dir + self._wkbk_output_json).read()
        return json.loads(json_data)
    
    @property
    def get_json_object(self):
        return self._json_object

    def set_wkbks(self):
        wkbks = self._json_object["workbooks"]
        return wkbks
    @property
    def get_wkbks(self):
        return self._wkbks
        

class UpdateDataDictsAfterCreation:
    '''Class to Update Spreadsheet after datadicts have been created '''
    
    def __init__(self, gSpread_Stuff, afterCreateDataDicts, configItems):
        self._gSpread_Stuff= gSpread_Stuff
        self._afterCreateDataDicts =  afterCreateDataDicts
        self._updt_sht_name = configItems['update_info']['updt_sht_name']
        self._updt_wkbk_key = configItems['update_info']['wkbk_key']
        self._updt_wkbk = self._gSpread_Stuff.get_wkbk(self._updt_wkbk_key)
        self._updt_sht =   self._gSpread_Stuff.get_sht( self._updt_wkbk, self._updt_sht_name)
        self._field_positions = configItems['update_info']['field_positions']
        self._current_date = datetime.datetime.now().strftime("%m/%d/%Y")
        self._updt_statuses = configItems['update_info']['statuses']
       
    @staticmethod
    def getDatasetsList(wkbk):
        return [elem['Dataset Name'].strip() for elem in wkbk['datasets'] if 'Dataset Name' in elem]
    
    @staticmethod
    def checkUpdateStatus(cell_updt_status_dict):
        cells_updated = False
        if not (False in cell_updt_status_dict.values()):
            cells_updated = True
        return cells_updated
        
        
    def updatewkbk_info(self):
        wkbk_cells_updted = {}
        for wkbk in self._afterCreateDataDicts._wkbks:
            cells_updated = False
            datasetsList =  self.getDatasetsList(wkbk)
            all_rows = self._gSpread_Stuff.getCellRows(self._updt_sht, datasetsList)
            cell_ranges_dt_changed = self._gSpread_Stuff.getCellRanges(all_rows, self._field_positions['date_last_changed'])
            updt_dt_changed  = self._gSpread_Stuff.batchUpdateCellRanges( self._updt_sht , cell_ranges_dt_changed,  self._current_date )
            cell_ranges_status =  self._gSpread_Stuff.getCellRanges(all_rows, self._field_positions['status'])     
            updt_statuses  = self._gSpread_Stuff.batchUpdateCellRanges( self._updt_sht , cell_ranges_status,  self._updt_statuses['for_review_steward'] )
            if self.checkUpdateStatus(updt_statuses) and self.checkUpdateStatus(updt_dt_changed):
                cells_updated = True
            wkbk_cells_updted[wkbk["data_cordinator"]["Email"]] = cells_updated 
        return self.checkUpdateStatus(wkbk_cells_updted)
            
if __name__ == "__main__":
    main()