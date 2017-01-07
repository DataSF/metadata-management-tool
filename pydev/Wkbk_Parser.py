

# coding: utf-8
import datetime
import pandas as pd
from PandasUtils import *
import xlrd
from Utils import *
from ConfigUtils import *
from Wkbk_Json import *
from DictUtils import *
from Wkbk_Generator import *
from MetaDatasets import *


class WkbkParser:
    '''class to parse wkbk responses'''
    def __init__(self, configItems):
        self._config_dir =  configItems['config_dir']
        #self.keys_to_keep = ['columnID', 'Field Type', 'Field Definition', 'Field Alias', 'Field Type Flag']
        self.keys_to_keep = ['columnID', 'Field Definition', 'Field Alias', 'Field Type Flag']
        self.keys_to_keep_field = ['columnid', 'field_definition', 'field_alias', 'field_type_flag', 'status', 'date_last_changed']
        self.keys_to_keep_field_unmatched = ['columnid', 'dataset_name', 'field_name', 'field_type', 'field_definition']

        self.updt_fields_json_name = configItems['updt_fields_json_fn']
        self.json_key = "updt_fields"
        self.wkbk_uploads_dir = configItems['wkbk_uploads_dir']
        self.field_positions = configItems['update_info']['field_positions']
        self.field_positions_numeric = configItems['update_info']['field_positions_numeric']
        self.field_name_mappings = configItems['update_info']['field_name_mappings']
        self.statuses = configItems['update_info']['statuses']
        self.statuses_submited = self.statuses['submitted_steward']
        self._current_date = DateUtils.get_current_date_month_day_year()
        self._screendoor_config_file = configItems['screendoor_config_file']
        self._screendoor_configs = ConfigUtils.setConfigs(self._config_dir, self._screendoor_config_file)
        self._wkbk_uploads_dir = self._screendoor_configs['wkbk_uploads_dir']
        self._wkbk_uploads_json_fn = self._screendoor_configs['wkbk_uploads_json_fn']
        self._pickle_dir = configItems['pickle_dir']
        self._pickle_data_dir = configItems['pickle_data_dir']
        self._metadata_config_fn = configItems['metadataset_config']
        self._metadataset_config = ConfigUtils.setConfigs(self._config_dir,  self._metadata_config_fn )
        self._master_dd_config = self._metadataset_config['master_data_dictionary']
        self._master_dd_fn = self._master_dd_config['json_fn']
        self._df_master = MetaDatasets.set_master_df(self._pickle_data_dir, self._master_dd_config['json_fn'])
        self._valsToNotOverride= configItems['valsToNotOverride']
        self._do_not_process_columnids = self.get_do_not_process_list()
        self._current_date_year = DateUtils.get_current_date_year_month_day()


    def get_do_not_process_list(self):
        dnp_df = self._df_master[(self._df_master['status'].isin(self._valsToNotOverride))].reset_index()
        return list(dnp_df['columnid'])

    @staticmethod
    def get_sht_names(wkbk):
        shts =  wkbk.sheet_names
        return [ sht for sht in shts if sht != 'Dataset Summary']

    def add_status_info(self, df_wkbk):
        df_wkbk[self.field_name_mappings['status']] = self.statuses_submited
        df_wkbk[self.field_name_mappings['date_last_changed']] = self._current_date
        return df_wkbk

    def parse_sht(self, wkbk, sht_name):
        '''we only want to upload rows that should be updated.
        We want to filter the following records:
            -do not process
            -global fields
            --> these are filtered out when we grab the master data dictionary
            -Also want tot filter out rows with these statuses:
            -complete
            -do not process
            -submitted by cordinator
            -submitted by steward
            --> these vals are in the config variable, valsToNotOverride;
        When the class is intialized, we create the variable,  self._do_not_process_columnids
            -this variable is a list of columnids with rows that have a status in the do not process list
            - then, in the df_wkbk dataframe, we take just the rows where columnid NOT in self._do_not_process_columnids list
            '''

        df_wkbk =  wkbk.parse(sht_name)
        df_wkbk = self.add_status_info(df_wkbk)
        #rename the cols to the spreadsheet cols for ease
        field_name_mapping =  dict((v,k) for k,v in self.field_name_mappings.iteritems())
        #use string col locations onto the dict
        df_wkbk= df_wkbk.rename(columns = field_name_mapping)
        #print "**prior to filtering: " + str(len(df_wkbk))
        #grab only records with the correct statuses
        df_wkbk = df_wkbk[(~df_wkbk['columnid'].isin(self._do_not_process_columnids))].reset_index()
        #print "**filtering on status*: " + str(len(df_wkbk))
        #now make sure that the columnid is in the master datadict
        df_wkbk_in_mmdd = df_wkbk[(df_wkbk['columnid'].isin(list(self._df_master['columnid'])))].reset_index()
        #print "**filtering on mmdd*: " + str(len(df_wkbk_in_mmdd))
        #get a df of the columns not in the master dd
        df_wkbk_not_in_mmdd = df_wkbk[~(df_wkbk['columnid'].isin(list(self._df_master['columnid'])))].reset_index()
        #print "**cols not in mmdd*: " + str(len(df_wkbk_not_in_mmdd))
        #print df_wkbk_not_in_mmdd[['columnid','field_name']]
        #only keep relevant fields
        df_wkbk_in_mmdd = df_wkbk_in_mmdd[self.keys_to_keep_field]
        df_wkbk_not_in_mmdd = df_wkbk_not_in_mmdd[self.keys_to_keep_field_unmatched]

        df_dictList = df_wkbk_in_mmdd.to_dict(orient='records')
        df_dictList_not_in_mmdd = df_wkbk_not_in_mmdd.to_dict(orient='records')
        #filter out the nans
        df_dictList = [ DictUtils.filterDictOnNans(field_dict) for field_dict in df_dictList ]
        df_dictList_not_in_mmdd = [ DictUtils.filterDictOnNans(field_dict) for field_dict in df_dictList_not_in_mmdd ]
        #double check that the field actually changed- needs to a be at least a len of 4 for a field to actually be updted after removing all nan vals
        df_dictList =  [k for k in df_dictList if len(k.keys()) > 3]
        df_dictList_not_in_mmdd =  [k for k in df_dictList_not_in_mmdd if len(k.keys()) > 4]

        return [df_dictList, df_dictList_not_in_mmdd]

    def get_shts(self, fn):
        wkbk = PandasUtils.getWkbk(fn)
        sht_names = self.get_sht_names(wkbk)
        metadata_dicts = [ self.parse_sht(wkbk, sht_name) for sht_name in sht_names]
        #list of the fields we'll upload
        metadata_dicts_in_mmdd = [ metadata_dict[0] for metadata_dict in metadata_dicts if len(metadata_dict[0]) > 0]
        #list of fields that weren't in the mmdd
        unmatched_fields = [ metadata_dict[1] for metadata_dict in metadata_dicts if len(metadata_dict[1]) > 0]
        #wroteUnmatchedFields = FileUtils.write_wkbk_csv(self._pickle_dir+self._current_date_year+"_unmatched_fields" ,unmatched_fields, self._documented_fields_matched)
        metadata_dicts_in_mmdd =  ListUtils.flatten_list(metadata_dicts_in_mmdd)
        unmatched_fields = ListUtils.flatten_list(unmatched_fields)
        return [metadata_dicts_in_mmdd, unmatched_fields]



    def get_metadata_updt_fields_from_shts(self):
        fileList =  FileUtils.getFileListForDir(self._wkbk_uploads_dir + "*.xlsx")
        wroteJsonFile = False
        unmatchedFn = None
        metadata_dicts = [ self.get_shts(fn) for fn in fileList ]
        metadata_dicts_in_mmdd = [ metadata_dict[0] for metadata_dict in metadata_dicts if len(metadata_dict[0]) > 0]
        metadata_dicts_not_in_mmdd = [ metadata_dict[1] for metadata_dict in metadata_dicts if len(metadata_dict[1]) > 0]
        metadata_dictJson = { self.json_key: ListUtils.flatten_list(metadata_dicts_in_mmdd)}
        try:
            wroteJsonFile = WkbkJson.write_json_object( metadata_dictJson, self._pickle_dir, self.updt_fields_json_name)
        except Exception, e:
            print "could not wrtite file"
            print str(e)
        unmatched_cols = ['columnid',  'dataset_name', 'field_name', 'field_type', 'field_definition',]
        metadata_dict_not_in_mmdd_csv =  ListUtils.flatten_list(metadata_dicts_not_in_mmdd)
        #print metadata_dict_not_in_mmdd_csv
        fn = self._current_date_year+"unmatched_fields.csv"
        wroteUnmatchedFields = FileUtils.write_wkbk_csv(self._pickle_dir+fn,metadata_dict_not_in_mmdd_csv, unmatched_cols)
        if wroteUnmatchedFields:
            unmatchedFn = fn
        return wroteJsonFile, unmatchedFn

    def load_updt_fields_json(self):
        updt_fieldList = []
        try:
            jsonList = WkbkJson.loadJsonFile(self._pickle_dir, self.updt_fields_json_name)
            updt_fieldList = jsonList[self.json_key]
        except Exception, e:
            print str(e)
        return updt_fieldList


if __name__ == "__main__":
    main()
