

from __future__ import division
import pandas as pd
import pickle
import json
import inflection
from Utils import *
import re
from MetaDatasets import *
from ConfigUtils import *
from PandasUtils import *

class UploadAssetFieldDefs:
    """class to update master dd with existing definitions in asset fields dataset on the portal """
    def __init__(self,  configItems):
        self._config_dir = configItems['config_dir']
        self._pickle_data_dir = configItems['pickle_data_dir']
        self._pickle_dir = configItems['pickle_dir']
        self._metadata_config_fn = configItems['metadataset_config']

        self._metadataset_config = ConfigUtils.setConfigs(self._config_dir,  self._metadata_config_fn )
        self._master_dd_config = self._metadataset_config['master_data_dictionary']
        self._globalfields_config = self._metadataset_config['global_fields']
        self._assetfields_config = self._metadataset_config['asset_fields']

        self._df_master = MetaDatasets.set_master_df_dd(self._pickle_data_dir, self._master_dd_config['json_fn'])
        #self._globalfields_list = MetaDatasets.set_global_fields_list(self._pickle_data_dir, self._globalfields_config['json_fn'])
        self._asset_fields_df = MetaDatasets.get_metadata_df(self._pickle_data_dir, self._assetfields_config['json_fn'])

        self._valsToNotOverride =  ['Complete', "Submitted by Coordinator" , "Submitted by Steward"]
        self._keys_to_keep = ['columnid', 'field_definition', 'status', 'date_last_changed']
        self._current_date = DateUtils.get_current_date_month_day_year()
        self._output_file = configItems['output_json_fn']
        self.json_key = configItems['json_key']
        self.cols_to_rename =  {'field_description': 'field_definition'}


    def filter_dfs(self):
        '''filter the defs'''
        try:
            self._df_master = self._df_master[(~self._df_master['status'].isin(self._valsToNotOverride))].reset_index()
            self._df_master = self._df_master[['columnid']]
            self._asset_fields_df = PandasUtils.fillNaWithBlank(self._asset_fields_df)
            self._asset_fields_df = self._asset_fields_df[self._asset_fields_df['field_description'] != ""]
            self._asset_fields_df =  PandasUtils.mapFieldNames(self._asset_fields_df, self.cols_to_rename)
            return True
        except Exception, e:
            print str(e)
        return False

    def set_status(self, df):
        '''sets the status'''
        df['status'] = 'Entered on Portal'
        df['date_last_changed'] = self._current_date
        return df

    def getFieldDefList(self):
        filtered = self.filter_dfs()
        wroteJsonFile = False
        if filtered:
            df_asset_field_defs = pd.merge(self._df_master, self._asset_fields_df, on='columnid')
            df_asset_field_defs = self.set_status(df_asset_field_defs)
            df_asset_field_defs = df_asset_field_defs[self._keys_to_keep]
            asset_field_defs = PandasUtils.convertDfToDictrows(df_asset_field_defs)
            try:
                wroteJsonFile = WkbkJson.write_json_object({self.json_key: asset_field_defs}, self._pickle_dir, self._output_file)
            except Exception, e:
                print "could not wrtite file"
                print str(e)
        return wroteJsonFile
