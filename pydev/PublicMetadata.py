

from __future__ import division
import pandas as pd
import json
from Utils import *
from MetaDatasets import *
from ConfigUtils import *
from PandasUtils import *

class PublicMetadata:
    """class to update master dd with existing definitions in asset fields dataset on the portal """
    def __init__(self,  configItems):
        self._config_dir = configItems['config_dir']
        self._pickle_data_dir = configItems['pickle_data_dir']
        self._pickle_dir = configItems['pickle_dir']
        self._metadata_config_fn = configItems['metadataset_config']

        self._metadataset_config = ConfigUtils.setConfigs(self._config_dir,  self._metadata_config_fn )
        self._master_dd_config = self._metadataset_config['master_data_dictionary']
        self._df_master = MetaDatasets.set_master_df_dd(self._pickle_data_dir, self._master_dd_config['json_fn'])
        #self._keys_to_keep_globals = ['columnid', 'datasetid', 'inventoryid', 'open_data_portal_url', 'department', 'dataset_name', 'field_name' , 'field_alias', 'field_type', 'api_key', 'field_definition', 'global_field_definition', 'field_type_flag', 'data_dictionary_attached', 'field_documented', 'attachment_url']
        self._keys_to_keep = ['columnid', 'datasetid', 'inventoryid', 'open_data_portal_url', 'department', 'dataset_name', 'field_name' , 'field_alias', 'field_type', 'api_key', 'field_definition', 'field_type_flag', 'data_dictionary_attached', 'field_documented', 'attachment_url']
        self._output_file = configItems['output_json_fn']
        self.json_key = configItems['json_key']


    def master_df_public_list(self):
        '''grab the dfs with gloads'''
        def set_field_def_globals(row):
            if(row['global_field_definition'] != ''):
               return row['global_field_definition']
            return row['field_definition']
        self._df_master['field_definition'] = self._df_master.apply(lambda row:set_field_def_globals(row ),axis=1)
        self._df_master = self._df_master[ self._keys_to_keep ]
        return PandasUtils.convertDfToDictrows(self._df_master)

    def get_public_fields(self):
        mm_dd_list = self.master_df_public_list()
        if len(mm_dd_list) > 0:
            try:
                wroteJsonFile = WkbkJson.write_json_object({self.json_key: mm_dd_list}, self._pickle_dir, self._output_file)
            except Exception, e:
                print "could not wrtite file"
                print str(e)
        return wroteJsonFile
