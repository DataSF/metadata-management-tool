
from ConfigUtils import *
from Wkbk_Json import *
from PandasUtils import *

class MetaDatasets:
  '''utility functions to handle metadatasets'''


  def __init__(self,  configItems, socrataQueriesObject, logger):
    self._config_inputdir = configItems['config_dir']
    self._metadata_config_fn = configItems['metadataset_config']
    self._metadataset_config = ConfigUtils.setConfigs(self._config_inputdir,  self._metadata_config_fn )
    self._master_dd_config = self._metadataset_config['master_data_dictionary']
    self._fieldtypes_config = self._metadataset_config['field_types']
    self._globalfields_config = self._metadataset_config['global_fields']
    self._pickle_dir = configItems['pickle_dir']
    self._pickle_data_dir = configItems['pickle_data_dir']
    self._socrataQueriesObject = socrataQueriesObject
    self._logger = logger
    self._wkbk_json = WkbkJson(configItems, logger)

  @staticmethod
  def set_master_df(pickle_data_dir, json_file):
    '''creates a dataframe of the master field list from json'''
    '''returns all rows where Do Not Process == False '''
    json_obj = WkbkJson.loadJsonFile(pickle_data_dir, json_file)
    df = PandasUtils.makeDfFromJson(json_obj)
    df_master = df[ (df["do_not_process"] == False)  & (df['datasetid'] != '#N/A') & (df['global_field'] == False) ]
    print len(df_master)
    return df_master

  @staticmethod
  def set_master_df_dd(pickle_data_dir, json_file):
    '''creates a dataframe of the master field list from json'''
    '''returns all rows, except  global fields'''
    json_obj = WkbkJson.loadJsonFile(pickle_data_dir, json_file)
    df = PandasUtils.makeDfFromJson(json_obj)
    df_master = df[  (df['datasetid'] != '#N/A')] #& (df['global_field'] == False) ]
    print len(df_master)
    return df_master

  @staticmethod
  def set_global_fields_list(pickle_data_dir, json_file):
    '''creates a list of global field strings'''
    json_obj = WkbkJson.loadJsonFile(pickle_data_dir, json_file)
    df = PandasUtils.makeDfFromJson(json_obj)
    df = PandasUtils.colToLower(df, 'global_string')
    df = PandasUtils.colToLower(df, 'field_name')
    return list(set(list(df['global_string']) + list(df['field_name'])))

  def get_master_metadataset_as_json(self):
    results_json = self._socrataQueriesObject.pageThroughResultsSelect( self._master_dd_config['fourXFour'], '*')
    return self._wkbk_json.write_json_object(results_json, self._pickle_data_dir, self._master_dd_config['json_fn'])

  def get_field_types(self):
    results_json  = self._socrataQueriesObject.pageThroughResultsSelect( self._fieldtypes_config['fourXFour'], self._fieldtypes_config['columns_to_fetch'])
    return self._wkbk_json.write_json_object(results_json, self._pickle_data_dir, self._fieldtypes_config['json_fn'])

  def get_global_fields_as_json(self):
    results_json  = self._socrataQueriesObject.pageThroughResultsSelect( self._globalfields_config['fourXFour'], self._globalfields_config['columns_to_fetch'])
    return self._wkbk_json.write_json_object(results_json, self._pickle_data_dir, self._globalfields_config['json_fn'])

  def get_base_datasets(self):
    downloaded_datasets = False
    downloaded_master_dd = self.get_master_metadataset_as_json()
    downloaded_fieldtypes = self.get_field_types()
    if downloaded_master_dd and downloaded_fieldtypes:
      downloaded_datasets = True
    return downloaded_datasets

  def set_master_dd_updt_info(self, updt_rows):
    dataset = self._master_dd_config
    dataset = self._socrataQueriesObject.setDatasetDicts(dataset)
    dataset[self._socrataQueriesObject.src_records_cnt_field] = len(updt_rows)
    return dataset




if __name__ == "__main__":
    main()
