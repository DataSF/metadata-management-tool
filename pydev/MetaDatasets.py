
from ConfigUtils import *
from Wkbk_Json import *


class MetaDatasets:
  '''utility functions to handle metadatasets'''


  def __init__(self,  configItems, socrataQueriesObject, logger):
    self._config_inputdir = configItems['config_dir']
    self._metadata_config_fn = configItems['metadataset_config']
    self._metadataset_config = ConfigUtils.setConfigs(self._config_inputdir,  self._metadata_config_fn )
    self._master_dd_config = self._metadataset_config['master_data_dictionary']
    self._fieldtypes_config = self._metadataset_config['field_types_dataset']
    self._pickle_dir = configItems['pickle_dir']
    self._socrataQueriesObject = socrataQueriesObject
    self._logger = logger
    self._wkbk_json = WkbkJson(configItems, logger)

  def get_master_metadataset_as_json(self):
    results_json = self._socrataQueriesObject.pageThroughResultsSelect( self._master_dd_config['fourXFour'], '*')
    return self._wkbk_json.write_json_object(results_json, self._pickle_dir, self._master_dd_config['json_fn'])

  def get_field_types(self):
    results_json  = self._socrataQueriesObject.pageThroughResultsSelect( self._fieldtypes_config['fourXFour'], self._fieldtypes_config['columns_to_fetch'])
    return self._wkbk_json.write_json_object(results_json, self._pickle_dir, self._fieldtypes_config['json_fn'])

  def get_base_datasets(self):
    downloaded_datasets = False
    downloaded_master_dd = self.get_master_metadataset_as_json()
    downloaded_fieldtypes = self.get_field_types()
    if downloaded_master_dd and downloaded_fieldtypes:
      downloaded_datasets = True
    return downloaded_datasets


if __name__ == "__main__":
    main()
