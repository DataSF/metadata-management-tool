# coding: utf-8
#!/usr/bin/env python

from optparse import OptionParser
from ConfigUtils import *
from SocrataStuff import *
from Utils import *
from PandasUtils import *
from MasterDataset import *
from JobStatusEmailerComposer import *
from PyLogger import *
from MetaDatasets import *
from  MetadataMaintenance import *

def parse_opts():
  helpmsgConfigFile = 'Use the -c to add a config yaml file. EX: fieldConfig.yaml'
  parser = OptionParser(usage='usage: %prog [options] ')
  parser.add_option('-c', '--configfile',
                      action='store',
                      dest='configFn',
                      default=None,
                      help=helpmsgConfigFile ,)

  helpmsgConfigDir = 'Use the -d to add directory path for the config files. EX: /home/ubuntu/configs'
  parser.add_option ('-d', '--configdir',
                      action='store',
                      dest='configDir',
                      default=None,
                      help=helpmsgConfigDir ,)

  (options, args) = parser.parse_args()

  if  options.configFn is None:
    print 'ERROR: You must specify a config yaml file! '
    print helpmsgConfigFile
    exit(1)
  elif options.configDir is None:
    print "ERROR: You must specify a directory path for the config files!"
    print helpmsgConfigDir
    exit(1)

  config_inputdir = None
  fieldConfigFile = None
  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  return fieldConfigFile, config_inputdir

def main():
  fieldConfigFile, config_inputdir = parse_opts()
  cI =  ConfigUtils(config_inputdir ,fieldConfigFile  )
  configItems = cI.getConfigs()
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  logger.info("****************JOB START******************")
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  socrataLoadUtils = SocrataLoadUtils(configItems)
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sqry = SocrataQueries(clientItems, configItems, logger)
  tables = configItems['tables']
  tables =  {'asset_fields': tables['asset_fields']}
  metadatasets = MetaDatasets(configItems, sqry, logger)
  dfs_dict = BuildDatasets.getDatasets(tables, sqry)
  asset_fields = dfs_dict['asset_fields']
  df_master_dd = []
  master_dd_json = metadatasets.get_master_metadataset_as_json()
  #master_dd_json = True
  master_dd_json_obj = WkbkJson.loadJsonFile(configItems['inputDataDir'], configItems['master_dd_json_fn'])
  df_master_dd = PandasUtils.makeDfFromJson(master_dd_json_obj)
  job_success = False
  if(len(df_master_dd) > 0 and len(asset_fields) > 0) :
    nbeid_list = NbeIds.get_nbeids_final(configItems, sqry, df_master_dd, asset_fields)
    dataset_info = MasterDataDictionary.set_dataset_info(configItems, socrataLoadUtils, nbeid_list)
    dataset_info = scrud.postDataToSocrata(dataset_info, nbeid_list )
    print dataset_info
    dsse = JobStatusEmailerComposer(configItems, logger)
    dsse.sendJobStatusEmail([dataset_info])
    job_success = True
  if(not(job_success)):
    dataset_info = {'Socrata Dataset Name': "NbeIds", 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': "Job Failed"}
    dataset_info['isLoaded'] = 'failed'
    dsse.sendJobStatusEmail([dataset_info])
  updateMasterDDs = MetadataMaintenance.deleteStaleMasterDDRows(configItems, scrud, df_master_dd, asset_fields)
  print updateMasterDDs
  df_master_dd_documented =  MetaDatasets.update_field_documented(df_master_dd)
  df_master_dd_documented = df_master_dd_documented[['columnid', 'field_documented']].reset_index()
  document_dictList = PandasUtils.convertDfToDictrows(df_master_dd_documented)
  dataset_info = MasterDataDictionary.set_dataset_info(configItems, socrataLoadUtils, document_dictList)
  dataset_info = scrud.postDataToSocrata(dataset_info, document_dictList )
  print dataset_info



if __name__ == "__main__":
    main()



