
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

def parse_opts():
  helpmsgConfigFile = 'Use the -c to add a config yaml file. EX: fieldConfig.yaml'
  parser = OptionParser(usage='usage: %prog [options] ')
  parser.add_option('-c', '--configfile',
                      action='store',
                      dest='configFn',
                      default=None,
                      help=helpmsgConfigFile ,)

  helpmsgConfigDir = 'Use the -d to add directory path for the config files. EX: /home/ubuntu/configs'
  parser.add_option('-d', '--configdir',
                      action='store',
                      dest='configDir',
                      default=None,
                      help=helpmsgConfigDir ,)

  (options, args) = parser.parse_args()

  if  options.configFn is None:
    print "ERROR: You must specify a config yaml file!"
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


  dfs_dict = BuildDatasets.getDatasets(tables, sqry)
  df_sizes = {}
  df_names = dfs_dict.keys()
  for dataset in df_names:
    df_sizes[dataset] = len(dfs_dict[dataset])
  logger.info(df_sizes)

  master_data_dictionary = configItems['master_data_dictionary']
  base_url = configItems['base_url']


  mm_df = MasterDataDictionary.build_base(dfs_dict)
  mm_df = MasterDataDictionary.add_calculated_fields(mm_df, base_url, dfs_dict)
  mm_dd_rows =  MasterDataDictionary.export_master_df_as_rows(mm_df)
  dataset_info = MasterDataDictionary.set_dataset_info(configItems, socrataLoadUtils, mm_dd_rows)
  dataset_info = scrud.postDataToSocrata(dataset_info, mm_dd_rows )
  print dataset_info


  logger.info(dataset_info)
  dsse = JobStatusEmailerComposer(configItems, logger)
  dsse.sendJobStatusEmail([dataset_info])



if __name__ == "__main__":
    main()



