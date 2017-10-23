# coding: utf-8
#!/usr/bin/env python

import sys
from Utils import *
from optparse import OptionParser
from Emailer import *
from WkBk_Writer import *
from Wkbk_Generator import *
from Wkbk_Json import *
from ExistingFieldDefs import *
from SocrataStuff import *
from ConfigUtils import *
from PyLogger import *
from JobStatusEmailerComposer import *
from UploadAssetFieldDefs import *
from PublicMetadata import *
from MetaDatasets import *

def parse_opts():
  sys.setdefaultencoding('utf8')
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
  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  return config_inputdir, fieldConfigFile

def main():
  reload(sys)
  config_inputdir, fieldConfigFile = parse_opts()
  configItems = ConfigUtils.setConfigs(config_inputdir, fieldConfigFile )
  app_name = configItems['app_name']
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sqry = SocrataQueries(clientItems, configItems, logger)
  metadatasets = MetaDatasets(configItems, sqry, logger)
  dsse = JobStatusEmailerComposer(configItems, logger)
  job_success = False
  '''
  #get these supporting datasets from the portal
  master_dd_json = metadatasets.get_master_metadataset_as_json()
 
  if master_dd_json:
    #do a final pass to see if the field is documented

    updt_rows = WkbkJson.loadJsonFile(configItems['pickle_data_dir'], 'master_data_dictionary.json')
    df_master = PandasUtils.makeDfFromJson(updt_rows)
    df_master = metadatasets.calc_field_documented(df_master)
    dfList =  df_master[['columnid', 'field_documented']]
    dfList = PandasUtils.convertDfToDictrows(dfList)
    dataset_info = metadatasets.set_master_dd_updt_info(dfList)
    #push to socrata
    dataset_info = scrud.postDataToSocrata(dataset_info, dfList )
  '''
  #now push the public version of the master data dictionary.
  public_dd_json = metadatasets.get_master_dd_public_as_json()
  print 
  print len (public_dd_json)
  print 
  if len(public_dd_json) > 0 :
    #load = [{'field_type': 'geometry: line', 'field_alias': 'Geometry', 'field_documented': True, 'columnid': 'ejbq-qm2c_the_geom', 'data_dictionary_attached': False, 'department': 'GSA - Technology', 'field_type_flag': '', 'field_definition': 'Contains the geometry of the record in Well Known Text (WKT) format.', 'dataset_name': 'stclines_highways', 'inventoryid': 'TIS-0017', 'open_data_portal_url': 'http://data.sfgov.org/resource/ejbq-qm2c', 'api_key': u'the_geom', 'field_name': 'the_geom', 'datasetid': 'ejbq-qm2c'}]
    public_master_dictList = PublicMetadata.set_master_df_public_list(public_dd_json)
    for item in public_master_dictList:
      if item['datasetid'] = '8zp7-ik63'
        print "***"
        print item
        print "****"
    dataset_info = metadatasets.set_public_dd_updt_info(public_master_dictList)
    print dataset_info
    #replace the dataset on the portal rather than upserting
    dataset_info['row_id'] = ''
    #push to socrata
    dataset_info = scrud.postDataToSocrata(dataset_info, public_master_dictList )
    dsse = JobStatusEmailerComposer(configItems, logger)
    dsse.sendJobStatusEmail([dataset_info])
    job_success = True   
  if(not(job_success)):
    dataset_info = {'Socrata Dataset Name': "Public DD", 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': "Job Failed"}
    dataset_info['isLoaded'] = 'failed'
    dsse.sendJobStatusEmail([dataset_info])


if __name__ == "__main__":
    main()

