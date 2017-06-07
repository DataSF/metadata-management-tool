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
  #get these supporting datasets from the portal
  master_dd_json = metadatasets.get_master_metadataset_as_json()
  job_success = False
  if master_dd_json:
    #do a final pass to see if the field is documented
    updt_rows = WkbkJson.loadJsonFile(configItems['pickle_data_dir'], 'master_data_dictionary.json')
    df_master = PandasUtils.makeDfFromJson(updt_rows)
    #print df_master.columns
    #print 
    #print df_master[['columnid', 'field_documented']].head(10)
    df_master = metadatasets.calc_field_documented(df_master)

    dfList =  df_master[['columnid', 'field_documented']]
    dfList = PandasUtils.convertDfToDictrows(dfList)
    dataset_info = metadatasets.set_master_dd_updt_info(dfList)
    #print dfList
    dataset_info = scrud.postDataToSocrata(dataset_info, dfList )
    #now push the public version of the master data dictionary.
    pm = PublicMetadata(configItems)
    wroteJsonFile = pm.get_public_fields()
    print "successfully grabbed metadata fields to update from public master data dictionary"
    updt_rows = WkbkJson.loadJsonFile(configItems['pickle_dir'], configItems['output_json_fn'])
    if(len(updt_rows['updt_fields'])> 0):
      dataset_info = metadatasets.set_public_dd_updt_info(updt_rows['updt_fields'])
      #replace the dataset on the portal rather than upserting
      dataset_info['row_id'] = ''
      dataset_info = scrud.postDataToSocrata(dataset_info, updt_rows['updt_fields'] )
      dsse = JobStatusEmailerComposer(configItems, logger)
      dsse.sendJobStatusEmail([dataset_info])
      job_success = True

  if(not(job_success)):
    dataset_info = {'Socrata Dataset Name': "Public DD", 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': "Job Failed"}
    dataset_info['isLoaded'] = 'failed'
    dsse.sendJobStatusEmail([dataset_info])


if __name__ == "__main__":
    main()

