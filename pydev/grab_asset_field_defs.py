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
  master_dd_json  = True
  asset_fields_json =  True
  master_dd_json = metadatasets.get_master_metadataset_as_json()
  asset_fields_json = metadatasets.get_asset_fields_as_json()
  job_success = False
  if asset_fields_json and master_dd_json:
    iafd = UploadAssetFieldDefs(configItems)
    wroteJsonFile = iafd.getFieldDefList()
    if wroteJsonFile:
      updt_rows = WkbkJson.loadJsonFile(configItems['pickle_dir'], configItems['output_json_fn'])
      if(len(updt_rows[configItems['json_key']])> 0):
          dataset_info = metadatasets.set_master_dd_updt_info(updt_rows['updt_fields'])
          dataset_info = scrud.postDataToSocrata(dataset_info, updt_rows['updt_fields'] )
          dsse = JobStatusEmailerComposer(configItems, logger)
          dsse.sendJobStatusEmail([dataset_info])
          job_success = True
          logger.info("****************END JOB******************")
  if(not(job_success)):
    dataset_info = {'Socrata Dataset Name': "Master DD", 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': "Job Failed"}
    dataset_info['isLoaded'] = 'failed'
    #dsse.sendJobStatusEmail([dataset_info])



if __name__ == "__main__":
    main()

