# coding: utf-8

from optparse import OptionParser
from Screendoor_Stuff import *
from MetaDatasets import *
from Wkbk_Parser import *
from Utils import *
from ConfigUtils import *
from Wkbk_Json import *
import sys
from SocrataStuff import *
from PyLogger import *
from JobStatusEmailerComposer import *

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

  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  return config_inputdir, fieldConfigFile

def main():
  config_inputdir, fieldConfigFile =  parse_opts()
  configItems = ConfigUtils.setConfigs(config_inputdir, fieldConfigFile )
  configItems['app_name']= "Upload Wkbks"
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  screendoor_stuff = ScreenDoorStuff(configItems)
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sqry = SocrataQueries(clientItems, configItems, logger)
  metadatasets = MetaDatasets(configItems, sqry, logger)
  master_dd_json = metadatasets.get_master_metadataset_as_json()
  if (not (master_dd_json)):
    print "*****errror could not grab the master dd"
    exit(1)
  dsse = JobStatusEmailerComposer(configItems, logger)
  print "*****Starting to download workbooks*****"
  downloaded_files, number_of_wkbks_to_load = screendoor_stuff.get_attachments()
  if downloaded_files:
    print "Awesome, downloaded files and made json list"
    #print "Downloaded " + str(number_of_wkbks_to_load) + " wkbks"
  else:
    print "ERROR: Not able to download load workbooks!"
    exit(1)
  wkbk_parser = WkbkParser(configItems)
  updt_metadata_fields_json, unmatchedFn = wkbk_parser.get_metadata_updt_fields_from_shts()
  if (not(updt_metadata_fields_json)):
      print "Error: Something went wrong, could not parse worksheets"
      exit(1)
  else:
      print "successfully grabbed metadata fields to update from worksheets"
      print "Updating rows in the master dd"
      updt_rows = WkbkJson.loadJsonFile(configItems['pickle_dir'], configItems['updt_fields_json_fn'])
      print updt_rows
      if(len(updt_rows['updt_fields'])> 0):
        dataset_info = metadatasets.set_master_dd_updt_info(updt_rows['updt_fields'])
        print dataset_info
        #post update master dd on portal
        dataset_info = scrud.postDataToSocrata(dataset_info, updt_rows['updt_fields'] )
        print dataset_info
        dataset_info['jobStatus'] = 'success'
        dataset_info['isLoaded'] = 'Success'
        dsse.sendJobStatusEmail([dataset_info], [{unmatchedFn: configItems['pickle_dir']+unmatchedFn}])
      else:
        print "**** No rows to update*****"
        dataset_info = metadatasets.set_master_dd_updt_info(updt_rows)
        dataset_info['isLoaded'] = 'success'
        dsse.sendJobStatusEmail([dataset_info], [{unmatchedFn: configItems['pickle_dir']+unmatchedFn}])



if __name__ == "__main__":
    main()
