# coding: utf-8

from optparse import OptionParser
from Screendoor_Stuff import *
from UpdateMetadata import *
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

  print "*****Starting to download workbooks*****"
  downloaded_files = False
  downloaded_files, number_of_wkbks_to_load = screendoor_stuff.get_attachments()
  if downloaded_files:
    print "Awesome, downloaded files and made json list"
    print "Downloaded " + str(number_of_wkbks_to_load) + " wkbks"
  else:
    print "ERROR: Not able to download load workbooks!"
    exit(1)
  wkbk_parser = WkbkParser(configItems)
  updt_metadata_fields_json = wkbk_parser.get_metadata_updt_fields_from_shts()
  wrote_updshts_json = False

  if (not(updt_metadata_fields_json)):
      print "Error: Something went wrong, could not parse worksheets"
      exit(1)
  else:
      print "successfully grabbed metadata fields to update from worksheets"
      #wrote_updshts_json = wkbk_parser.load_updt_fields_json()
      if master_dd_json:
        print "Updating rows in the master dd"
        updt_rows = WkbkJson.loadJsonFile(configItems['pickle_dir'], configItems['updt_fields_json_fn'])
        print updt_rows
        if(len(updt_rows['updt_fields'])> 0):
          dataset_info = metadatasets.set_master_dd_updt_info(updt_rows['updt_fields'])
          #post update master dd on portal
          dataset_info = scrud.postDataToSocrata(dataset_info, updt_rows['updt_fields'] )
          print dataset_info
          dsse = JobStatusEmailerComposer(configItems, logger)
          dsse.sendJobStatusEmail([dataset_info])
        else:
          print "**** No rows to update*****"
      else:
        print "*****errror could not upload dataasets"
        exit(1)

if __name__ == "__main__":
    main()
