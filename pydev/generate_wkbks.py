# coding: utf-8

from Utils import *
from PyLogger import *
from optparse import OptionParser
from Emailer import *
from MetaData_Email_Composer import *
from WkBk_Writer import *
from Wkbk_Generator import *
from Wkbk_Json import *
from UpdateMetadata import *
from SocrataStuff import *
from ConfigUtils import *
from MetaDatasets import *
from Emailer import *
from MetaData_Email_Composer import *
import sys


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
  reload(sys)
  sys.setdefaultencoding('utf8')
  config_inputdir, fieldConfigFile = parse_opts()
  configItems = ConfigUtils.setConfigs(config_inputdir, fieldConfigFile )
  configItems['app_name']= "Generate Workbooks"
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  logger.info("****************JOB START- generating workbooks******************")
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sqry = SocrataQueries(clientItems, configItems, logger)
  metadatasets = MetaDatasets(configItems, sqry, logger)
  emailer =  Emailer(configItems)
  wkbk_json = WkbkJson(configItems, logger)
  #metadata_json = metadatasets.get_base_datasets()
  metadata_json = True
  if metadata_json:
    print "Awesome! Downloaded master dd"
    wkbk_generator = WkbkGenerator(configItems,logger)
    generated_wkbks, update_rows = wkbk_generator.build_Wkbks()
    if generated_wkbks:
      print "Awesome, generated data steward workbooks!"
      dataset_info = metadatasets.set_master_dd_updt_info(update_rows)
      dataset_info = scrud.postDataToSocrata(dataset_info, update_rows )
      print dataset_info
  #now email out the workbooks
  wkbks = wkbk_json.loadJsonFile(configItems['pickle_dir'], configItems['wkbk_output_json'])
  print wkbks
  emailer_review_steward = ForReviewBySteward(configItems, emailer)
  wkbks_sent_out = emailer_review_steward.generate_All_Emails(wkbks)
  print wkbks_sent_out


if __name__ == "__main__":
    main()
