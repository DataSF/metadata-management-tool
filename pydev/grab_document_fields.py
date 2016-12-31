# coding: utf-8
import sys
from Utils import *
from optparse import OptionParser
from Emailer import *
from WkBk_Writer import *
from Wkbk_Generator import *
from Wkbk_Json import *
from UpdateMetadata import *
from ExistingFieldDefs import *
from SocrataStuff import *
from ConfigUtils import *
from PyLogger import *

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
  configItems['app_name']= "Upload Data Dictionary Attachments"
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sqry = SocrataQueries(clientItems, configItems, logger)
  metadatasets = MetaDatasets(configItems, sqry, logger)
  #master_dd_json = metadatasets.get_master_metadataset_as_json()
  ef = ExistingFieldDefs(configItems)
  stuff = ef.buildDocumentedFields()
  #

if __name__ == "__main__":
    main()
