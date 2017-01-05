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
from JobStatusEmailerComposer import *

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
  dsse = JobStatusEmailerComposer(configItems, logger)
  #get these supporting datasets from the portal
  master_dd_json = metadatasets.get_master_metadataset_as_json()
  global_fields_json = metadatasets.get_global_fields_as_json()
  ef = ExistingFieldDefs(configItems)
  wroteFileDefs, wroteFileOnlyInAttach, wroteFileOnlyInMaster, wrotePdfDatasets, wroteOthersDatasets = ef.buildDocumentedFields()
  if(wroteFileDefs):
    #load the csvs
    updt_rows = FileUtils.read_csv_into_dictlist(configItems['documented_fields_dir']+"output/"+ configItems['document_fields_outputfile_fn'])
    if len(updt_rows) > 0:
      dataset_info = metadatasets.set_master_dd_updt_info(updt_rows)
      print dataset_info
      #post update master dd on portal
      dataset_info = scrud.postDataToSocrata(dataset_info, updt_rows )
      print dataset_info
      #send out the email
      dsse.sendJobStatusEmail([dataset_info])
    else:
      dataset_info = metadatasets.set_master_dd_updt_info(updt_rows)
      dataset_info['isLoaded'] = 'success'
      print "*******No rows to update****"
      dsse.sendJobStatusEmail([dataset_info])

if __name__ == "__main__":
    main()
