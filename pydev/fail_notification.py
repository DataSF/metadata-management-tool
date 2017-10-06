# coding: utf-8
#!/usr/bin/env python


from optparse import OptionParser
import sys
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
  helpmsgConfigDir = 'Use the -m to add a fail notication messge. EX: "Failed: This job failed"'
  parser.add_option('-m', '--fail_msg',
                      action='store',
                      dest='fail_msg',
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
  fail_msg = options.fail_msg
  return config_inputdir, fieldConfigFile, fail_msg


def main():
  reload(sys)
  config_inputdir, fieldConfigFile, fail_msg = parse_opts()
  configItems = ConfigUtils.setConfigs(config_inputdir, fieldConfigFile )
  configItems['job_name'] = fail_msg
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  dsse = JobStatusEmailerComposer(configItems, logger)
  dataset_info = {'Socrata Dataset Name': fail_msg, 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': "Job Failed"}
  dataset_info['isLoaded'] = 'failed'
  dsse.sendJobStatusEmail([dataset_info])

if __name__ == "__main__":
    main()