# coding: utf-8
from Screendoor_Stuff import *
from optparse import OptionParser
from Utils import *
from Wkbk_Json import *
import sys  

reload(sys)  
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

configItems = myUtils.setConfigs(config_inputdir, fieldConfigFile )

screendoor_stuff = ScreenDoorStuff(configItems)
downloaded_files = screendoor_stuff.get_attachments()

jsonfile = WkbkJson.write_json_object(downloaded_files, screendoor_stuff._wkbk_uploads_dir, screendoor_stuff._wkbk_uploads_json_fn)
print jsonfile