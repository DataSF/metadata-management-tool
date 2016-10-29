# coding: utf-8

from Utils import *
from Gpread_Stuff import *
from optparse import OptionParser
from Emailer import *
from WkBk_Writer import *
from Wkbk_Generator import *
from Wkbk_Json import *
from UpdateMetadata import *
from ExistingFieldDefs import *
from Gpread_Stuff import *


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

#instantiate classes
#emailer =  Emailer(configItems)
#gspread_stuff = gSpread_Stuff(configItems)
#wkbk_json = WkbkJson(configItems)


#set up the configs
wkbk_key = configItems["wkbk_key_master"]
sht_name_datadict  = configItems['datadict']['sht_name']
pickle_name_datadict  = configItems['datadict']['pickle_name']
sht_name_stewards  =  configItems['stewards']['sht_name']
pickle_name_stewards  = configItems['stewards']['pickle_name']
sht_name_field_types = configItems['field_types']['sht_name']
pickle_name_fieldtypes  = configItems['field_types']['pickle_name']

def unpickle_cells(pickle_name):
    return pickle.load( open( "../pickled/" + pickle_name +"_pickled_cells.p", "rb" ) )


print "********getting metadata fields from google spreadsheets***********"
#gspread_stuff.getMetaDataset(wkbk_key, sht_name_datadict, pickle_name_datadict)
#gspread_stuff.getMetaDataset(wkbk_key, sht_name_stewards,pickle_name_stewards )
#gspread_stuff.getMetaDataset(wkbk_key, sht_name_field_types, pickle_name_fieldtypes)

cells_dataDict = unpickle_cells(pickle_name_datadict)
#print cells_dataDict
#cells_stewards = gspread_stuff.unpickle_cells(pickle_name_stewards)
#fieldtype_cells = gspread_stuff.unpickle_cells(pickle_name_fieldtypes)

ef = ExistingFieldDefs(configItems, cells_dataDict)
#existing_fields_def_list = ef.get_datasetsToLoadList()
stuff = ef.buildDocumentedFields()
