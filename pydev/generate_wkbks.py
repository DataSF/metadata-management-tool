# coding: utf-8

from Utils import *
from Generate_DataDicts import *
from Gpread_Stuff import *
from optparse import OptionParser
from Emailer import *
from MetaData_Email_Composer import *
from WkBk_Writer import *
from Wkbk_Generator import *
from Wkbk_Json import *
from UpdateMetadata import *


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
emailer =  Emailer(configItems)
gspread_stuff = gSpread_Stuff(configItems)
wkbk_json = WkbkJson(configItems)


#set up the configs
wkbk_key = configItems["wkbk_key_master"]
sht_name_datadict  = configItems['datadict']['sht_name']
pickle_name_datadict  = configItems['datadict']['pickle_name']
sht_name_stewards  =  configItems['stewards']['sht_name']
pickle_name_stewards  = configItems['stewards']['pickle_name']
sht_name_field_types = configItems['field_types']['sht_name']
pickle_name_fieldtypes  = configItems['field_types']['pickle_name']


print "********getting metadata fields from google spreadsheets***********"
#gspread_stuff.getMetaDataset(wkbk_key, sht_name_datadict, pickle_name_datadict)
#gspread_stuff.getMetaDataset(wkbk_key, sht_name_stewards,pickle_name_stewards )
#gspread_stuff.getMetaDataset(wkbk_key, sht_name_field_types, pickle_name_fieldtypes)

#cells_dataDict = gspread_stuff.unpickle_cells(pickle_name_datadict)
#cells_stewards = gspread_stuff.unpickle_cells(pickle_name_stewards)
#fieldtype_cells = gspread_stuff.unpickle_cells(pickle_name_fieldtypes)

#wkbk_writer = WkBkWriter(configItems,fieldtype_cells)
print
print "********generating workbooks**************************"
#wkbk_generator = WkbkGenerator(configItems, cells_dataDict,cells_stewards)
 
 
#wkbks_json = wkbk_generator.build_Wkbks(wkbk_writer)
#json_obj = wkbk_json.write_json_object_wkbks(wkbks_json)
#if json_obj:
#    print "successfully output wkbks"

print "********updating google spreadsheets**************** "

wkbks = wkbk_json._wkbks
wkbk_cells_updted = None
#update_metadata_status = UpdateMetadataStatus(configItems, gspread_stuff)

#update_successful, wkbk_cells_updted = update_metadata_status.updatewkbk_info(wkbks)

#if update_successful:
 #   print "Success- Cells were successfully updated"
#else:
#   print "FAILED- Something went wrong-cells where not successfully updated"
#
print "*************Sendng out emails ******"
emailer_review_steward = ForReviewBySteward(configItems, emailer)
wkbks_sent_out = emailer_review_steward.generate_All_Emails(wkbks, wkbk_cells_updted)
print wkbks_sent_out