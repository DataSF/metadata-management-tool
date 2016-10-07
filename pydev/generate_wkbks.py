# coding: utf-8

from ConfigUtils import *
from Generate_DataDicts import *
from Gpread_Stuff import *
from optparse import OptionParser
from Emailer import *
from MetaData_Email_Composer import *

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

cI =  ConfigItems(config_inputdir, fieldConfigFile  )
configItems = cI.getConfigs()



wkbk_key = configItems["wkbk_key_master"]

sht_name_datadict  = configItems['datadict']['sht_name']
pickle_name_datadict  = configItems['datadict']['pickle_name']
sht_name_stewards  =  configItems['stewards']['sht_name']
pickle_name_stewards  = configItems['stewards']['pickle_name']
sht_name_field_types = configItems['field_types']['sht_name']
pickle_name_fieldtypes  = configItems['field_types']['pickle_name']


#instantiate classes
emailer =  Emailer(configItems)
gs = gSpread_Stuff(configItems)

print "********getting metadata fields from google spreadsheets***********"
gs.getMetaDataset(wkbk_key, sht_name_datadict, pickle_name_datadict)
gs.getMetaDataset(wkbk_key, sht_name_stewards,pickle_name_stewards )
gs.getMetaDataset(wkbk_key, sht_name_field_types, pickle_name_fieldtypes)

cells_dataDict = gs.unpickle_cells(pickle_name_datadict)
cells_stewards = gs.unpickle_cells(pickle_name_stewards)
fieldtype_cells = gs.unpickle_cells(pickle_name_fieldtypes)
print
print "********generating workbooks**************************"
dd = Generate_DataDicts(configItems, cells_dataDict, cells_stewards,fieldtype_cells)
workbooks = dd.build_Wkbk()


wkbkList = AfterCreateDataDicts(configItems)

print "********updating google spreadsheets**************** "
udd = UpdateDataDictsAfterCreation( gs, wkbkList, configItems)
if udd.updatewkbk_info():
    print "Success- Cells were successfully updated"
else:
    print "FAILED- Something went wrong-cells where not successfully updated"

print "*************Sendng out emails ******"
em_rs = ForReviewBySteward(configItems, emailer)
em_rs.generate_All_Emails(ls)