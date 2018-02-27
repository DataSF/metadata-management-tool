# coding: utf-8
#!/usr/bin/env python

#updates datadict dataset
from optparse import OptionParser
from ConfigUtils import *
from SocrataStuff import *
from Utils import *
from JobStatusEmailerComposer import *
from PyLogger import *
from MasterDataset import *


def getColInfo(col, row):
  row['internalcolumnid'] = col['id']
  row.columnid = row.systemid + "_" + col['fieldName']
  row.field_name = col['name']
  row['field_type'] = col['dataTypeName']
  row['field_render_type'] = col['renderTypeName']
  row['field_api_name'] = col['fieldName']
  if 'description' in col.keys():
    row['field_description'] = col['description']
  #row['rowsupdatedat'] = row['rowsupdatedat'].strftime('%Y-%m-%d')
  return row


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

  fieldConfigFile = None
  config_inputdir = None
  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  return config_inputdir, fieldConfigFile

def main():
  config_inputdir, fieldConfigFile = parse_opts()
  cI =  ConfigUtils(config_inputdir ,fieldConfigFile  )
  configItems = cI.getConfigs()
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  logger.info("****************JOB START******************")
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  socrataLoadUtils = SocrataLoadUtils(configItems)
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sqry = SocrataQueries(clientItems, configItems, logger)
  datasets = socrataLoadUtils.make_datasets()
  finshed_datasets = []

  for dataset in datasets:
    insertDataSet, dataset = socrataLoadUtils.makeInsertDataSet(dataset)
    dataset = scrud.postDataToSocrata(dataset, insertDataSet )
    finshed_datasets.append(dataset)

  q_asset_inventory = "?$select=u_id as systemid, name as dataset_name, creation_date as createdat, last_update_date_data as rowsupdatedat, publishing_department as department  where type = 'dataset' and publication_stage = 'published' and public = 'true' and derived_view = 'false' and publishing_department is not null and publishing_department != 'other' "
  results_json =   sqry.getQry('g9d8-sczp', q_asset_inventory)
  df_asset_inventory = BuildDatasets.makeDf(results_json)
  q_asset_fields = "?$select=systemid as datasetid, dataset_name as asset_field_dataset_name group by systemid, dataset_name"
  results_json2 =  sqry.getQry('skzx-6gkn', q_asset_fields)
  df_asset_fields = BuildDatasets.makeDf(results_json2)
  asset_field_datasetids = list(df_asset_fields['datasetid'])
  missing_datasets_orig = df_asset_inventory[~df_asset_inventory.systemid.isin(asset_field_datasetids) ].reset_index()
  missing_datasets = missing_datasets_orig.copy()
  print missing_datasets 
  missing_datasets['createdat'] =  (pd.to_datetime(missing_datasets['createdat'],unit='ms'))
  missing_datasets['rowsupdatedat'] =  (pd.to_datetime(missing_datasets['rowsupdatedat'],unit='ms'))
  missing_datasets['rowsupdatedat'] = missing_datasets['rowsupdatedat'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
  missing_datasets['createdat'] = missing_datasets['createdat'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
  extra_cols = ['columnid', 'internalcolumnid', 'data_type', 'field_name', 'field_type', 'field_render_type', 'field_api_name', 'field_description']
  for col in extra_cols:
    missing_datasets[col] =  ''
  missing_datasets['data_type'] = 'tabular'
  base_qry = 'https://data.sfgov.org/api/views/'
  all_rows = []
  for index, row in missing_datasets.iterrows():
    qry = base_qry + row['systemid'] + '.json'
    results = sqry.getQryGeneric(qry)
    cols = results['columns']
    for col in cols:
      new_row = getColInfo(col, row)
      all_rows.append(new_row.to_dict())
  
  dataset_extra = datasets[1]
  dataset_extra['row_id'] = 'columnid'
  #print all_rows
  dataset2 = scrud.postDataToSocrata(dataset_extra, all_rows )

  print datasets[1]
  logger.info(finshed_datasets)
  dsse = JobStatusEmailerComposer(configItems, logger)
  dsse.sendJobStatusEmail(finshed_datasets)



if __name__ == "__main__":
    main()
