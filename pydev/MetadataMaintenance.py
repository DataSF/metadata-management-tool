from PandasUtils import *
import pandas as pd


class MetadataMaintenance:
  '''utility functions to handle metadata maintenance'''
  @staticmethod
  def calcRowsToDeleteMasterDD(df_master_dd, df_asset_fields):
    asset_fields_ids = list(df_asset_fields['columnid'])
    master_dd_remove = df_master_dd[~df_master_dd.columnid.isin(asset_fields_ids)]
    rowsToRemove =  list(master_dd_remove['columnid'])
    return rowsToRemove

  @staticmethod
  def deleteStaleMasterDDRows(configItems, scrud, df_master_dd, df_asset_fields):
    '''Removes non-exist fields from master data dictionary; asset_fields and master dds are pandas df objects'''
    datasetsToUpdate = ['master_data_dictionary', 'public_data_dictionary', 'field_profiles' ]
    rowsToRemove = 0
    rowsToRemove =  MetadataMaintenance.calcRowsToDeleteMasterDD(df_master_dd, df_asset_fields)
    results = []
    if len(rowsToRemove) > 0:
      for dataset in datasetsToUpdate: 
        result = {'fbf': configItems[dataset]['fourXFour'], 'rowsToDelete': len(rowsToRemove) , 'deletedRows': 0}
        result['deletedRows'] = scrud.deleteRows(result['fbf'], rowsToRemove)
        print result
        results.append(result)
    return results