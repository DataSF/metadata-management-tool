

# coding: utf-8
#!/usr/bin/env python
import json
import pandas as pd
from pandas.io.json import json_normalize
from PandasUtils import *
from DictUtils import *
import inflection
from Utils import *
from ConfigUtils import *
import urllib

class NbeIds:
    @staticmethod
    def get_nbeids(df, socrataQueriesObject, configItems):
      def get_nbe_id(row,socrataQueriesObject):
          qry = configItems['migration_qry_url'] + row['datasetid'] + '.json'
          results = socrataQueriesObject.getQryGeneric(qry)
          if 'nbeId' in results.keys():
              print results['nbeId']
              return results['nbeId']
          else:
              return ''
      df['nbeid'] = df.apply(lambda row: get_nbe_id(row, socrataQueriesObject), axis=1)
      return df

    @staticmethod
    def get_assetfields_nbeid(asset_fields):
        asset_fields = asset_fields[asset_fields['nbeid_geo'] != '']
        asset_fields =  PandasUtils.groupbyCountStar(asset_fields, ['datasetid', 'nbeid_geo'])
        asset_fields = asset_fields[['datasetid', 'nbeid_geo']]
        return asset_fields


    @staticmethod
    def shift_nbeid_geo_to_nbeid(row):
        if row['nbeid_geo'] != '':
            return row['nbeid_geo']
        if row['nbeid'] == '':
            return row['datasetid']
        return row['nbeid']

    @staticmethod
    def get_nbeids_final(configItems, socrataQueriesObject, df_master_dd, asset_fields):
      df_datasetids =  PandasUtils.groupbyCountStar(df_master_dd, ['datasetid'])
      #df_datasetids = df_datasetids.head(10)
      df_datasetids = NbeIds.get_nbeids(df_datasetids, socrataQueriesObject, configItems)
      asset_fields_nbeidgeo = NbeIds.get_assetfields_nbeid(asset_fields)
      nbeid_df = pd.merge( df_datasetids, asset_fields_nbeidgeo, on='datasetid', how='left')
      #nbeid_df =  nbeid_df.head(10)
      nbeid_df = PandasUtils.fillNaWithBlank(nbeid_df)
      nbeid_df['nbeid'] = nbeid_df.apply(lambda row: NbeIds.shift_nbeid_geo_to_nbeid(row ),axis=1)
      nbeid_df = nbeid_df[['datasetid', 'nbeid']]
      df_master_dd = pd.merge(df_master_dd, nbeid_df, on='datasetid', how='left')
      df_master_dd = PandasUtils.fillNaWithBlank(df_master_dd)
      df_master_dd =  df_master_dd[df_master_dd['nbeid'] != '']
      df_master_dd = df_master_dd [['columnid', 'nbeid']]
      return PandasUtils.convertDfToDictrows(df_master_dd)

class BuildDatasets:
    '''class to build up the base of the master dataset'''
    @staticmethod
    def getQryCols(tables, dataset_name):
        dataset = tables[dataset_name]
        dataset_cols = dataset['field_list']
        dataset_cols_qry = ",".join(dataset_cols)
        return dataset_cols_qry

    @staticmethod
    def makeDf(json_obj):
        df = None
        if(json_obj):
            df = json_normalize(json_obj)
            df = df.fillna('')
        return df

    @staticmethod
    def getDatasets(tables, socrataQueriesObject):
        dfs_dict = {}
        datasets = tables.keys()
        for dataset in datasets:
            fbf = tables[dataset]['fbf']
            qryCols = BuildDatasets.getQryCols(tables, dataset)
            results_json = socrataQueriesObject.pageThroughResultsSelect(fbf, qryCols)
            df = BuildDatasets.makeDf(results_json)
            #print list(df.columns)
            #rename the fields
            if 'field_mapping' in tables[dataset].keys():
                df = PandasUtils.mapFieldNames(df, tables[dataset]['field_mapping'])
            dfs_dict[dataset] = df
        return dfs_dict


class MasterDataDictionary:
  
    @staticmethod
    def getXlxsDataDicts(data_dictionary_attachments):
      #print data_dictionary_attachments.columns
      df_dupes = PandasUtils.groupbyCountStar(data_dictionary_attachments, 'datasetid')
      df_dupes =  df_dupes[df_dupes['count'] > 1]
      df_dupe_ids = list(df_dupes['datasetid'])
      df_attachments_uniq =  data_dictionary_attachments[~data_dictionary_attachments['datasetid'].isin(df_dupe_ids)].reset_index(drop=True)
      df_attachments_uniq_list = list(df_attachments_uniq['datasetid'])
      df_dataset_dupe =  data_dictionary_attachments[~data_dictionary_attachments['datasetid'].isin(df_attachments_uniq_list)].reset_index(drop=True)
      df_dataset_dupe['isxls'] = df_dataset_dupe['attachment_url'].str.contains('xlsx')
      df_dataset_dupe = df_dataset_dupe[df_dataset_dupe['isxls'] == True].reset_index(drop=True)
      df_xls_list = list(df_dataset_dupe['datasetid'])
      df_not_in_both = data_dictionary_attachments[(~data_dictionary_attachments['datasetid'].isin(df_attachments_uniq_list)) & (~data_dictionary_attachments['datasetid'].isin(df_xls_list))]
      df_dataset_dupe = df_dataset_dupe[['attachment_url', 'data_dictionary_attached', 'datasetid']] 
      df_final_attachment =  pd.concat([df_attachments_uniq, df_dataset_dupe])
      return df_final_attachment

    @staticmethod
    def filter_base_datasets(dfs_dict):
        '''filter out irrelevant records in datasets'''
        #filter out the relevant data:
        asset_fields = dfs_dict['asset_fields']
        asset_inventory =  dfs_dict['asset_inventory']
        data_dictionary_attachments = dfs_dict['data_dictionary_attachments']
        dataset_inventory = dfs_dict['dataset_inventory']
        coordinators = dfs_dict['cordinators']
        ##asset_inventory-> we only want rows that are datsets
        asset_inventory = asset_inventory[ (asset_inventory['type']== 'dataset') | (asset_inventory['type']== 'esri map') | (asset_inventory['type']== 'gis map') ]
        #asset_inventory-> remove all the geo fields
        asset_fields = asset_fields[ (asset_fields['data_type'] ==  'tabular') | (asset_fields['data_type'] ==  'geo') ]

        #filter out records that don't have data_dictionaries + remove dupes
        data_dictionary_attachments = data_dictionary_attachments[data_dictionary_attachments['data_dictionary_attached'] ==  True]
        data_dictionary_attachments = MasterDataDictionary.getXlxsDataDicts(data_dictionary_attachments)

        #grab the primary data coordinator
        coordinators = coordinators[coordinators['primary'] == 'Yes']
        return asset_fields, asset_inventory, data_dictionary_attachments, dataset_inventory, coordinators

    @staticmethod
    def buildInventoryInfo(dataset_inventory,cordinators):
        '''joins the dataset inventory to the coordinators dataset'''
        dataset_inventory = pd.merge(dataset_inventory, cordinators, left_on='department_from_inventory', right_on='department', how='left')
        return dataset_inventory

    @staticmethod
    def build_base(dfs_dict):
        '''builds up all the non-transformed fields'''
        fields_to_include= ['last_updt_dt_data', 'created_date', 'columnid', 'datasetid', 'dataset_name', 'inventoryid', 'field_name', 'socrata_field_type', 'field_type', 'api_key', 'data_steward', 'data_steward_name', 'department_from_inventory', 'department_from_catalog', 'data_coordinator', 'data_dictionary_attached', 'attachment_url', 'field_definition'  ]
        asset_fields, asset_inventory, data_dictionary_attachments, dataset_inventory, coordinators = MasterDataDictionary.filter_base_datasets(dfs_dict)
        dataset_inventory =  MasterDataDictionary.buildInventoryInfo(dataset_inventory,coordinators)
        #join everything together to make the master dataset
        master_df = pd.merge(asset_fields, asset_inventory, on='datasetid', how='left').merge(data_dictionary_attachments, on='datasetid', how='left').merge(dataset_inventory, on='datasetid', how='left')
        master_df = master_df.fillna('')
        master_df = master_df[fields_to_include ]
        return master_df

    @staticmethod
    def set_blank_data_dictionary_attachment_flag(master_df):
        '''fills in blanks as false for data_dictionary_attached field'''
        def set_blank_data_dictionary_attachment_flag_row(row):
            if(row['data_dictionary_attached']):
                return True
            return False
        master_df['data_dictionary_attached'] = master_df.apply(lambda row:set_blank_data_dictionary_attachment_flag_row(row ),axis=1)
        return master_df

    @staticmethod
    def add_url(master_df, base_url):
        '''returns the url for a dataset'''
        master_df['open_data_portal_url'] =  "http://"+ base_url+ '/resource/' + master_df['datasetid']
        return master_df

    @staticmethod
    def add_default_privateordeleted(master_df):
        '''returns the url for a dataset'''
        master_df['privateordeleted'] =  False
        return master_df


    @staticmethod
    def calc_department_field(master_df, dfs_dict):
        '''implements the following logic:
           if an inventory dept exist, use it;
           if inventory dept blank, use the catalog dept; if using the catalog dept, use the lookup'''
        def calc_dept_row(row, dept_dict, dept_keys):
            dept = ''
            if row['department_from_inventory'] != '':
                dept = row['department_from_inventory']
            elif row['department_from_inventory'] == '':
                if row['department_from_catalog'] in dept_keys:
                    dept = dept_dict[row['department_from_catalog']]
            return dept
        df_dept_lookup = dfs_dict['department_lookup']
        dept_dict = dict(zip(list(df_dept_lookup['department_string']), list(df_dept_lookup['department_transform'])))
        dept_keys = dept_dict.keys()
        master_df['department'] = master_df.apply(lambda row:calc_dept_row(row,dept_dict, dept_keys ),axis=1)
        return master_df

    @staticmethod
    def calc_global_field(master_df, dfs_dict):
        '''flags whether a field is a global field:
            if field_name in global_fields list, then true
            also sets the field alias for global fields'''
        def calc_global_field_row(row, global_field_list):
            if row['field_name'] in global_field_list:
                return True
            return False

        def calc_global_field_alias(row, global_field_dict,global_field_list ):
            if row['field_name'] in global_field_list:
                return global_field_dict[row['field_name']]
            return ""

        def calc_global_field_def(row, global_def_dict, global_field_list):
            row_global_field_def = None
            if ((row['field_alias'] != '') and (row['field_name'] in global_field_list)):
                #below catches weird casing issues...
                try:
                    row_global_field_def = global_def_dict[row['field_alias']]
                except Exception, e:
                    try:
                        row_global_field_def = global_def_dict[inflection.titleize(row['field_alias'])]
                    except Exception, e:
                        print "***something went wrong!***"
                        print row['field_alias']
                        print str(e)
            return row_global_field_def

        df_global_defs = dfs_dict['global_field_defs']
        df_global_defs_list_names = list(df_global_defs['global_field_name'])
        df_global = dfs_dict['global_fields']
        df_global = df_global[df_global['field_name'].isin(df_global_defs_list_names)].reset_index()
        global_field_dict = PandasUtils.makeLookupDictOnTwo(df_global, 'global_string', 'field_name')
        global_def_dict = PandasUtils.makeLookupDictOnTwo(df_global_defs, 'global_field_name', 'global_field_definition')
        global_field_list = list(set( list( df_global['global_string'])))
        master_df['global_field'] = master_df.apply(lambda row: calc_global_field_row(row, global_field_list),axis=1)
        master_df['field_alias'] = master_df.apply(lambda row: calc_global_field_alias(row, global_field_dict, global_field_list),axis=1)
        master_df['global_field_definition'] =  master_df.apply(lambda row: calc_global_field_def(row, global_def_dict, global_field_list ),axis=1)
        return master_df

  

    @staticmethod
    def calc_field_documented(master_df):
        '''calculates if a field has been documented or not:
           a field is documented if its a global field, or has a data_dictionary_attachment, or has an existing field_definition '''
        def calc_field_documented_row(row):
            if row['global_field']:
                return True
            elif row['field_definition'] != '':
                return True
            return False
    
        master_df['field_documented'] = master_df.apply(lambda row: calc_field_documented_row(row),axis=1)
        return master_df

    @staticmethod
    def calc_do_not_process(master_df,dfs_dict):
        '''sets the do not process flag; criteria for this flag is as follows:
           -is a global field OR
           - has a data_dictionary attached
           - or the department or steward is in the do not process list'''
        def calc_do_not_process_row(row, depts_exclude, stewards_exclude):
            if row['data_steward'] in stewards_exclude:
                return True
            elif row['department'] in depts_exclude:
                return True
            elif row['data_dictionary_attached']:
                return True
            elif row['global_field']:
                return True
            return False
        df_data_dictionary_do_not_process = dfs_dict['data_dictionary_do_not_process']
        depts_exclude =  list(df_data_dictionary_do_not_process['departments'])
        stewards_exclude =list(df_data_dictionary_do_not_process['stewards'])
        master_df['do_not_process'] =  master_df.apply(lambda row: calc_do_not_process_row(row, depts_exclude, stewards_exclude),axis=1)
        return master_df

    @staticmethod
    def add_calculated_fields(master_df, base_url, dfs_dict):
        #fields_calculate= ['department', 'field_documented', 'global_field', 'do_not_process', 'field_definition', 'open_data_portal_url']
        master_df = MasterDataDictionary.add_url(master_df, base_url)
        master_df = MasterDataDictionary.add_default_privateordeleted(master_df)
        master_df = MasterDataDictionary.set_blank_data_dictionary_attachment_flag(master_df)
        master_df =  MasterDataDictionary.calc_department_field(master_df,dfs_dict)
        master_df = MasterDataDictionary.calc_global_field(master_df,dfs_dict)
        master_df =  MasterDataDictionary.calc_field_documented(master_df)
        master_df =  MasterDataDictionary.calc_do_not_process(master_df, dfs_dict)       
        return master_df

    @staticmethod
    def export_master_df_as_rows(master_df):
        '''exports the master dd as a dict list; removes field definitions that are blank from the row'''
        master_dd_rows = PandasUtils.convertDfToDictrows(master_df)
        master_dd_rows = [ DictUtils.remove_blanks_from_dict_on_key(row, 'field_definition') for row in master_dd_rows]
        return master_dd_rows

    @staticmethod
    def set_dataset_info(configItems, socrataLoadUtilsObject, master_df_rows):
        dataset = configItems['master_data_dictionary']
        dataset = socrataLoadUtilsObject.setDatasetDicts(dataset)
        dataset[ socrataLoadUtilsObject.src_records_cnt_field] = len(master_df_rows)
        return dataset

if __name__ == "__main__":
    main()

