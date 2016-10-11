# coding: utf-8
import datetime
from Utils import *
from Wkbk_Json import *

class UpdateMetadata(object):
    '''Class to Update Metadata google spreadsheet '''
    
    def __init__(self, configItems, gSpread_Stuff):
        self._gSpread_Stuff = gSpread_Stuff
        self._updt_sht_name = configItems['update_info']['updt_sht_name']
        self._updt_wkbk_key = configItems['update_info']['wkbk_key']
        self._updt_wkbk = gSpread_Stuff.get_wkbk(self._updt_wkbk_key)
        self._updt_sht =   self._gSpread_Stuff.get_sht( self._updt_wkbk, self._updt_sht_name)
        self._field_positions = configItems['update_info']['field_positions']
        self._current_date = datetime.datetime.now().strftime("%m/%d/%Y")
        self._updt_statuses = configItems['update_info']['statuses']

    @staticmethod
    def getDatasetsList(wkbk):
        return [elem['Dataset Name'].strip() for elem in wkbk['datasets'] if 'Dataset Name' in elem]


class UpdateMetadataStatus(UpdateMetadata):
    '''class updates google spreadsheet after generating wkbks'''
    def __init__(self, configItems, gSpread_Stuff):
        UpdateMetadata.__init__(self, configItems, gSpread_Stuff)
        
    @staticmethod
    def checkUpdateStatus(cell_updt_status_dict):
        cells_updated = False
        if not (False in cell_updt_status_dict.values()):
            cells_updated = True
        return cells_updated
        
        
    def updatewkbk_info(self, wkbks):
        '''method that does the heavy lifting/updating-uses info in the json wkbk object'''
        wkbk_cells_updted_dict = {}
        for wkbk in wkbks:
            cells_updated = False
            try:
                datasetsList =  self.getDatasetsList(wkbk)
                all_rows = self._gSpread_Stuff.getCellRows(self._updt_sht, datasetsList)
                cell_ranges_dt_changed = self._gSpread_Stuff.getCellRanges(all_rows, self._field_positions['date_last_changed'])
                updt_dt_changed  = self._gSpread_Stuff.batchUpdateCellRanges( self._updt_sht , cell_ranges_dt_changed,  self._current_date )
                cell_ranges_status =  self._gSpread_Stuff.getCellRanges(all_rows, self._field_positions['status'])     
                updt_statuses  = self._gSpread_Stuff.batchUpdateCellRanges( self._updt_sht , cell_ranges_status,  self._updt_statuses['for_review_steward'] )
                if self.checkUpdateStatus(updt_statuses) and self.checkUpdateStatus(updt_dt_changed):
                    cells_updated = True
                wkbk_cells_updted_dict[wkbk["data_cordinator"]["Email"]] = cells_updated
            except Exception, e:
                print str(e)
        return self.checkUpdateStatus(wkbk_cells_updted_dict), wkbk_cells_updted_dict
    

class UpdateMetadataFields(UpdateMetadata):
    '''class updates google spreadsheet after generating wkbks'''
    def __init__(self, configItems, gSpread_Stuff):
        UpdateMetadata.__init__(self, configItems, gSpread_Stuff)
        
    
   
    

if __name__ == "__main__":
    main()