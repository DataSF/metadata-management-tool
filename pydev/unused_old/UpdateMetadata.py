# coding: utf-8

from datetime import datetime, timedelta

from Utils import *
from Wkbk_Json import *

class UpdateMetadata(object):
    '''Class to Update Metadata google spreadsheet '''

    def __init__(self, configItems, gSpread_Stuff):
        self._gSpread_Stuff = gSpread_Stuff
        self._updt_sht_name = configItems['update_info']['updt_sht_name']
        self._updt_wkbk_key = configItems['update_info']['wkbk_key']
        self._wkbk_uploads_dir = configItems['wkbk_uploads_dir']
        self._json_wksht_status_update_fname = configItems['json_wksht_status_update_fname']
        try:
            self._updt_wkbk = gSpread_Stuff.get_wkbk(self._updt_wkbk_key)
        except Exception, e:
            print str(e)
        try:
            self._updt_sht =   gSpread_Stuff.get_sht( self._updt_wkbk, self._updt_sht_name)
        except Exception, e:
            print str(e)
        self._field_positions = configItems['update_info']['field_positions']
        #self._current_date = datetime.datetime.now() - timedelta(days=10)
        #self._current_date = self._current_date.strftime("%m/%d/%Y")
        self._current_date = DateUtils.get_current_date_month_day_year()

        self._updt_statuses = configItems['update_info']['statuses']
        self._valsToNotOverride = configItems['valsToNotOverride']

    @staticmethod
    def getDatasetsList(wkbk):
        return [elem['Dataset Name'].strip() for elem in wkbk['datasets'] if 'Dataset Name' in elem]

    def get_overrride_cells(self):
        excludeRowsEmpty = []
        #"****vals do not override ***"
        excludeRows = self._gSpread_Stuff.getCellRows(self._updt_sht, self._valsToNotOverride)
        print len(excludeRows)
        #if len( excludeRows > 1 ):
        #     excludeRows = excludeRows.sort()
        #    return excludeRows
        return excludeRows

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
        #get a list where the status in the list
        doNotOverrideList = self.get_overrride_cells()
        print "****vals do not override ***"
        #doNotOverrideList = [4657, 6799, 7185, 4653, 4654, 4656, 4651, 4652, 4655, 7186]
        for wkbk in wkbks['workbooks']:
            print wkbk
            try:
                datasetsList =  self.getDatasetsList(wkbk)
                print "*****"
                print datasetsList
                #get the cell ranges
                datasets_rows = self._gSpread_Stuff.getCellRows(self._updt_sht, datasetsList)
                datasets_rows = list(set(datasets_rows).difference(set(doNotOverrideList)))

                if len(datasets_rows) > 1:
                    datasets_rows = [str(row) for row in datasets_rows ]
                    print datasets_rows
                #cell_ranges_dt_changed = self._gSpread_Stuff.generateCellLocations(all_rows, self._field_positions['date_last_changed'])
                #print cell_ranges_dt_changed

                #cell_ranges_status =  self._gSpread_Stuff.generateCellLocations(all_rows, self._field_positions['status'])
                    print "updating status"
                    updt_status = self._gSpread_Stuff.update_many_cells_by_addr(self._updt_sht, datasets_rows, "8",  self._updt_statuses['for_review_steward'])
                    updt_status
                    print "updating date"
                    updt_dt_changed = self._gSpread_Stuff.update_many_cells_by_addr(self._updt_sht,  datasets_rows, "10", self._current_date)
                    updt_dt_changed
                    #wkbk_cells_updted_dict[wkbk["data_cordinator"]["Email"]] = True
                #sht, rows, col, cell_val
                #updt_status = self._gSpread_Stuff.update_many_cells_by_addr_str(self._updt_sht, cell_ranges_status, self._updt_statuses['for_review_steward'])
                #updt_dt_changed = self._gSpread_Stuff.update_many_cells_by_addr_str(self._updt_sht, cell_ranges_dt_changed, self._current_date)

                #check to make sure that stuff actually updated correctly
                #wkbk_cells_updted_dict[wkbk["data_cordinator"]["Email"]] = True
            except Exception, e:
                print str(e)
                #wkbk_cells_updted_dict[wkbk["data_cordinator"]["Email"]] = False
            #write the results to json file
            wkbk_cells_updted_dict[wkbk["data_cordinator"]["Email"]] = True
        wroteFile = WkbkJson.write_json_object({"updated":wkbk_cells_updted_dict}, self._wkbk_uploads_dir, self._json_wksht_status_update_fname )
        return wroteFile


class UpdateMetadataFields(UpdateMetadata):
    '''class updates google spreadsheet after generating wkbks'''
    def __init__(self, configItems, gSpread_Stuff):
        UpdateMetadata.__init__(self, configItems, gSpread_Stuff)
        print self._updt_sht

    def findUpdtRow( self, field_dict):
        row_num = self._gSpread_Stuff.findRow(self._updt_sht, str(field_dict['1']))
        return row_num


    @staticmethod
    def build_fieldUpdate_dict(row_num, field_dict):
        '''builds up up the string to pass to the updt fxn'''
        return { k+str(row_num):v for k,v in field_dict.iteritems() if k != "A"}

    #these 2 methods update the cells using the numeric row, col location - ie 1,2, some val to updt cell
    def update_fieldDict_cells_addr(self, row_num, field_dict ):
        sucessupdt = []
        print field_dict
        for k,v in field_dict.iteritems():
            if k != "1":
                print str(k) + ":" + str(v)
                sucessupdt.append(self._gSpread_Stuff.update_cell_addr(self._updt_sht, row_num, int(k), v ) )
        return sucessupdt

    def update_fieldList_numeric(self, fieldList):
        for field_dict in fieldList:
            print "finding row"
            row_num = self.findUpdtRow( field_dict)
            print "updating row"
            #field_updt_dict = self.build_fieldUpdate_dict(row, field_dict)
            updted = self.update_fieldDict_cells_addr( row_num, field_dict )
        return True

    #these 2 methods update the cells using the alpha location - ie  B2, some val to updt cell
    def update_fieldDict_cells_addr_str(self, row_num, field_dict ):
        sucessupdt = []
        print field_dict
        for k,v in field_dict.iteritems():
            if k != "A":
                print str(k) + ":" + str(v)
                sucessupdt.append(self._gSpread_Stuff.update_cell_addr(self._updt_sht, row_num, int(k), v ) )
        return sucessupdt

    def update_fieldList_alpha(self, fieldList):
        rows_to_exclude = self.get_overrride_cells()
        print rows_to_exclude
        for field_dict in fieldList:
            print "finding row"
            row_num = self.findUpdtRow(field_dict)
            print row_num
            if row_num not in rows_to_exclude:
                print "updating row"
                field_updt_dict = self.build_fieldUpdate_dict(row_num, field_dict)
                updted = self.update_fieldDict_cells_addr( row_num, field_dict )
            else:
                print row_num
        return True


if __name__ == "__main__":
    main()
