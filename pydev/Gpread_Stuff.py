# coding: utf-8
#https://github.com/burnash/gspread

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle
import re

class gSpread_Stuff:
    ''' class to deal with google spreadsheet stuff '''
    def __init__(self,   configItems):
        self.client_key = configItems["client_key"]
        self.gscope = ['https://spreadsheets.google.com/feeds']
        self.gc = self.get_goog_client()
        self.picked_dir = configItems["pickle_dir"]

    def get_goog_client(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.client_key, self.gscope)
        gc = gspread.authorize(credentials)
        return gc

    def get_wkbk(self, wkbook_key):
        wkbk = self.gc.open_by_key(wkbook_key)
        return wkbk

    @staticmethod
    def get_all_shts(wkbk):
        return wkbk.worksheets()

    @staticmethod
    def get_sht(wkbk, sht_name):
        return wkbk.worksheet(sht_name)

    @staticmethod
    def get_all_cells(sht):
        return sht.get_all_records()

    @staticmethod
    def findCell(sht, regex):
        regex = re.compile(r'%s' % regex)
        cell = sht.find(regex)
        return cell

    @staticmethod
    def findRow(sht, regex):
        regex = re.compile(r'%s' % regex)
        cell = sht.find(regex)
        return cell.row

    @staticmethod
    def findCells(sht, regex_str):
        # Find all cells with regexp
        cell_list = sht.findall(regex_str)
        return cell_list

    @staticmethod
    def findRows(sht, regex_str):
        '''returns a list a row numbers that match a regex'''
        cell_list = sht.findall(regex_str)
        rows = [ cell.row for cell in cell_list ]
        return rows

    @staticmethod
    def findCellList(sht, regex_str):
         return sht.findall(regex_str)

    @staticmethod
    def batchUpdateCellRange(sht, cell_range, valToSet, valsToNotOverride=[], cellRowsToNotOverride=[]):
        do_not_override = []
        # Select a range
        cell_list = sht.range(cell_range)
        #set the cell value
        counter = 0
        #print cell_list
        for cell in cell_list:
            #print cell.value
            if (cell.value not in valsToNotOverride) or (cell.row not in cellRowsToNotOverride):
                cell.value = valToSet
                counter = counter +1
            else:
                #print cell.value
                #remove the cell from the update list
                #print counter
                cell_list.pop(counter)
                do_not_override.append(cell.row)
        # Update in batch
        sht.update_cells(cell_list)
        print do_not_override
        return  do_not_override

    def batchUpdateCellRanges( self, sht, cell_ranges, valToSet, valsToNotOverride=[], cellRowsToNotOverride=[]):
        updt_log = {}
        all_cellrows_do_not_override = []
        for cell_range in cell_ranges:
            try:
                do_not_override   =  self.batchUpdateCellRange(sht, cell_range, valToSet, valsToNotOverride, cellRowsToNotOverride)
                #print do_not_override
                updt_log[cell_range] = True
            except Exception, e:
                print "Batch errorCell ranges Error"
                print str(e)
                updt_log[cell_range] = False
            all_cellrows_do_not_override = all_cellrows_do_not_override + do_not_override
        print all_cellrows_do_not_override
        return updt_log, all_cellrows_do_not_override

    @staticmethod
    def update_cell_addr(sht, row, col, cell_val ):
        updated = False
        try:
            sht.update_cell(row, col, cell_val)
            updated = True
        except Exception, e:
            print str(e)
        return updated

    @staticmethod
    def update_cell_addr_str(sht, cell_addr_str, cell_val):
        return sht.update_acell(cell_addr_str, cell_val)


    def update_many_cells_by_addr_str(self, sht, cell_addr_list, cell_val):
        '''updates a list of cells with the same value'''
        for cell_addr_str in cell_addr_list:
            try:
                self.update_cell_addr_str(sht, cell_addr_str, cell_val)
            except Exception, e:
                print "*********"
                print str(e)
                print cell_addr_str
                print cell_val
                print "*********"
        return True

        
        
    @staticmethod
    def getCellRange(column,rows):
        rows =  sorted(rows)
        cell_range = ""
        cell_range = column + str( rows[0] ) + ":" + column + str(rows[-1])
        return cell_range

    @staticmethod
    def generateCellLocations( allRows, column):
        return [ column+str(row) for row in allRows]


    def getCellRows(self, sht, listOfDataToFind):
        all_rows = []
        for dataToFind in listOfDataToFind:
            rows = self.findRows(sht, dataToFind)
            all_rows = all_rows + rows
        return all_rows

    def getCellRanges(self, allRows, column):
        cell_ranges = []
        for rows in allRows:
            if len(rows) > 1:
                cell_range = self.getCellRange(column, rows )
                cell_ranges.append(cell_range)
        return cell_ranges


    def pickle_cells(self, cells, pickle_name ):
        pickle.dump( cells, open( self.picked_dir + pickle_name + "_pickled_cells.p", "wb" ) )

    def unpickle_cells(self, pickle_name):
        return pickle.load( open( self.picked_dir + pickle_name +"_pickled_cells.p", "rb" ) )

    def getMetaDataset(self, wkbk_key, sht_name, pickle_name):
        wkbk = self.get_wkbk(wkbk_key)
        sht = self.get_sht(wkbk, sht_name)
        cells = self.get_all_cells(sht)
        self.pickle_cells(cells, pickle_name)



if __name__ == "__main__":
    main()
