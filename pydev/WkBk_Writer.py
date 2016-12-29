# coding: utf-8
import pandas as pd
from openpyxl import Workbook
import datetime


class WkBkWriter:
    '''class to format and write excel workbooks'''


    def __init__(self, configItems, logger, field_types=None):
        self.wkbk_output_dir = configItems['wkbk_output_dir']
        self.current_date = datetime.datetime.now().strftime("%Y_%m_%d")
        self._fieldTypes = field_types
        self._fieldType_Flag = configItems['fieldType_flag']
        self.logger = logger


    @property
    def fieldTypes(self):
        return self._fieldTypes

    @staticmethod
    def format_worksheet_cols(worksheet, format_num, format_font):
        '''formats the width of sheet columns and sets the font'''
        worksheet.set_column('A:A', None, format_num)
        worksheet.set_column('F:F', 50, format_num)
        worksheet.set_column('D:D', 35, format_font)
        worksheet.set_column('B:B', 30, format_font)
        worksheet.set_column('C:C', 25, format_font)
        worksheet.set_column('B:B', 30, format_font)
        worksheet.set_column('E:E', 25, format_font)
        worksheet.set_column('G:G', 30, format_font)
        worksheet.set_column('H:H', 30, format_font)
        return worksheet


    @staticmethod
    def set_workbook_format(writer):
        '''sets workbook number and font formats'''
        workbook = writer.book
        format_num = workbook.add_format({'num_format': '0000000000'})
        format_font = workbook.add_format({'font_name': 'Arial','font_size': '11' })
        return format_num, format_font


    def add_sht_validations(self, worksheet, sheet_len):
        '''sets validations on fieldtype and category columns'''
        if self._fieldTypes:
            worksheet.data_validation('E2:E'+str(sheet_len), {'validate': 'list',
                                  'source': self._fieldTypes })
        worksheet.data_validation('H2:H'+str(sheet_len), {'validate': 'list',
                                 'source': self._fieldType_Flag })
        return worksheet

    def format_wkbk_shts(self, sheets, writer):
        '''formats wkbk sheets'''
        format_num, format_font = self.set_workbook_format(writer)
        for sheet in sheets:
            sheet_len = len(sheet["sheet_df"])
            worksheet = writer.sheets[sheet['datasetid']]
            worksheet.set_zoom(90)
            #format = workbook.add_format()
            worksheet = self.format_worksheet_cols(worksheet, format_num, format_font)
            worksheet = self.add_sht_validations(worksheet, sheet_len)
        return writer

    @staticmethod
    def parse_steward_info(steward_info):
        steward_info_dict = {}
        stwd_info_list = steward_info['data_steward_name'].split(" ")
        steward_info_dict["First Name"] = "_".join(stwd_info_list[0:-1])
        steward_info_dict["Last Name"] = stwd_info_list[-1]
        return steward_info_dict

    def wkbk_name(self, steward_info):
        steward_info  = self.parse_steward_info(steward_info)
        wkbk_name = "data_dictionary_" + steward_info["First Name"] + "_" + steward_info["Last Name"] + "_" + self.current_date+ ".xlsx"
        wkbk_fullpath = self.wkbk_output_dir + wkbk_name
        return wkbk_fullpath

    def write_wkbk(self, sheets, steward_info):
        '''writes workbook with formatting'''
        wkbk_fullpath = self.wkbk_name(steward_info)
        writer = pd.ExcelWriter( wkbk_fullpath,  engine='xlsxwriter' )
        for sheet in sheets:
            sheet["sheet_df"].to_excel( writer, index=False,  sheet_name=sheet['datasetid'])
        writer  = self.format_wkbk_shts(sheets, writer)
        writer.save()
        return wkbk_fullpath, self.current_date




if __name__ == "__main__":
    main()
