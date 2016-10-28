#!/bin/bash

#sets up initial directory structure for repo.
mkdir logs
mkdir output_wkbks
mkdir wkbk_upoads
mkdir pickled

#installs dependencies
pip install --upgrade oauth2client
pip install gspread
pip install openpyxl
pip install pycurl
pip install xlrd
pip install xlsxwriter