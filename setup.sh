#!/bin/bash

#sets up initial directory structure for repo.
mkdir logs
mkdir output_wkbks
mkdir wkbk_uploads
mkdir pickled
mkdir documented_fields
mkdir documented_fields/xlsx
mkdir documented_fields/pdf
mkdir documented_fields/other

#installs dependencies
pip install --upgrade oauth2client
pip install gspread
pip install openpyxl
pip install pycurl
pip install xlrd
pip install xlsxwriter
pip install inflection
