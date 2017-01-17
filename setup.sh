#!/bin/bash

#sets up initial directory structure for repo.
mkdir logs
mkdir output_wkbks
mkdir wkbk_uploads
mkdir pickled
mkdir pickled/datasets
mkdir documented_fields
mkdir documented_fields/xlsx
mkdir documented_fields/pdf
mkdir documented_fields/other
mkdir documented_fields/output

#installs dependencies
#pip install --upgrade oauth2client
#pip install gspread
pip install openpyxl
pip install pycurl
pip install xlrd
pip install xlsxwriter
pip install inflection
