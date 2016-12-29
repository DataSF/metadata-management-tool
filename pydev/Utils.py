# coding: utf-8
import csv
import time
import datetime
import logging
from retry import retry
import yaml
import os
import itertools
import base64
import inflection
import csv, codecs, cStringIO
import glob
import math
import pycurl
from io import BytesIO
import pandas as pd
import requests
import shutil
from csv import DictWriter
from cStringIO import StringIO


class PickleUtils:
    @staticmethod
    def pickle_cells(cells, pickle_name ):
        pickle.dump( cells, open(picked_dir + pickle_name + "_pickled_cells.p", "wb" ) )

    @staticmethod
    def unpickle_cells(pickle_name):
        return pickle.load( open(picked_dir + pickle_name +"_pickled_cells.p", "rb" ) )


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


    @staticmethod
    def getFileListForDir(filepath_str_to_search):
        '''gets file list in a directory based on some path string to search- ie: /home/adam/*.txt'''
        return glob.glob(filepath_str_to_search)


    @staticmethod
    def flatten_list(listofLists):
        return [item for sublist in listofLists for item in sublist]

    @staticmethod
    def getAttachmentFullPath(output_dir, output_fn, download_url):
        '''downloads an attachment from whereever'''
        #equivelent to: curl -L "https://screendoor.dobt.co/attachments/s5wflD750Nxhai9MfNmxes4TR-0xoDyw/download" > whateverFilename.csv
        # As long as the file is opened in binary mode, can write response body to it without decoding.
        downloaded = False
        try:
            with open(output_dir + output_fn, 'wb') as f:
                c = pycurl.Curl()
                c.setopt(c.URL, download_url)
                # Follow redirect.
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
                downloaded = True
        except Exception, e:
            print str(e)
        return downloaded

    @staticmethod
    def getFiles(output_dir, output_fn, download_url ):
        dowloaded = False
        r = requests.get(download_url, stream=True)
        with open(output_dir+output_fn, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
            downloaded = True
        return downloaded


    @staticmethod
    def write_wkbk_csv(fn, dictList, headerCols):
        wrote_wkbk = False
        with open(fn, 'w') as csvfile:
            try:
                fieldnames = headerCols
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for data in dictList:
                    try:
                        writer.writerow({ s:str(v).encode("ascii",  'ignore') for s, v in data.iteritems()  } )
                    except:
                        print "could not write row"
                wrote_wkbk = True
            except Exception, e:
                print str(e)
        return wrote_wkbk


class ShtUtils:
    '''class for common wksht util functions'''

    @staticmethod
    def getWkbk(fn):
        wkbk = pd.ExcelFile(fn)
        return wkbk

    @staticmethod
    def get_sht_names(wkbk):
        shts =  wkbk.sheet_names
        return [ sht for sht in shts if sht != 'Dataset Summary']

class PandasUtils:
    '''class for common pandas utility functions'''

    @staticmethod
    def renameCols(df, colMappingDict):
        df = df.rename(columns=colMappingDict)
        return df



if __name__ == "__main__":
    main()
