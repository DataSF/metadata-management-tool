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
#import pycurl
from io import BytesIO
import pandas as pd
import requests
import shutil

class pyLogger:
    def __init__(self, configItems):
        self.logfn = configItems['exception_logfile']
        self.log_dir = configItems['log_dir']
        self.logfile_fullpath = self.log_dir+self.logfn

    def setConfig(self):
        #open a file to clear log
        fo = open(self.logfile_fullpath, "w")
        fo.close
        logging.basicConfig(level=logging.DEBUG, filename=self.logfile_fullpath, format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger=logging.getLogger(__name__)


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


class myUtils:

    @staticmethod
    def removeKeys( mydict, keysToRemove):
        for key in keysToRemove:
            try:
                remove_columns = mydict.pop(key, None)
            except:
                noKey = True
        return dataset

    @staticmethod
    def filterDictList( dictList, keysToKeep):
        return  [ {key: x[key] for key in keysToKeep if key in x.keys() } for x in dictList]

    @staticmethod
    def filterDict(mydict, keysToKeep):
        mydictKeys = mydict.keys()
        return {key: mydict[key] for key in keysToKeep  if key in mydictKeys }
    @staticmethod
    def filterDictListOnKeyVal(dictlist, key, valuelist):
        #filter list of dictionaries with matching values for a given key
        return [dictio for dictio in dictlist if dictio[key] in valuelist]

    @staticmethod
    def filterDictListOnKeyValExclude(dictlist, key, excludelist):
        #filter list of dictionaries that aren't in an excludeList for a given key
        return [dictio for dictio in dictlist if dictio[key] not in excludelist]
    @staticmethod
    def filterDictOnVals(some_dict, value_to_exclude):
        return {k: v for k, v in some_dict.items() if v != value_to_exclude}

    @staticmethod
    def is_nan(x):
        return isinstance(x, float) and math.isnan(x)
    @staticmethod
    def is_blank(x):
        blank = False
        if( (x == "") or (x == " ") or (x is None)):
            blank = True
        return blank

    @staticmethod
    def filterDictOnNans(some_dict):
        '''excludes all k,v in a dict with v = NaN'''
        return {k: v for k, v in some_dict.items() if not(myUtils.is_nan(v))}

    @staticmethod
    def filterDictOnBlanks(some_dict):
        return {k: v for k, v in some_dict.items() if not(myUtils.is_blank(v))}

    @staticmethod
    def setConfigs(config_dir, config_file):
        '''returns contents of yaml config file'''
        with open( config_dir + config_file ,  'r') as stream:
            try:
                config_items = yaml.load(stream)
                return config_items
            except yaml.YAMLError as exc:
                print(exc)
        return 0

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


class ShtUtils:

    @staticmethod
    def getWkbk(fn):
        wkbk = pd.ExcelFile(fn)
        return wkbk

    @staticmethod
    def get_sht_names(wkbk):
        shts =  wkbk.sheet_names
        return [ sht for sht in shts if sht != 'Dataset Summary']





if __name__ == "__main__":
    main()
