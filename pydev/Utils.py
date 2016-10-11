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
from ConfigUtils import *


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
    def setConfigs(config_dir, config_file):
        with open( config_dir + config_file ,  'r') as stream:
            try:
                config_items = yaml.load(stream)
                return config_items
            except yaml.YAMLError as exc:
                print(exc)
        return 0
    
if __name__ == "__main__":
    main()