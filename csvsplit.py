#!/usr/bin/env python
"""
csvsplit
--------


This program is a tool for performing SQL against a CSV file.  The backend engine is sqlite3, so most SQL options for WHERE, GROUP BY, and ORDER BY for sqlite3 will work.

SYNOPSIS:

process csv file:

  python csvsplit.py --help

  python csvsplit.py --split='note'  < ~/Downloads/WRMS-Data.csv  1>/tmp/tmp.csv



Options:


--split
split the row on '|' delimiter for this field

Copyright (C) Piers Harding 2012 and beyond, All rights reserved

csvsplit.py is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

"""



import sys, os
import re
from optparse import OptionParser, SUPPRESS_HELP
import logging
from datetime import datetime
import time
import csv



class csvdata(object):
    """
    Simple data object for parsed csv data
    """

    def __init__(self, header, data):
        self.header = header
        self.data = data


class csvfile(object):
    """
    Base class used to trigger everything off
    """

    @classmethod
    def read(cls, f):
        csv_file = []

        # preprocess the file to remove blank lines and comments
        lines = []
        for line in f.readlines():
            # eliminate blank lines
            if re.match('^$', line):
                continue
            # eliminate comment lines
            if re.match('^#', line):
                continue
            lines.append(line.strip())
    
        # setup the csv processor
        csvReader = csv.reader(lines, delimiter=',', quotechar='"')
    
        # get header row
        fields = csvReader.next() 
    
        # process each csv record into a hash
        for row in csvReader:
            items = zip(fields, row)
            item = {}
            for (name, value) in items:
                # data that gets inserted in SQLite must be utf-8 encoded
                item[name] = value.strip().decode('utf-8', 'ignore')
            csv_file.append(item)
            #print(item)
        
        return csvdata(fields, csv_file)


# CSV file output handler
def output_csv_file(fh, data):
    writer = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    writer.writerows(data)


# main of application
def main():

    # setup logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

    parser = OptionParser()
    parser.add_option("-d", "--head", dest="head", default=False, action="store_true",
                          help="list column headings and a sample rows", metavar="HEAD")
    parser.add_option("-f", "--file", dest="csv_file", default=None, type="string",
                          help="A CSV file for input", metavar="CSV_FILE")
    parser.add_option("-s", "--split", dest="split", default=None, type="string",
                          help="What column to to split on eg: --split='note'", metavar="SPLIT")
    (options, args) = parser.parse_args()

    # load the csv file
    fh = None
    if options.csv_file == None:
        options.csv_file = 'stdin'
        fh = sys.stdin
    else:
        if not os.path.isfile(options.csv_file):
            logging.error("CSV file not found: " + str(options.csv_file))
            sys.exit(1)
        fh = open(options.csv_file, 'rb')

    logging.info("CSV file to process: " + str(options.csv_file))
    logging.info("options are: " + str(options))


    # set the encoding to stop errors on the input/putput streams
    reload(sys)
    sys.setdefaultencoding("utf-8")

    # import the file
    r = csvfile.read(fh)
    if not r.data or len(r.data) == 0:
        logging.info('CSV file is empty')
        sys.exit(1)

    # load the table
    out = []
    for row in r.data:
        # process split
        if options.split in row:
            vals = row[options.split].split('|')
            for val in vals:
                row[options.split] = val
                out.append([row[h] for h in r.header])
        else:
            out.append([row[h] for h in r.header])
                

    out.insert(0, r.header)
    logging.info("header is: " + str(r.header))
    #logging.info("data is: " + str(out))
    output_csv_file(sys.stdout, out)
    sys.exit(0)


# ------ Good Ol' main ------
if __name__ == "__main__":
    main()

