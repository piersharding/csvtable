#!/usr/bin/env python
"""
csvtable
--------


This program is a tool for performing SQL against a CSV file.  The backend engine is sqlite3, so most SQL options for WHERE, GROUP BY, and ORDER BY for sqlite3 will work.

SYNOPSIS:

process csv file:

  python csvtable.py --help

  python csvtable.py --convert='date_epoch:date,hours:int' --list="*, sum(hours) AS hours_sum" --group='organisation_code, system_code, request_id'  < ~/Downloads/WRMS-Data.csv  1>/tmp/tmp.csv



Options:

--list
essentially what you can put in the SELECT part of an SQL statement eg: --list='*, sum(widgetvalue)'

defaults to ordered list of fields in the CSV file

--convert
field conversion of CSV fields as the file is imported eg: --convert='date_epoch:date,hours:int'
currently handles date, float, and int
date field must be in '9999-99-99', and date is converted to epoch seconds

--where
essentially what you can put in the WHERE part of an SQL statement eg: --where="system == 'XYZ""

--sort
essentially what you can put in the ORDER BY part of an SQL statement eg: --sort='system,date'

--group
essentially what you can put in the GROUP BY part of an SQL statement eg: --sort='system,date'
This is used automatically for the ORDER BY as well.  It doesn't make a great deal of sense unless you use sum(), and count() etc. in the --list option too.


Copyright (C) Piers Harding 2012 and beyond, All rights reserved

csvtable.py is free software; you can redistribute it and/or
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
import sqlite3

DB_FILE = './table_tmp.db'

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
                item[name] = value.strip().decode('utf-8', 'ignore')
            csv_file.append(item)
            #print(item)
        
        return csvdata(fields, csv_file)


# try and strip weird unicode stuff before output
def out(val):
    if type(val) == float or type(val) == int:
        return str(val)
    try:
        return val.decode('utf-8', 'ignore')
    except UnicodeEncodeError:
        return ""


def main():

    # setup logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

    parser = OptionParser()
    parser.add_option("-f", "--file", dest="csv_file", default=None, type="string",
                          help="A CSV file for input", metavar="CSV_FILE")
    parser.add_option("-c", "--convert", dest="convert", default=None, type="string",
                          help="What columns to to convert to what eg: --convert='date_epoch:date,hours:int'", metavar="CONVERT")
    parser.add_option("-w", "--where", dest="where", default=None, type="string",
                          help="What WHERE clause to filter the CSV FIle by", metavar="WHERE")
    parser.add_option("-s", "--sort", dest="sort", default=None, type="string",
                          help="What columns to sort the CSV FIle by", metavar="SORT")
    parser.add_option("-g", "--group", dest="group", default=None, type="string",
                          help="What columns to group by", metavar="GROUP")
    parser.add_option("-l", "--list", dest="list", default=None, type="string",
                          help="alternate field list", metavar="LIST")
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

    # what conversions do we do
    conversions = {}
    if options.convert:
        conversions = [c.strip().split(":")[0] for c in options.convert.split(",")]
        conversions = dict(zip(conversions, [c.strip().split(":")[1] for c in options.convert.split(",")]))
    logging.info("conversions are: " + str(conversions))


    # import the file
    r = csvfile.read(fh)
    if not r.data or len(r.data) == 0:
        logging.info('CSV file is empty')
        sys.exit(1)

    # build up the row schema from the header
    hdrs = [h + ' TEXT' for h in r.header]
    isrt = ['?' for h in r.header]

    # build a temporary sqlite DB for queries
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS temptable")
    cur.execute("CREATE TABLE temptable(" + ", ".join(hdrs) + ")")

    # load the table
    for row in r.data:
        # process conversions
        for cvsn, action in conversions.items():
            if action == 'date':
                row[cvsn] = time.mktime(datetime.strptime(row[cvsn], '%Y-%m-%d').timetuple())
            elif action == 'float':
                if len(row[cvsn]) > 0 and re.match('^[\s\d\.]+$', row[cvsn]):
                    row[cvsn] = float(row[cvsn])
            elif action == 'int':
                if len(row[cvsn]) > 0 and re.match('^[\s\d\.]+$', row[cvsn]):
                    row[cvsn] = int(float(row[cvsn]))
                
        vals = [row[h] for h in r.header]
        #print vals
        cur.execute("INSERT INTO temptable VALUES(" + ", ".join(isrt) + ")", vals)

    con.commit()

    # select and sort
    cur = con.cursor()
    groupby = ''
    orderby = ''
    where = ''
    if options.where:
        where = " WHERE " + options.where

    if options.group:
        orderby = " ORDER BY " + options.group
        groupby = " GROUP BY " + options.group
    elif options.sort:
        orderby = " ORDER BY " + options.sort
        
    # what fields to select including summaries
    flds = ", ".join(r.header)
    if options.list:
        flds = options.list
    sql = 'SELECT ' +flds + ' FROM temptable ' + where + groupby + orderby
    logging.info("SQL is: " + str(sql))
    cur.execute(sql)
    col_names = [cn[0] for cn in cur.description]
    rows = cur.fetchall()

    # output as CSV again
    logging.info("column names out: " + str(col_names))
    print ",".join(col_names)
    for row in rows:
        print ",".join(['"' + str(out(i)) + '"' for i in row])

    os.unlink(DB_FILE)
    sys.exit(0)


# ------ Good Ol' main ------
if __name__ == "__main__":
    main()
