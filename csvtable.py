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

--head
Shows what the column names are and the first 10 rows.


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

--delimiter
change the delimiter from ',' eg: --delimiter='|'

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
import nltk
import nltk.chunk
import itertools


#import nltk
#from nltk.collocations import *
#bigram_measures = nltk.collocations.BigramAssocMeasures()
#trigram_measures = nltk.collocations.TrigramAssocMeasures()
#
## change this to read in your data
#finder = BigramCollocationFinder.from_words(nltk.corpus.genesis.words('english-web.txt'))
#
## only bigrams that appear 3+ times
#finder.apply_freq_filter(3) 
#
## return the 10 n-grams with the highest PMI
#finder.nbest(bigram_measures.pmi, 10)

#import nltk.chunk
 
#def conll_tag_chunks(chunk_sents):
#    tag_sents = [nltk.chunk.tree2conlltags(tree) for tree in chunk_sents]
#    return [[(t, c) for (w, t, c) in chunk_tags] for chunk_tags in tag_sents]
#
##import nltk.corpus, nltk.tag
#print nltk.corpus.genesis.words('english-web.txt')
#
##train_chunks = conll_tag_chunks(nltk.corpus.genesis.words('english-web.txt'))
#train_chunks = nltk.corpus.brown.tagged_sents()
##train_chunks = conll_tag_chunks('/home/piers/nltk_data/corpora/genesis/english-web.txt')
#u_chunker = nltk.tag.UnigramTagger(train_chunks)
#ub_chunker = nltk.tag.BigramTagger(train_chunks, backoff=u_chunker)
##ubt_chunker = nltk.tag.TrigramTagger(train_chunks, backoff=ub_chunker)
##ut_chunker = nltk.tag.TrigramTagger(train_chunks, backoff=u_chunker)
##utb_chunker = nltk.tag.BigramTagger(train_chunks, backoff=ut_chunker)



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
        fields = [ i.strip() for i in csvReader.next() ]
    
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
def output_csv_file(fh, data, delimiter):
    writer = csv.writer(fh, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    writer.writerows(data)

# calculate the TABLE column types
def coltypes(c, conversions):
    t = 'TEXT'
    if c in conversions:
        if conversions[c] == 'int':
            t = 'INTEGER'
        elif conversions[c] == 'float':
            t = 'REAL'
        elif conversions[c] == 'date':
            t = 'INTEGER'
    return t

# determine the date conversion required
def date_conversion(val):
    if re.match('^\d{4}\-\d\d\-\d\d$', val):
        # 2012-12-31
        val = time.mktime(datetime.strptime(val, '%Y-%m-%d').timetuple())
    elif re.match('^\d\d?\/\d\d\/\d{4}$', val):
        # 31/12/2012
        val = time.mktime(datetime.strptime(val, '%d/%m/%Y').timetuple())
    elif re.match('^\d\d?\.\d\d\.\d{4}$', val):
        # 31.12.2012
        val = time.mktime(datetime.strptime(val, '%d.%m.%Y').timetuple())
    elif re.match('^\d{4}\d{2}\d{2}$', val):
        # 20121231
        val = time.mktime(datetime.strptime(val, '%Y%m%d').timetuple())
    elif re.match('^(\w+\,\s+)?\d+ \w+ \d{4}\s+\d\d?\:\d\d(:\d\d)?', val) or re.match('^\w+,?\s+?\d+ \d{2-4}\s+\d\d?\:\d\d(:\d\d)?', val):
        # Jan 24 2003 15:26:20 +0000
        # Mon, 20 Dec 04 08:37:31 GMT
        # Sat, 13 May 2006 06:15 +0000
        # Mon, 01 Jul 2002 18:38:25
        # Mon, 26 Sep 2005 7:35:00 -0800
        # Thu, 23 Apr 2009 13:32:15 +1200
        # 26 Aug 2009 02:07:34 +0400
        import rfc822
        import datetime as dt
        # [year, month, day, hour, min, sec]
        yyyy, mm, dd, hh, mins, ss = rfc822.parsedate(val)[:-3]
        val = time.mktime(dt.datetime(yyyy, mm, dd, hh, mins, ss).timetuple())
        try:
            val = float(str(val))
        except ValueError:
            pass
    return val

 
#class TagChunker(nltk.chunk.ChunkParserI):
#    def __init__(self, chunk_tagger):
#        self._chunk_tagger = chunk_tagger
# 
#    def parse(self, tokens):
#        # split words and part of speech tags
#        (words, tags) = zip(*tokens)
#        # get IOB chunk tags
#        chunks = self._chunk_tagger.tag(tags)
#        # join words with chunk tags
#        wtc = itertools.izip(words, chunks)
#        # w = word, t = part-of-speech tag, c = chunk tag
#        lines = [' '.join([w, t, c]) for (w, (t, c)) in wtc if c]
#        # create tree from conll formatted chunk lines
#        logging.info("lines for chunker: " + str(lines))
#        try:
#            return nltk.chunk.conllstr2tree('\n'.join(lines))
#        except ValueError:
#            return ''

# use nltk to find the nouns in text
def nltk_nounphrase(val):
    #val = re.sub('[\[\]\|\!\$\%\&\(\)\-\_\=\+\\\/]+', ' ', val)
    #tokens = nltk.word_tokenize(val)
    ##tagged = nltk.pos_tag(tokens)
    ## sentence should be a list of words
    grammar = "NP: {<DT>?<JJ.*>*<NN.*>+}"
    nouns = []
    cp = nltk.RegexpParser(grammar)
    #tagged = ub_chunker.tag(tokens)
    val = re.sub('[\[\]\|\!\$\%\&\(\)\-\_\=\+\\\/]+', ' ', val)
    tokens = nltk.word_tokenize(val)
    tagged = nltk.pos_tag(tokens)
    #logging.info("elements from tagger: " + str(tagged))
    tree = cp.parse(tagged)
    for subtree in tree.subtrees():
        #logging.info("node type: " + str(subtree.node))
        if subtree.node == 'NP' or subtree.node == 'CHUNK':
            words = [ w for (w, t) in subtree.leaves() ]
            nouns.append(" ".join(words))
    #tagchunker = TagChunker(ub_chunker)
    #tagged = tagger.tag(tokens)
    #tagged = ub_chunker.tag(tokens)
    #tree = False
    #try:
    #    tree = tagchunker.parse(tokens)
    #except ValueError:
    #    pass
    # for each noun phrase sub tree in the parse tree
    #if tree:
    #    for subtree in tree.subtrees(filter=lambda t: t.node == 'NP'):
    #        # print the noun phrase as a list of part-of-speech tagged words
    #        nouns.append(subtree.leaves())
    #for tag in tagged:
    #    item, typ = tag
    #    if typ == 'NNP' or typ == 'NN':
    #        nouns.append(item)
    #logging.info('nouns: ' + repr(nouns))
    return '|'.join(nouns)



# use nltk to find the nouns in text
def nltk_nouns(val):
    val = re.sub('[\[\]\|\!\$\%\&\(\)\-\_\=\+\\\/]+', ' ', val)
    tokens = nltk.word_tokenize(val)
    tagged = nltk.pos_tag(tokens)
    nouns = []
    for tag in tagged:
        item, typ = tag
        if typ == 'NNP' or typ == 'NN':
            nouns.append(item)
    #logging.info('nouns: ' + repr(nouns))
    return '|'.join(nouns)


# main of application
def main():

    # setup logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

    parser = OptionParser()
    parser.add_option("-d", "--head", dest="head", default=False, action="store_true",
                          help="list column headings and a sample rows", metavar="HEAD")
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
    parser.add_option("-b", "--delimiter", dest="delimiter", default=',', type="string",
                          help="alternate field delimiter", metavar="DELIMITER")
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

    # do we just list a few rows
    if options.head:
        for i, line in enumerate(fh):
            if i > 10:
                break
            sys.stdout.write(line)
        sys.exit(0)

    # what conversions do we do
    conversions = {}
    if options.convert:
        conversions = [c.strip().split(":")[0] for c in options.convert.split(",")]
        conversions = dict(zip(conversions, [c.strip().split(":")[1].lower() for c in options.convert.split(",")]))
    logging.info("conversions are: " + str(conversions))

    # set the encoding to stop errors on the input/putput streams
    reload(sys)
    sys.setdefaultencoding("utf-8")

    # import the file
    r = csvfile.read(fh)
    if not r.data or len(r.data) == 0:
        logging.info('CSV file is empty')
        sys.exit(1)

    # what should the table column types be
    cols = dict(zip(r.header, [coltypes(c, conversions) for c in r.header]))

    # build up the row schema from the header
    hdrs = ['"' + h + '" ' + cols[h] for h in r.header]
    isrt = ", ".join(['?' for h in r.header])

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
                row[cvsn] = date_conversion(row[cvsn])
            elif action == 'float':
                if len(row[cvsn]) > 0 and re.match('^[\s\d\.]+$', row[cvsn]):
                    row[cvsn] = float(row[cvsn])
            elif action == 'int':
                if len(row[cvsn]) > 0 and re.match('^[\s\d\.]+$', row[cvsn]):
                    row[cvsn] = int(float(row[cvsn]))
            elif action == 'noun':
                row[cvsn] = nltk_nouns(row[cvsn])
            elif action == 'nounphrase':
                row[cvsn] = nltk_nounphrase(row[cvsn])
                
        # load CSV data into table
        #logging.info("the row: " + str(row))
        vals = [row[h] for h in r.header]
        cur.execute("INSERT INTO temptable VALUES(" + isrt + ")", vals)

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
    flds = ", ".join(['"' + h + '"' for h in r.header])
    #cur.execute('PRAGMA table_info(temptable);')
    #newcols = cur.fetchall()
    #newcols = [f for (i, f, t, p1, p2, p3) in newcols]
    #newcols = dict(zip(newcols, newcols))
    #logging.info("temtable cols: " + str(newcols))

    if options.list:
        flds = options.list
    sql = 'SELECT ' +flds + ' FROM temptable ' + where + groupby + orderby
    logging.info("SQL is: " + str(sql))
    try:
        cur.execute(sql)
    except sqlite3.OperationalError as (msg):
        logging.info("SQL Error: " + str(msg))
        sys.exit(-1)

    # get the header and data from the SQL SELECT
    col_names = [cn[0] for cn in cur.description]
    rows = cur.fetchall()

    # output as CSV again
    logging.info("column names out: " + str(col_names))
    rows.insert(0, col_names)
    output_csv_file(sys.stdout, rows, options.delimiter)

    # tidy up sqlite db
    os.unlink(DB_FILE)
    sys.exit(0)


# ------ Good Ol' main ------
if __name__ == "__main__":
    main()

