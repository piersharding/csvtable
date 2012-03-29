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

