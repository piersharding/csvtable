#!/bin/sh

./csvtable.py --where="a != 'a1'" --group='a,b' --list='*,SUM(d) AS tot' < tst.csv 
