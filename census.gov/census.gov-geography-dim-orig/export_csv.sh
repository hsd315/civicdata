#!/bin/sh
sqlite3 -csv -header build/census.gov/geography-dim-orig-a7d9-r1/$1.db "select * from $1" > /tmp/$1.csv; open /tmp/$1.csv