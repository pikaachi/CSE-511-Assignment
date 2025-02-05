#!/usr/bin/python3


import psycopg2
import os
import sys


DATABASE_NAME='dds_assignment'
RATINGS_TABLE_NAME='ratings'
RANGE_TABLE_PREFIX='range_part'
RROBIN_TABLE_PREFIX='rrobin_part'
RANGE_QUERY_OUTPUT_FILE='RangeQueryOut.txt'
PONT_QUERY_OUTPUT_FILE='PointQueryOut.txt'
RANGE_RATINGS_METADATA_TABLE ='rangeratingsmetadata'
RROBIN_RATINGS_METADATA_TABLE='roundrobinratingsmetadata'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    # open file to write
    file = open('RangeQueryOut.txt', 'w')
    query = 'SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s'
    # Execute the query with the parameter
    cur.execute(query, ('range_part%',))
    count = cur.fetchone()[0]
    #roundrobin
    query2= 'SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s'
    cur.execute(query2, ('rrobin_part%',))
    count2 = cur.fetchone()[0]
    #print(count)
    for i in range(0, count):
        table_name = 'range_part'+str(i)
        cur.execute('SELECT * FROM range_part%s where rating >= %s AND rating <= %s;' % (i, ratingMinValue, ratingMaxValue))
        row = cur.fetchall()
        #print(row)
        for value in row:
            #print(value)
            file.write('%s,%s,%s,%s\n' % (table_name, value[0], value[1], value[2]))
    i = 0
    for i in range(0,count2):
        table_name = 'rrobin_part'+str(i)
        cur.execute('SELECT * FROM rrobin_part%s where rating >= %s AND rating <= %s;' % (i, ratingMinValue, ratingMaxValue))
        row = cur.fetchall()
        #print(row)
        for value in row:
            file.write('%s,%s,%s,%s\n' % (table_name, value[0], value[1], value[2]))
    file.close()
def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    # open file to write
    file2 = open('PointQueryOut.txt', 'w')
    query = 'SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s'
    # Execute the query with the parameter
    cur.execute(query, ('range_part%',))
    count = cur.fetchone()[0]
    #print(count)
    #roundrobin
    query2= 'SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s'
    cur.execute(query2, ('rrobin_part%',))
    count2 = cur.fetchone()[0]
    #print(count)
    for i in range(0, count):
        table_name = 'range_part'+str(i)
        cur.execute('SELECT * FROM range_part%s where rating = %s;' % (i, ratingValue))
        row = cur.fetchall()
        #print(row)
        for value in row:
            #print(value)
            file2.write('%s,%s,%s,%s\n' % (table_name, value[0], value[1], value[2]))
    i = 0
    for i in range(0,count2):
        table_name = 'rrobin_part'+str(i)
        cur.execute('SELECT * FROM rrobin_part%s where rating = %s;' % (i, ratingValue))
        row = cur.fetchall()
        #print(row)
        for value in row:
            file2.write('%s,%s,%s,%s\n' % (table_name, value[0], value[1], value[2]))
    file2.close()
def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
