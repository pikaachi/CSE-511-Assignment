#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='Dipi7546', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute('CREATE TABLE %s (userid INT, movieid INT, rating FLOAT)'% (ratingstablename))
    openconnection.commit()
    #open ratings file and insert into ratingstablename
    with open(ratingsfilepath, "r") as file_name:
        for row in file_name:
            [userid, movieid, rating, timestamp] = row.split("::")
            cur.execute('INSERT INTO %s VALUES (%s,%s,%s);' % (ratingstablename, userid, movieid, rating))
    openconnection.commit()
    cur.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    #create meta data
    cur.execute("CREATE TABLE IF NOT EXISTS range_meta(num_partition INT, from_range FLOAT, to_range float)")
    openconnection.commit()
    rating_range = 5.0/numberofpartitions
    temp = 0.0
    i = 0
    while temp < 5.0:
        #print('temp %s , rating_range %s)' % (temp, temp+rating_range)) 
        #create table range_part0 between temp<= rating <= temp + rating_range
        if temp == 0:
            cur.execute("CREATE TABLE range_part%s AS SELECT * FROM %s WHERE rating>=%s AND rating<=%s" % (i, ratingstablename, temp, (temp+rating_range)))
            openconnection.commit()
            i = i + 1
            temp = temp + rating_range
        #create table range_part{i} between temp < rating <= temp + rating_range
        else:
            cur.execute("CREATE TABLE range_part%s AS SELECT * FROM %s WHERE rating>%s AND rating<=%s" % (i, ratingstablename, temp, (temp+rating_range)))
            openconnection.commit()
            i = i + 1
            temp = temp + rating_range
        cur.execute('INSERT INTO range_meta (num_partition, from_range, to_range) VALUES (%d,%f,%f)' % (i, temp, (temp+rating_range)))    
        openconnection.commit()
        #print ('inserted')
    cur.execute("SELECT * FROM range_meta;")
    openconnection.commit()
    cur.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS rrobin_meta(num_partition INT, idx INT)')
    cur.execute('SELECT * FROM %s'% ratingstablename)
    values = cur.fetchall()
    i = 0
    last_insert = 0 #last idx
    for row in values:
        if i < numberofpartitions:
            cur.execute("CREATE TABLE rrobin_part%s (userid INT, movieid INT, rating FLOAT)" % i)
            openconnection.commit()
            cur.execute("INSERT INTO rrobin_part%s (userid, movieid, rating) VALUES (%d, %d,%f)" % (i, row[0], row[1], row[2]))
            openconnection.commit()
            i = i+1
            last_insert = last_insert + 1
            j = (last_insert %numberofpartitions) #becomes 0 at #partition
        else:
            cur.execute("INSERT INTO rrobin_part%s (userid, movieid, rating) VALUES (%d, %d,%f)" % (j, row[0], row[1], row[2]))
            openconnection.commit()
            last_insert = (last_insert +1) % numberofpartitions
            j = last_insert
    cur.execute("INSERT INTO rrobin_meta(num_partition, idx) VALUES(%d, %d)" % (numberofpartitions, last_insert))
    openconnection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM rrobin_meta")
    values = cur.fetchone()
    num_partition = values[0]
    last_insert = values[1]
    j = last_insert % num_partition
    cur.execute("INSERT INTO rrobin_part%s (userid, movieid, rating) VALUES (%d, %d,%f)" % (j, userid, itemid, rating)) 
    openconnection.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute('SELECT MIN(num_partition) FROM range_meta as rm WHERE rm.from_range <= %s AND rm.to_range >= %s ' % (rating, rating))
    num_partition = cur.fetchone()
    #for row in num_partition:
     #   print('row to add %s'% row)
    cur.execute('INSERT INTO %s VALUES (%d,%d,%f);' % (ratingstablename, userid, itemid, rating))
    openconnection.commit()
    cur.execute('INSERT INTO range_part%s VALUES (%d,%d,%f);' % (num_partition[0], userid, itemid, rating))
    openconnection.commit()
    cur.close()
def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
