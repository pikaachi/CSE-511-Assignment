#!/usr/bin/python3
#
#Tester for students
# Do not hard code values in your program for ratings.
# table name and input file name.
# Do not close con objects in your program.
# Invalid ranges will not be tested.
# Order of output does not matter, only correctness will be checked.Fragmentation
# Use discussion board extensively to clear doubts.
# Sample output does not correspond to data in test_data.txt.
#

DATABASE_NAME = 'dds_assignment'
RATINGS_TABLE='ratings'
RATING_INPUT_FILEPATH='test_data.dat'

import Fragmentation as Fragmentation #Assignment 3: Data Fragmentation Submission file
import Interface as MySolution # Assignment 4: Query Processing Submission file
import traceback

if __name__ == '__main__':
    try:
        #Creating Database ddsassignment2
        print("Creating Database named as " + DATABASE_NAME)
        Fragmentation.createDB(DATABASE_NAME)

        # Getting connection to the database
        print("Getting connection from the "+ DATABASE_NAME + " database")
        con = Fragmentation.getOpenConnection(dbname=DATABASE_NAME)

        # Clear the database existing tables
        print("Delete tables")
        Fragmentation.deleteTables('all', con)

        # Loading Ratings table
        print("Creating and Loading the ratings table")
        Fragmentation.loadRatings(RATINGS_TABLE, RATING_INPUT_FILEPATH, con)

        # Doing Range Partition
        print("Doing the Range Partitions")
        Fragmentation.rangePartition(RATINGS_TABLE, 5, con)

        # Doing Round Robin Partition
        print("Doing the Round Robin Partitions")
        Fragmentation.roundRobinPartition(RATINGS_TABLE, 5, con)

        # Deleting Ratings Table because Point Query and Range Query should not use ratings table instead they should use partitions.
        Fragmentation.deleteTables(RATINGS_TABLE, con)

        # Calling RangeQuery
        print("Performing Range Query")
        MySolution.RangeQuery(RATINGS_TABLE, 1.5, 3.5, con)
        #MySolution.RangeQuery('ratings',1,4,con);

        # Calling PointQuery
        print("Performing Point Query")
        MySolution.PointQuery(RATINGS_TABLE, 4.5, con)
        #MySolution.PointQuery('ratings',2,con);
        
        # Deleting All Tables
        Fragmentation.deleteTables('all', con)

        if con:
            con.close()

    except Exception as detail:
        traceback.print_exc()
