# This program should take data from some webpage and update a database. 
# It should all happen automatically upon prgram initiation.

# Setup the database connection
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
          
host = 'localhost'
port = 27017

def check_db_access(str: host, port):
    '''A check that there is write access to the database'''
   
    client = MongoClient(host=host, port=port)
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print('Connection made')
    except ConnectionFailure:
        print("Server not available")

    # check the database connections
        # Get a count of the number of databases at the connection (accessible through that port) before attempting to add to it
    db_count_pre = len(client.list_database_names())
        # Add a database and collection
    db = client.test_db
    col = db.test_col

    # Insert something to the db
    post = {'name':'Chuck VanHoff',
           'age':'38',
           'hobby':'gardening'
           }
    col.insert_one(post)

        # Get a count of the databases after adding one
    db_count_post = len(client.list_database_names())

    if db_count_pre-db_count_post>=0:
        print('Your conneciton is flipped up')
    else:
        print('You have write access')

    client.drop_database(db)
    client.close()
    return()
