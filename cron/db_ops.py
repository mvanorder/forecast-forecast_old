''' Useful functions for forecast-forecast specific operations '''

import time

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from pymongo.errors import InvalidDocument, OperationFailure, ConfigurationError
from urllib.parse import quote


database = 'test'


def check_db_access(client):
    '''A check that there is write access to the database'''
 
    try:
        client.admin.command('ismaster')
    except ConnectionFailure:
        print("Server not available")
    # check the database connections
        # Get a count of the databases in the beginning
        # Add a database and collection
        # Insert something to the db
        # Get a count of the databases after adding one
    db_count_pre = len(client.list_database_names())
    db = client.test_db
    col = db.test_col
    post = {'name':'Chuck VanHoff',
           'age':'38',
           'hobby':'gardening'
           }
    col.insert_one(post)
    db_count_post = len(client.list_database_names())
    if db_count_pre - db_count_post >= 0:
        print('Your conneciton is flipped up')
    else:
        print('You have write access')
    # Dump the extra garbage and close out
    client.drop_database(db)
    client.close()
    return

def Client(host=None, port=None, uri=None):
    ### THIS IS NOT VERY VALUABLE AS A FUNCITON ###
    ''' Create and return a pymongo MongoClient object. Connect with the given
    parameters if possible, switch to local if the remote connection is not
    possible, using the default host and port.
    
    :param host: the local host to be used. defaults within to localhost
    :type host: sting
    :param port: the local port to be used. defaults within to 27017
    :type port: int
    :param uri: the remote server URI. must be uri encoded
    type uri: uri encoded sting'''
    
    if host and port:
        try:
            client = MongoClient(host=host, port=port)
            check_db_access(client)
            return client
        except ConnectionFailure:
            # connect to the remote server if a valid uri is given
            if uri:
                print('caught ConnectionFailure on local server. Trying to make it with remote')
                client = MongoClient(uri)
                print(f'established remote MongoClient on URI={uri}')
                return client
            print('caught ConnectionFailure on local server. Returning None')
            return None
    elif uri:
        # verify that the connection with the remote server is active and switch to the local server if it's not
        try:
            client = MongoClient(uri)
            check_db_access(client)
            return client
        except ConfigurationError:
            print(f'Caught configurationError in client() for URI={uri}. It was likely triggered by a DNS timeout.')
            client = MongoClient(host=host, port=port)
            print('connection made with local server, even though you asked for the remote server')
            return client
    
def dbncol(client, collection, database=database):
    ''' Make a connection to the database and collection given in the arguments.

    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the name of the database to be used. It must be a database name present at the client
    :type database: str
    :param collection: the database collection to be used.  It must be a collection name present in the database
    :type collection: str
    
    :return col: the collection to be used
    :type: pymongo.collection.Collection
    '''

    n=0
    try:
        db = Database(client, database)
    except AttributeError:
        from config import uri
        client = MongoClient(uri)
        db = Database(client, database)
    col = Collection(db, collection)
    return col

def load(data, client, database, collection):
    ''' Load data to specified database collection. Also checks for a
    preexisting document with the same instant and zipcode, and updates it in
    the case that there was already one there.

    :param data: the dictionary created from the api calls
    :type data: dict
    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the database to be used
    :type database: str
    :param collection: the database collection to be used
    :type collection: str
    '''

    col = dbncol(client, collection, database=database)

    # set the appropriate database collections, filters and update types
    if collection == 'instant':
        filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
        updates = {'$push': {'forecasts': data}} # append to the forecasts list
        try:
            # check to see if there is a document that fits the parameters. If
            # there is, update it, if there isn't, upsert it.
            return col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
    elif collection == 'observed' or collection == 'forecasted':
        try:
            col.insert_one(data)
            return
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')
    else:
        try:
            filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
            updates = {'$set': {'forecasts': data}} # append to forecasts list
            return col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data to {collection}')

def copy_docs(col, destination_db, destination_col, filters={}, delete=False):
    ### THIS DOES NOT WORK ###
    ''' move or copy a collection within and between databases 
    
    :param col: the collection to be copied
    :type col: a pymongo collection
    :param destination_col: the collection you want the documents copied into
    :type destination_col: a pymongo.collection.Collection object
    :param destination_db: the database you want the documents copied into
    :type destination_db: a pymongo database pymongo.databse.Database
    :param filters: a filter for the documents to be copied from the collection
    By default all collection docs will be copied
    :type filters: dict
    '''
    client = Client(host=host, port=port)
    original = col.find(filters).batch_size(1000)
    copy = []
    for item in original:
        copy.append(item)
    destination = dbncol(client, collection=destination_col, database=destination_db)
    inserted_ids = destination.insert_many(copy).inserted_ids # inserted IDs 
    if delete == True:
        # remove all the documents from the origin collection
        for item in inserted_ids:
            filter = {'_id': item}
            col.delete_one(filter)
        print(f'MOVED docs from {col} to {destination}.')
    else:
        print(f'COPIED docs in {col} to {destination}.')
