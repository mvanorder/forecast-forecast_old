''' go into '''import time

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure, ConfigurationError
from urllib.parse import quote

from config import user, password, socket_path, host, port
''' Useful functions for forecast-forecast specific operations '''

def Client(host=None, port=None, uri=None):
    ''' Create and return a pymongo MongoClient object. Connect with the given parameters if possible, switch to local if the
    remote connection is not possible, using the default host and port.
    
    :param host: the local host to be used. defaults within to localhost
    :type host: sting
    :param port: the local port to be used. defaults within to 27017
    :type port: int
    :param uri: the remote server URI. must be uri encoded
    type uri: uri encoded sting'''
    
    if host and port:
        try:
            client = MongoClient(host=host, port=port)
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
            return client
        except ConfigurationError:
            print(f'Caught configurationError in client() for URI={uri}. It was likely triggered by a DNS timeout.')
            client = MongoClient(host=host, port=port)
            print('connection made with local server, even though you asked for the remote server')
            return client

def dbncol(client, collection, database='test'):
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

    db = Database(client, database)
    col = Collection(db, collection)
    return col

def move_sorted(client, from_db, from_col, to_db, to_col, id_list):
    ''' Move the documents from the active database to the archive database.
    Once the forecasted and observed weather objects are sorted into instant documents and loaded 
    to the insants collection they can be moved for permanent storage to another database that will 
    contain only previously sorted weather documents.
    '''

    from_db = from_db
    from_col = from_col
    to_db = to_db
    to_col = to_col

    from_collection = datollection(client, from_db, form_col)
    to_collection = datollection(client, to_db, to_col)

    deleted = from_collection.delete_many(id_list)
    print(type(deleted))
    inserted = to_collection.insert_many(deleted)
    print(type(inserted))
    
def load_weather_object(data, client, database, collection):
    ''' Load data to specified database collection. Also checks for a preexisting document with the same instant and 
    zipcode, and updates it in the case that there was already one there.

    :param data: the dictionary created from the api calls
    :type data: dict
    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the database to be used
    :type database: str
    :param collection: the database collection to be used
    :type collection: str
    '''

    col = dbncol(client, collection, database='test1')

    # set the appropriate database collections, filters and update types
    if collection == 'instant':
        filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
        updates = {'$push': {'forecasts': data}} # append the forecast object to the forecasts list
        try:
            # check to see if there is a document that fits the parameters. If there is, update it, if there isn't, upsert it
            return col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data into {collection}.')
    elif collection == 'observed' or collection == 'forecasted':
        try:
            col.insert_one(data)
            return
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data into {collection}.')
    else:
        try:
            filters = {'zipcode':data['zipcode'], 'instant':data['instant']}
            updates = {'$set': {'forecasts': data}} # append the forecast object to the forecasts list
            return col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data into {collection}.')


if __name__ == "__main__":
    host = host
    port = port
    database = 'OWM'
    collection = 'instant'

    client = Client(host=host, port=port)
    col = dbncol(client, database=database, collection=collection)
    updated_doc_ids = []
    result = col.find({}).batch_size(10)
    r = result
    # change each document in restored instant collection. ** many, maybe all, the documents have all the fields for the observation
    # separtely key'd in the parent document. This will put each of those weather fields together under the field 'observation'
    from pprint import pprint
    n = 0
    fs_and_us = {'passed' : 0, 'replaced' : 0, 'updated' : 0} # for counting the loop results
    for item in r:
        if n%200 == 0:
            print(f'processed {n} documents')
        try:
            ref_time = item.pop('reference_time')
            item['observed'] = {
                'clouds' : item.pop('clouds'),
                'detailed_status' : item.pop('detailed_status'),
                'humidity' : item.pop('humidity'),
                'pressure' : item.pop('pressure'),
                'rain' : item.pop('rain'),
                'snow' : item.pop('snow'),
                'status' : item.pop('status'),
                'temperature' : item.pop('temperature'),
                'weather_code' : item.pop('weather_code'),
                'wind' : item.pop('wind'),
                'time_to_instant': item['instant']-ref_time
            }
        except KeyError:
    #         print('keyerror...continuing')
            fs_and_us['passed'] += 1
            continue
        filters = {'zipcode':item['zipcode'], 'instant':item['instant']}
        updates = {'$set': item}
        try:
            col.find_one_and_replace(filters, item, upsert=True)
    #         print('found and replaced')
            fs_and_us['replaced'] += 1
        except DuplicateKeyError:
            col.find_one_and_update(filters, updates, upsert=True)
            fs_and_us['updated'] += 1
        n += 1
    pprint(fs_and_us)