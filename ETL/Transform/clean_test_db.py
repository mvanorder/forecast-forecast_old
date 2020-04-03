import time

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

def add_timeto_inst(cursor):
    ''' replace the instant and reference_time in each weather object in the forecasts
    array with 'time_to_instant'. Add each updated document to an array and return it

     :param cursor: a cursor object over the results of a qurey on the test.instant collection
     :type cursor: pymongo.cursor.Cursor

     :return: array of updated documents
    '''
    cursor_keyerror_list = [] # hold object id's for further investigation
    updates_list = []
    # loop through all the objects on the cursor. Handle the various KeyErrors as the arise
    # add the correct docs to an update list and the others to an errors list
    for doc in cursor:
        try:
            data = doc['forecasts']
        except KeyError:
            cursor_keyerror_list.append(doc['_id'])
            continue
        for forecast in data:
            try:
                forecast['time_to_instant'] = forecast.pop('reference_time') - forecast.pop('reception_time')
            except KeyError:
                if forecast['instant'] and forecast['time_to_instant']:
                    forecast.pop('instant')
            except KeyError:
                print(forecast)
        updates_list.append(doc)            
    # print(f'length of updates list:{len(updates_list)}\nlength of keyerror_list: {len(cursor_keyerror_list)}')
    return updates_list


if __name__ == "__main__":
    host = host
    port = port

    updated_doc_ids = []
    client = Client(host=host, port=port)
    col = dbncol(client, 'instant', database='test')
    
    result = col.find({}).batch_size(200)
    
    updates = add_timeto_inst(result)
    col = dbncol(client, 'instants_temp', database='forecast-forecast')
    col.insert_many(updates)
    # for item in updates:
    #     # col.insert_one(item)
    #     updated_doc_ids.append(item['_id'])
    client.close()
    print(f'there are {len(updated_doc_ids)} updated docs in updated_doc_ids.')
    filename = '/Users/chuckvanhoff/data/forcast-forcast/ETL/Transform/testdb_sorted_instant_ids.txt'
    with open(filename, 'w') as f:
        for item in updated_doc_ids:
            post = str(item) + '\n'
            f.write(post)