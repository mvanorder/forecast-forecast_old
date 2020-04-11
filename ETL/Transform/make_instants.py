''' Make the instant documents. Pull all documents from the "forecasted" and the "observed" database collections. Sort those
documents according to the type: forecasted documents get their forecast arrays sorted into forecast lists within the documents
having the same zipcode and instant values, observed documents are inserted to the same document corrosponding to the 
zipcode and instant values. '''


import time

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import ConnectionFailure, InvalidDocument, DuplicateKeyError, OperationFailure, ConfigurationError
from urllib.parse import quote

from config import user, password, socket_path


# use the local host and port for all the primary operations
port = 27017
host = 'localhost'
# use the remote host and port when the instant document is complete and is ready for application
password = quote(password)    # url encode the password for the mongodb uri
uri = "mongodb+srv://%s:%s@%s" % (user, password, socket_path)
print(uri)


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

def find_data(client, database, collection, filters={}):
    ''' Find the items in the specified database and collection using the filters.

    :param client: a MongoClient instance
    :type client: pymongo.MongoClient
    :param database: the name of the database to be used. It must be a database name present at the client
    :type database: str
    :param collection: the database collection to be used.  It must be a collection name present in the database
    :type collection: str
    :param filters: the parameters used for filtering the returned data. An empty filter parameter returns the full collection
    :type filters: dict
    
    :return: the result of the query
    :type: pymongo.cursor.CursorType
    '''

    db = Database(client, database)
    col = Collection(db, collection)
    return col.find(filters).batch_size(100)

def load_weather(data, client, database, collection):
    ''' Load data to specified database collection. This determines the appropriate way to process the load depending on the
    collection to which it should be loaded. Data is expected to be a weather-type dictionary. When the collection is "instants"
    the data is appended the specified object's forecasts array in the instants collection; when the collection is either
    "forecasted" or "observed" the object is insterted uniquely to the specified collection. Also checks for a preexisting
    document with the same instant and zipcode, then updates it in the case that there was already one there.

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
    # decide how to handle the loading process depending on where the document will be loaded.
    if collection == 'instant' or collection == 'test_instants':
        # set the appropriate database collections, filters and update types
        if "Weather" in data:
            updates = {'$set': {'weather': data['Weather']}}
        else:
            updates = {'$push': {'forecasts': data}} # append the forecast object to the forecasts list
        try:
            filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
            col.find_one_and_update(filters, updates,  upsert=True)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data into {collection}.')
        except KeyError:
            print(f'you just got keyerror on {zipcode} or {instant}')
    elif collection == 'observed' or collection == 'forecasted':
        try:
            col.insert_one(data)
        except DuplicateKeyError:
            return(f'DuplicateKeyError, could not insert data into {collection}.')
        
def bulk_entry(data):
    ''' data should be a weather type object. it will have its filter and update set according to the entry content. It
    returns a command to update in a pymongo database.

    :param data: the dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    from pymongo import UpdateOne
    
    if "Weather" in data:
        filters = {'zipcode': data['Weather'].pop('zipcode'), 'instant': data['Weather'].pop('instant')}
        updates = {'$set': {'weather': data['Weather']}}
    else:
        filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
        updates = {'$push': {'forecasts': data}} # append the forecast object to the forecasts list
    return UpdateOne(filters, updates,  upsert=True)

def make_load_list_from_cursor(pymongoCursorOnWeather):
    ''' create the list of objects from the database to be loaded through bulk_write() 
    
    :param pymongoCursorOnWeather: it is just what the name says it is
    :type pymongoCursorOnWeather: a pymongo cursor
    :return update_list: list of update commands for the weather objects on the cursor
    '''
    
    update_list = []
    if 'Weather' in pymongoCursorOnWeather[0]:
        for obj in pymongoCursorOnWeather:
            update_list.append(bulk_entry(obj))
        return update_list
    else:
        for obj in pymongoCursorOnWeather:
            casts = obj['weathers'] # use the weathers array from the forecast
            for cast in casts:
                update_list.append(bulk_entry(cast))
        return update_list


if __name__ == "__main__":
    client = Client(host=host, port=port)
    # set the database and collection to pull from
    database = "test"
    collection = "forecasted"
    forecasts = find_data(client, database, collection)
    collection = "observed"
    observations = find_data(client, database, collection)
    # set the collection to be updated
    collection = 'test_instants'
    start = time.time()
    # sort the forecasts into instants
    col = dbncol(client, collection)
    col.bulk_write(make_load_list_from_cursor(forecasts))
    col.bulk_write(make_load_list_from_cursor(observations))
    print(f'{time.time()-start} seconds passed while sorting each weathers array and adding observations to instants')