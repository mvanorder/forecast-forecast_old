''' Make the instant documents. Pull all documents from the "forecasted" and the "observed" database collections. Sort those
documents according to the type: forecasted documents get their forecast arrays sorted into forecast lists within the documents
having the same zipcode and instant values, observed documents are inserted to the same document corrosponding to the 
zipcode and instant values. '''


import time

import pymongo
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
        
def update_command_for(data):
    ''' the 'update command' is the MongoDB command that is used to update data should be a weather type object. it will have its filter and update set according to the entry content. It
    returns a command to update in a pymongo database.

    :param data: the dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    from pymongo import UpdateOne
    if "Weather" in data:
        try:
            filters = {'zipcode': data['Weather'].pop('zipcode'), 'instant': data['Weather'].pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except:
        ### this if for the processing of data in OWM.forecasted and OWM.observed.
            if "Weather" in data:
                try:
                    data['Weather']['time_to_instant'] =  data['Weather'].pop('reference_time') - data['reception_time']
                    filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
                    updates = {'$set': {'weather': data['Weather']}}
                except KeyError:
                    print('caught KeyError')
            else:
                try:
                    filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
                    updates = {'$push': {'forecasts': data}} 
                except KeyError:
                    print('caught keyerror')
    else:
        filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
        updates = {'$push': {'forecasts': data}} # append the forecast object to the forecasts list
    return UpdateOne(filters, updates,  upsert=True)

def delete_command_for(data):
    ''' the 'delete command' is the MongoDB command that is used to update data should be a weather type object. it will have its filter and update set according to the entry content. It
    returns a command to update in a pymongo database.

    :param data: the dictionary created from the api calls
    :type data: dict
    :return: the command that will be used to find and update documents
    ''' 
    from pymongo import DeleteOne

    # if "Weather" in data:
    #     filters = {'zipcode': data['Weather'].pop('zipcode'), 'instant': data['Weather'].pop('instant')}
    #     updates = {'$set': {'weather': data['Weather']}}
    ### this if for the processing of data in OWM.forecasted and OWM.observed.
    if "Weather" in data:
        try:
            data['Weather']['time_to_instant'] =  data['Weather'].pop('reference_time') - data['reception_time']
            filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
            updates = {'$set': {'weather': data['Weather']}}
        except KeyError:
            print('caught KeyError')
    # else:
    #     filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
    #     updates = {'$push': {'forecasts': data}} # append the forecast object to the forecasts list
    ### Same thing down here ###
    else:
        try:
            filters = {'zipcode': data.pop('zipcode'), 'instant': data.pop('instant')}
            updates = {'$push': {'forecasts': data}} 
        except KeyError:
            print('caught keyerror')
    return UpdateOne(filters, updates,  upsert=True)


def make_load_list_from_cursor(pymongoCursorOnWeather):
    ''' create the list of objects from the database to be loaded through bulk_write() 
    
    :param pymongoCursorOnWeather: it is just what the name says it is
    :type pymongoCursorOnWeather: a pymongo cursor
    :return update_list: list of update commands for the weather objects on the cursor
    '''

    update_list = []
#     try:
#         pymongoCursorOnWeather.next()
#     except StopIteration:
#         print('got to the end of the cursor')
        
    # check the first entry to know whether it's forecast or observation
    # print(pymongoCursorOnWeather.count_documents())
    
    if 'Weather' in pymongoCursorOnWeather[0]:
#         print('it is Weather')
        for obj in pymongoCursorOnWeather:
            update_list.append(update_command_for(obj))
        return update_list
    else:
#         print('it is something other than Weather; going into for loop')
        
        for obj in pymongoCursorOnWeather:

            # Trying things that will capture any of the formats published over the developemnet period.
            try:
                if 'reception_time':
                    casts = obj['weathers'] # use the 'weathers' array from the forecast
                    for cast in casts:
                        cast['zipcode'] = obj['zipcode']
                        cast['time_to_instant'] = cast['instant'] - obj['reception_time']
                        update_list.append(update_command_for(cast))
                else:    
                    casts = obj['weathers'] # use the 'weathers' array from the forecast
                    for cast in casts:
                        # this is just setting the fields as I need them for each update object as it
                        # gets loaded to the database
                        cast['zipcode'] = obj['zipcode']
                        cast['time_to_instant'] = cast['instant'] - cast['reception_time']
                        update_list.append(update_command_for(cast))
            except KeyError:
                filename = 'keyerror_from_id_not_updated.txt'
                print(f'printing to {filename}')
                with open(filename, 'a') as f:
                    f.write(str(obj['_id'])+'\n')
                # print(f'KeyError....{obj["_id"]}')
            except:
                filename = 'some_other_error_from_id_not_updated.txt'
                print(f'printing to {filename}')
                with open(filename, 'a') as f:
                    f.write(obj['_id']+'\n')
                # print(obj['_id'])
#             print('completed loop iteration')
#         print(f'here is the update list {update_list}')
        return update_list

# def convert_keys_to_string(dictionary):
#     """Recursively converts dictionary keys to strings."""
#     if not isinstance(dictionary, dict):
#         return dictionary
#     return dict((str(k), convert_keys_to_string(v)) 
#         for k, v in dictionary.items())

# def convert(data):
#     if isinstance(data, basestring):
#         return str(data)
#     elif isinstance(data, int):
#         return str(data)
#     elif isinstance(data, collections.Mapping):
#         return dict(map(convert, data.iteritems()))
#     elif isinstance(data, collections.Iterable):
#         return type(data)(map(convert, data))
#     else:
#         return data


def copy_docs(col, destination_db, destination_col, filters={}, delete=False):
    ''' move or copy a collection within and between databases 
    
    :param col: the collection to be copied
    :type col: a pymongo collection or 
    :param destination_col: the collection you want the documents copied into
    :type destination_col: a pymongo.collection.Collection object
    :param destination_db: the database with the collection you want the documents copied into
    :type destination_db: a pymongo database pymongo.databse.Database
    :param filters: a filter for the documents to be copied from the collection. By default all collection docs will be copied
    :type filters: dict
    '''
    client = Client(host=host, port=port)
#     print(original[0])
    copy = []
    n=0
#     if type(col) == dict:
#         for key, value in col:
#             copy.append(str(value))
#     elif type(col) == list:
#         for value in col:
#             copy.append(str(value))
#     else:
#         original = col.find(filters)
#         for item in col.find(filters):
#             copy.append(item)
    original = col.find(filters)
    for item in col.find(filters):
        copy.append(item)
    print(f'type(copy) = {type(copy)}')
#     print(f'copy[0] = {copy[0]}')
    print(f'len(copy) = {len(copy)}. Just about to name the dbncol and insert_many into it')
    destination = dbncol(client, collection=destination_col, database=destination_db)
    inserted_ids = destination.insert_many(copy).inserted_ids
    print(type(inserted_ids))
#     if type(inserted_ids) == dict:
#         print('inserted_ids is a dict and I am converting the keys to strings...')
#         convert_keys_to_string(inserted_ids)
    print(f'Insertion complete: the inserted id\'s are: {inserted_ids}') # list of the doc ids that were successfully inserted
    if delete == True:
        # remove all the documents from the origin collection
        for row in inserted_ids:
            filters = {'_id': row}
            col.delete_one(filters)
        print(f'MOVED docs from {col} to {destination}, that is {destination_db}.{destination_col}')
    else:
        print(f'COPIED docs in {col} to {destination}, that is {destination_db}.{destination_col}')

# def copy_docs(col, destination_db, destination_col, filters={}, delete=False):
#     ''' move or copy a collection within and between databases 
    
#     :param col: the collection to be copied
#     :type col: a pymongo collection or 
#     :param destination_col: the collection you want the documents copied into
#     :type destination_col: a pymongo.collection.Collection object
#     :param destination_db: the database with the collection you want the documents copied into
#     :type destination_db: a pymongo database pymongo.databse.Database
#     :param filters: a filter for the documents to be copied from the collection. By default all collection docs will be copied
#     :type filters: dict
#     '''
#     client = Client(host=host, port=port)
# #     print(original[0])
#     copy = []
#     n=0
# #     if type(col) == dict:
# #         for key, value in col:
# #             copy.append(str(value))
# #     elif type(col) == list:
# #         for value in col:
# #             copy.append(str(value))
# #     else:
# #         original = col.find(filters)
# #         for item in col.find(filters):
# #             copy.append(item)
#     original = col.find(filters)
#     for item in col.find(filters):
#         copy.append(item['_id'])
#     print(f'type(copy) = {type(copy)}')
#     print(f'len(copy) = {len(copy)}. Just about name the dbncol and insert_many into it')
#     destination = dbncol(client, collection=destination_col, database=destination_db)
#     inserted_ids = destination.insert_many(copy).inserted_ids
#     print(type(inserted_ids))
# #     if type(inserted_ids) == dict:
# #         print('inserted_ids is a dict and I am converting the keys to strings...')
# #         convert_keys_to_string(inserted_ids)
#     print(f'Insertion complete: the inserted id\'s are: {inserted_ids}') # list of the doc ids that were successfully inserted
#     if delete == True:
#         # remove all the documents from the origin collection
#         for row in inserted_ids:
#             filters = {'_id': value}
#             col.delete_one(filters)
#         print(f'MOVED docs from {col} to {destination}, that is {destination_db}.{destination_col}')
#     else:
#         print(f'COPIED docs in {col} to {destination}, that is {destination_db}.{destination_col}')

__name__ = '__main__'
if __name__ == "__main__":
    print('make_instants imported and running main')
    client = Client(host=host, port=port)
    # set the database and collection to pull from
    database = "test"
    cast_col = dbncol(client, "cast_temp", database=database)
    obs_col = dbncol(client, "obs_temp", database=database)
    inst_col = dbncol(client, "instant_temp", database=database)
    num_docs_cast = cast_col.count_documents({}) # get a count of the number of documents under the cursor so that you
    num_docs_obs = obs_col.count_documents({})   # can know when to break out of the while loop so not to cause error
    print(num_docs_obs, num_docs_cast)
    forecasts = cast_col.find({})
    observations = obs_col.find({})
    inst_col.create_index([('instant', pymongo.DESCENDING)])
    inst_col.bulk_write(make_load_list_from_cursor(forecasts))
    inst_col.bulk_write(make_load_list_from_cursor(observations))
            
    print('attempting to move those docs just sorted....I am expecting that this is every document in the cast_temp collection')
    copy_docs(cast_col, database, 'cast_archive', delete=True)
    print('attempting to move those docs just sorted....I am assuming that this is every dcoument in the obs_temp collection')
    copy_docs(obs_col, database, 'obs_archive', delete=True)

# __name__ = '__main__'
# if __name__ == "__main__":
    
#     client = Client(host=host, port=port)
#     # set the database and collection to pull from
#     database = "test"
#     cast_col = dbncol(client, "cast_temp", database=database)
#     obs_col = dbncol(client, "obs_temp", database=database)
#     inst_col = dbncol(client, "instant_temp", database=database)
#     num_docs_cast = cast_col.count_documents({}) # get a count of the number of documents under the cursor so that you
#     num_docs_obs = obs_col.count_documents({})   # can know when to break out of the while loop so not to cause error
#     print(num_docs_obs, num_docs_cast)
#     m, n, i = 0, 0, 0 # n to count the total number of documents sorted, i to track the number of passes of the loop
#     while m < num_docs_cast and n < num_docs_obs:
#         print(f'in the while loop with i={i}')
#         # this block will get 1000 forecast documents, create the bulk_write commands list, and execute the
#         # bulk write function.
#         forecasts = cast_col.find({})[m:m+1000]
#         m += 1000
#         observations = obs_col.find({})[n:n+1000]
#         n += 1000
#         inst_col.create_index([('instant', pymongo.DESCENDING)])
# #         print('OBSERVATIONS.................\n\n', '\n', observations[0])
        
#         # within each line: create the update command for each ducument update, create the list of those updates,
#         # execute the bulk_write command on the list of updates, and finally set a dict of inserted object id's.
#         bwr_casts = inst_col.bulk_write(make_load_list_from_cursor(forecasts)).upserted_ids
#         bwr_obs = inst_col.bulk_write(make_load_list_from_cursor(observations)).upserted_ids
# #         bwr_casts = convert_key_to_string(bwr_casts)
# #         bwr_obs = convert_key_to_string(bwr_obs)
# #         print(bwr_casts)
#         print('attempting to move those docs just sorted....bwr_casts= {bwr_casts}\n')
#         copy_docs(cast_col, database, 'cast_archive', bwr_casts, delete=True)
#         print('attempting to move those docs just sorted....bwr_obs= {bwr_obs}\n')
#         copy_docs(obs_col, database, 'obs_archive', bwr_obs, delete=True)
        
#         i += 1
#         num_docs_cast = cast_col.count_documents({}) # get a count of the number of documents under the cursor so that you
#         print(f'num_docs_cast= {num_docs_cast}')
#         num_docs_obs = obs_col.count_documents({})   # can know when to break out of the while loop so not to cause error
#         print(f'num_docs_obs= {num_docs_obs}')
#         print(f'sorted {n} forecast arrays after {i} main while loop iterations')
    
#     print('past while loop')
    
#     # repeat the commands from the while loop, this time on the last <1000 documents from the collection
#     forecasts = cast_col.find({})[n:]
#     observations = obs_col.find({})[n:]
#     inst_col.create_index([('instant', pymongo.DESCENDING)])
#     # within each line: create the update command for each ducument update, create the list of those updates,
#     # execute the bulk_write command on the list of updates, and finally set a dict of inserted object id's.
#     bwr_casts = inst_col.bulk_write(make_load_list_from_cursor(forecasts)).upserted_ids
#     m += len(bwr_casts)
#     print(f'just sorted the last {len(bwr_casts) } documents from the "forecasted" collection')
#     print(bwr_casts)
#     print('starting sort job of the "observed" collection')
#     bwr_obs = inst_col.bulk_write(make_load_list_from_cursor(observations)).upserted_ids
#     n += len(bwr_obs)
#     print(f'just sorted the last {len(bwr_obs)} documents from the "observed" collection')
# #     print('starting the copy and delete the sorted ')
# #     copy_docs(cast_col, database, 'cast_archive', bwr_casts, delete=True)
# #     copy_docs(cast_col, database, 'obs_archive', bwr_obs, delete=True)