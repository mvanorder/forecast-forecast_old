''' Useful functions for forecast-forecast specific operations '''

def datollection(client, database, collection0):
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
    # for _id in id_list:
    
        